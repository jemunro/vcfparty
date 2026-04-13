## run — scatter a VCF into N shards and pipe each through a tool pipeline.
##
## This module is responsible for:
##   1. Parsing the "---"-separated argv into vcfparty args + pipeline stages.
##   2. Building the sh -c command string for each shard.
##   3. Mode inference from -o / {} flags.
##   4. Executing per-shard pipelines concurrently (all N shards run at once).

import std/[cpuinfo, os, posix, sequtils, strformat, strutils]
{.warning[Deprecated]: off.}
import std/threadpool
{.warning[Deprecated]: on.}
import scatter
import vcf_utils

# ---------------------------------------------------------------------------
# Argv parsing
# ---------------------------------------------------------------------------

proc isSep(tok: string): bool {.inline.} =
  ## Return true for "---" or ":::" — both are valid pipeline stage separators.
  tok == "---" or tok == ":::"

proc parseRunArgv*(argv: seq[string]): (seq[string], seq[seq[string]]) =
  ## Split argv at "---" / ":::" separators.
  ## Returns (vcfpartyArgs, stages).
  ## Exits 1 with a message if no separator is present or any stage is empty.
  var firstSep = -1
  for i, tok in argv:
    if isSep(tok):
      firstSep = i
      break
  if firstSep < 0:
    stderr.writeLine "vcfparty run: at least one --- stage is required"
    quit(1)
  let vcfpartyArgs = argv[0 ..< firstSep]

  var stages: seq[seq[string]]
  var cur: seq[string]
  for i in firstSep + 1 ..< argv.len:
    if isSep(argv[i]):
      if cur.len == 0:
        stderr.writeLine "vcfparty run: empty pipeline stage"
        quit(1)
      stages.add(cur)
      cur = @[]
    else:
      cur.add(argv[i])
  if cur.len == 0:
    stderr.writeLine "vcfparty run: empty pipeline stage"
    quit(1)
  stages.add(cur)
  result = (vcfpartyArgs, stages)

# ---------------------------------------------------------------------------
# Shell command construction
# ---------------------------------------------------------------------------

proc buildShellCmd*(stages: seq[seq[string]]): string =
  ## Build a sh -c command string from pipeline stages (no {} substitution).
  ## Each token is shell-quoted so special characters (< > | & etc.) in
  ## filter expressions are passed through safely.  Stages are joined with " | ".
  var parts: seq[string]
  for stage in stages:
    parts.add(stage.mapIt(quoteShell(it)).join(" "))
  result = parts.join(" | ")

# ---------------------------------------------------------------------------
# Mode inference
# ---------------------------------------------------------------------------

type RunMode* = enum
  rmNormal,      ## vcfparty writes shard output files via -o
  rmToolManaged  ## tool manages own output; vcfparty discards shard stdout

proc hasBracePlaceholder*(stages: seq[seq[string]]): bool =
  ## Return true if any token in any stage contains an unescaped {}.
  ## A \{} sequence is considered escaped and does NOT count.
  for stage in stages:
    for tok in stage:
      var i = 0
      while i < tok.len:
        if tok[i] == '\\' and i + 2 < tok.len and tok[i+1] == '{' and tok[i+2] == '}':
          i += 3  # skip \{}
        elif tok[i] == '{' and i + 1 < tok.len and tok[i+1] == '}':
          return true
        else:
          i += 1
  false

proc inferRunMode*(hasOutput: bool; hasBrace: bool): RunMode =
  ## Infer run mode from -o presence and {} in tool cmd.
  ## Emits a warning when -o is ignored (tool-managed mode).
  ## Calls quit(1) when no output of any kind is specified.
  if not hasOutput and not hasBrace:
    stderr.writeLine "error: no output specified: provide -o or {} in the tool command"
    quit(1)
  if hasBrace:
    if hasOutput:
      stderr.writeLine "warning: -o is ignored in tool-managed mode (tool command contains {})"
    return rmToolManaged
  return rmNormal

# ---------------------------------------------------------------------------
# {} substitution
# ---------------------------------------------------------------------------

proc substituteToken*(tok: string; shardNum: string): string =
  ## Replace each unescaped {} in tok with shardNum.
  ## Replace \{} with a literal {} (backslash consumed by vcfparty).
  ## Other characters are copied unchanged.
  var r = newStringOfCap(tok.len + shardNum.len)
  var i = 0
  while i < tok.len:
    if tok[i] == '\\' and i + 2 < tok.len and tok[i+1] == '{' and tok[i+2] == '}':
      r.add('{')
      r.add('}')
      i += 3
    elif tok[i] == '{' and i + 1 < tok.len and tok[i+1] == '}':
      r.add(shardNum)
      i += 2
    else:
      r.add(tok[i])
      i += 1
  r

proc buildShellCmdForShard*(stages: seq[seq[string]]; shardIdx: int; nShards: int): string =
  ## Build a per-shard shell command with {} replaced by the zero-padded shard number.
  ## \{} in tokens is replaced by a literal {} passed to the tool.
  let padded = align($(shardIdx + 1), len($nShards), '0')
  var parts: seq[string]
  for stage in stages:
    parts.add(stage.mapIt(quoteShell(substituteToken(it, padded))).join(" "))
  result = parts.join(" | ")

# ---------------------------------------------------------------------------
# Per-shard pipe execution
# ---------------------------------------------------------------------------

type InFlight = object
  ## Tracks one active shard: child process + writer thread.
  pid:      Pid
  writeFv:  FlowVar[int]
  shardIdx: int

proc forkExecSh(pipeReadFd: cint; pipeWriteFd: cint; stdoutFd: cint;
                shellCmd: string; shardIdx: int): Pid =
  ## Fork a child that runs sh -c shellCmd with stdin = pipeReadFd and
  ## stdout = stdoutFd.  stderr is inherited.  Returns child PID.
  ## pipeWriteFd is closed in the child so the child does not hold the
  ## write-end of its own stdin pipe (which would prevent EOF).
  let pid = posix.fork()
  if pid < 0:
    stderr.writeLine &"error: fork() failed for shard {shardIdx + 1}"
    quit(1)
  if pid == 0:
    # Child: rewire stdin and stdout, close the pipe write-end, exec shell.
    if posix.dup2(pipeReadFd, STDIN_FILENO) < 0 or
       posix.dup2(stdoutFd,   STDOUT_FILENO) < 0:
      exitnow(1)
    discard posix.close(pipeReadFd)
    discard posix.close(pipeWriteFd)
    discard posix.close(stdoutFd)
    let args = allocCStringArray(["sh", "-c", shellCmd])
    discard posix.execvp("sh", args)
    deallocCStringArray(args)
    exitnow(127)
  result = pid

proc killAll(running: seq[InFlight]) =
  ## Send SIGTERM to every in-flight child process.
  for s in running:
    discard posix.kill(s.pid, SIGTERM)

proc waitOne(running: var seq[InFlight]; failed: var bool) =
  ## Wait for any one child to finish; sync writer thread; record failure.
  var status: cint
  let donePid = posix.waitpid(-1, status, 0)
  let code    = int((status shr 8) and 0xff)
  var j = 0
  while j < running.len:
    if running[j].pid == donePid:
      discard ^running[j].writeFv
      if code != 0:
        stderr.writeLine &"shard {running[j].shardIdx + 1}: pipeline exited with code {code}"
        failed = true
      running.del(j)
      return
    j += 1

# ---------------------------------------------------------------------------
# Pipe setup, shard resolution, reap
# ---------------------------------------------------------------------------

type ShardPipes = object
  ## Per-shard pipe set: stdin pipe + output fd.
  stdinR*, stdinW*: cint
  outFileFd*:       cint

proc openShardPipes(shardIdx, nShards: int;
                    outputTemplate: string; toolManaged: bool): ShardPipes =
  ## Allocate the stdin pipe and per-shard output fd.
  result = ShardPipes(stdinR: -1, stdinW: -1, outFileFd: -1)
  var stdinPipe: array[2, cint]
  if posix.pipe(stdinPipe) != 0:
    stderr.writeLine &"error: pipe() failed for shard {shardIdx + 1}"
    quit(1)
  discard posix.fcntl(stdinPipe[1], F_SETFD, FD_CLOEXEC)
  result.stdinR = stdinPipe[0]
  result.stdinW = stdinPipe[1]
  if toolManaged:
    result.outFileFd = posix.open("/dev/null".cstring, O_WRONLY)
    if result.outFileFd < 0:
      stderr.writeLine &"error: could not open /dev/null for shard {shardIdx + 1}"
      quit(1)
  else:
    let outPath = shardOutputPath(outputTemplate, shardIdx, nShards)
    createDir(outPath.parentDir)
    result.outFileFd = posix.open(outPath.cstring,
                                  O_WRONLY or O_CREAT or O_TRUNC,
                                  0o666.Mode)
    if result.outFileFd < 0:
      stderr.writeLine &"error: could not create output file: {outPath}"
      quit(1)

proc forkShardChild(pipes: var ShardPipes; shellCmd: string;
                    shardIdx: int): Pid =
  ## Fork the child with stdin = pipe read-end, stdout = outFileFd.
  ## Closes parent's copies of child-owned fds after fork.
  result = forkExecSh(pipes.stdinR, pipes.stdinW, pipes.outFileFd,
                      shellCmd, shardIdx)
  discard posix.close(pipes.stdinR); pipes.stdinR = -1
  discard posix.close(pipes.outFileFd); pipes.outFileFd = -1

proc resolveShards(vcfPath: string; nShards, nThreads: int;
                   forceScan, clampShards: bool):
    tuple[tasks: seq[ShardTask]; nShards: int] =
  ## Thread-pool sizing + sequential shard computation.
  let actualThreads = if nThreads == 0: countProcessors() else: nThreads
  setMaxPoolSize(nShards)
  let fmt = if vcfPath.endsWith(".bcf"): ffBcf else: ffVcf
  let tasks = computeShards(vcfPath, nShards, actualThreads, forceScan, fmt,
                            clampShards)
  (tasks: tasks, nShards: tasks.len)

proc reapAll(inFlight: var seq[InFlight]; noKill: bool; anyFailed: var bool) =
  ## If a failure has been observed and --no-kill is off, SIGTERM any
  ## still-running children, then drain every remaining child via waitOne.
  if anyFailed and not noKill:
    killAll(inFlight)
  while inFlight.len > 0:
    waitOne(inFlight, anyFailed)

template drainLaunch(nShardsVal: int; noKillVal: bool;
                     inFlightVar: var seq[InFlight]; anyFailedVar: var bool;
                     body: untyped) =
  ## Common drain-launch loop: iterate `i` from 0 to nShards-1, draining one
  ## finished shard whenever the in-flight set is full, and stopping early on
  ## failure unless --no-kill is set.
  for i {.inject.} in 0 ..< nShardsVal:
    if anyFailedVar and not noKillVal: break
    while inFlightVar.len >= nShardsVal:
      waitOne(inFlightVar, anyFailedVar)
      if anyFailedVar and not noKillVal: break
    if anyFailedVar and not noKillVal: break
    body

# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

type RunPipelineCfg* = object
  ## Configuration for the run subcommand.
  vcfPath*:        string
  nShards*:        int
  nThreads*:       int
  forceScan*:      bool
  stages*:         seq[seq[string]]
  noKill*:         bool
  clampShards*:    bool
  outputTemplate*: string
  toolManaged*:    bool

proc runPipeline*(cfg: RunPipelineCfg) =
  ## Scatter into N shards, pipe each through the pipeline. Either
  ## discards shard stdout to /dev/null (tool-managed) or writes per-shard
  ## output files via cfg.outputTemplate.
  let (tasks, nShards) = resolveShards(cfg.vcfPath, cfg.nShards, cfg.nThreads,
                                       cfg.forceScan, cfg.clampShards)
  var anyFailed = false
  var inFlight: seq[InFlight]
  drainLaunch(nShards, cfg.noKill, inFlight, anyFailed):
    var pipes = openShardPipes(i, nShards, cfg.outputTemplate, cfg.toolManaged)
    let writerOutFd = pipes.stdinW
    let shardCmd = buildShellCmdForShard(cfg.stages, i, nShards)
    let pid = forkShardChild(pipes, shardCmd, i)
    var task = tasks[i]
    task.outFd = writerOutFd
    task.decompress = true
    let writeFv = spawn doWriteShard(task)
    inFlight.add(InFlight(pid: pid, writeFv: writeFv, shardIdx: i))
  reapAll(inFlight, cfg.noKill, anyFailed)
  if anyFailed: quit(1)
