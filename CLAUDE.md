# CLAUDE.md — vcfparty

Agentic development instructions for Claude Code. Read this file in full before starting any task.

---

## Project overview

**vcfparty** (formerly `paravar`) is a Nim CLI tool for parallelising VCF/BCF processing by exploiting BGZF block boundaries and tabix/CSI indexes.

The canonical reference for scatter behaviour is `example/scatter_vcf.py`.

---

## Current implementation state

All of the following are fully implemented and tested:

- `scatter` — split VCF/BCF into N shards
- `run` — scatter + parallel tool pipelines → per-shard outputs
- `run --gather` — as above, gathered into single output via temp files
- `gather` — concatenate existing shard files (`--concat` default, `--merge` for merge-sorted output; similar to `bcftools concat -a`)
- BCF support (CSI index required)
- Stdout output for `gather` and `run --gather`
- `#CHROM` header validation
- `{}` tool-managed output mode with `\{}` escaping

---

## Subcommands (current)

| Subcommand | Description |
|---|---|
| `scatter` | Split VCF/BCF into N shards |
| `run` | Scatter + parallel pipelines → per-shard outputs |
| `run --gather` | As above, gathered into single `-o` file |
| `gather` | Concatenate existing shard files |

---

## Module layout

| File | Responsibility |
|---|---|
| `src/paravar.nim` | Entry point |
| `src/paravar/main.nim` | CLI parsing, subcommand dispatch |
| `src/paravar/scatter.nim` | Scatter algorithm (VCF + BCF) |
| `src/paravar/bgzf_utils.nim` | Low-level BGZF I/O, no external deps beyond `-lz` |
| `src/paravar/run.nim` | Run mode: pipeline spawning, worker pool, interceptor coordination |
| `src/paravar/gather.nim` | Format inference, sniffing, stripping, interceptor thread, concatenation |

Do not restructure the module layout without asking the user.

---

## Planned work — step-by-step plan

Execute steps in order. Do not skip ahead. Check off each step before proceeding.

---

### Milestone 0: rename to `vcfparty` ✅ complete

- [x] **V1** — Rename `paravar.nimble` → `vcfparty.nimble`. Update package name, binary name, and all internal `paravar` references in source files. Update all help text, error messages, and log output to say `vcfparty`. Update `README.md` and `CLAUDE.md` header. Run `nimble test` — all tests must pass with the new binary name.
- [x] **V2** — Update all test files: any hardcoded `paravar` binary invocations → `vcfparty`. Update `generate_fixtures.sh` if it references the binary. Run `nimble test` — full suite green.

---

### Milestone 1: interface consolidation ✅ complete

This milestone retires `--gather`, introduces terminal operators, consolidates `-n`/`-j`, adds `--concat`/`--merge` flags to `gather`, adds `-O` and `-d` flags, and aligns scatter/run feature symmetry. It is a pure refactor — no new algorithms.

#### Design reference

**Terminal operators** — appended after the last `:::` stage in `run`. `-o` is only valid when a terminal operator is present. Without a terminal operator, tool command must contain `{}`.

| Operator | Default scatter mode | Output order | Temp files |
|---|---|---|---|
| `+concat+` | Sequential | Genomic | Yes |
| `+merge+` | Interleaved (future) | Genomic | No |
| `+collect+` | Sequential | Arrival | No |
| none | Sequential | N/A | N/A (tool-managed via `{}`) |

**Scatter mode flags** — `-i`/`--interleave` and `-s`/`--sequential` on both `scatter` and `run`:

| Context | Default | Notes |
|---|---|---|
| `scatter` from indexed file | Sequential | `-i` allowed, warn: overlapping ranges |
| `scatter` from stdin (future) | Interleaved | `-s` allowed |
| `run +concat+` | Sequential | `-i` allowed, warn |
| `run +merge+` | Interleaved (future) | `-s` is an error |
| `run +collect+` | Sequential | `-i` allowed, warn |

**`-n`/`-j` consolidation** — retire `-j`/`--max-jobs`. `-n` controls both shard count and concurrent pipeline count. All N shards run concurrently.

**`-O` output format flag** (bcftools-style):

| Flag | Format | Compression |
|---|---|---|
| `-Ov` | VCF | Uncompressed |
| `-Oz` | VCF | BGZF |
| `-Ob` | BCF | BGZF |
| `-Ou` | BCF | Uncompressed |

Native (no bcftools): compress/decompress within same format. VCF↔BCF conversion delegated to bcftools if on PATH; if not found and conversion is required, exit 1 with a clear message suggesting an explicit `bcftools view` pipeline stage. Format inferred from `-o` extension when `-O` absent. `-O` overrides with a warning on extension mismatch. Stdout with no `-o` and no `-O`: uncompressed matching detected stream format.

**`-d`/`--decompress` flag** — on both `scatter` and `run`. In sequential mode, decompress BGZF blocks before piping to subprocess stdin rather than raw-copying. Eliminates boundary block recompression and subprocess decompression cost. In interleaved mode (future `+merge+`) decompression is already implicit — accept silently as a no-op.

#### Steps

- [x] **I1** — Retire `-j`/`--max-jobs` from `run` in `main.nim`. Remove all references in `run.nim`. Update help text. Add an error if `-j` is passed: `"error: -j is no longer supported; use -n to control both shard count and concurrency"`. Run `nimble test` — update any test that used `-j`.

- [x] **I2** — Implement terminal operator parsing in `main.nim`. Scan `run` argv for tokens exactly equal to `+concat+`, `+merge+`, or `+collect+`. Everything after the last `:::` / `---` / `+verb+` token is parsed accordingly. Validate: `-o` without a terminal operator is an error; no terminal operator and no `{}` in tool command is an error; `+merge+` with `-s` is an error. Unit test the parser with all valid and error cases — do not wire up any new output behaviour yet, just parse and validate.

- [x] **I3** — Wire `+concat+` as the replacement for `--gather`. `+concat+` uses the existing gather/temp-file path in `gather.nim` unchanged. `--gather` is retired — add a clear error if passed: `"error: --gather is retired; use +concat+ instead"`. Update `test_run.nim` and `test_cli.nim` to use `+concat+` syntax. Run `nimble test`.

- [x] **I4** — Implement `+collect+` streaming output in `run.nim`. Each shard's pipeline stdout is read in a dedicated thread. As complete records arrive (VCF: `\n`-terminated; BCF: `l_shared + l_indiv` bytes), they are immediately written to the output fd (stdout or `-o` file) under a mutex. No temp files. No ordering guarantee. Write `test_collect.nim` covering: single shard, 4 shards, BCF, stdout, all records present (order-insensitive check). Run `nimble test`.

- [x] **I5** — Add `--concat` (default) and `--merge` flags to the `gather` subcommand in `main.nim`. `gather` subcommand keeps its name. `--merge` is a no-op for now — accepted and noted but falls back to `--concat` behaviour with a warning: `"warning: --merge not yet implemented, using --concat"`. Note in help text that `vcfparty gather` is similar to `bcftools concat -a`. Update `test_gather.nim` to cover `--concat` and `--merge` (warning) flags. Run `nimble test`.

- [x] **I6** — Add `-i`/`--interleave` and `-s`/`--sequential` flags to both `scatter` and `run` in `main.nim`. For now, both flags are parsed and stored but only `-s` (sequential) actually changes behaviour — `-i` on an indexed file emits the overlapping-ranges warning and proceeds with sequential scatter (interleaved scatter is implemented in Milestone 3). Document this in help text. Run `nimble test`.

- [x] **I7** — Add `-O` flag to `run` and `scatter` in `main.nim`. Implement compression-only cases natively in `gather.nim` (BGZF↔uncompressed within same format). For VCF↔BCF conversion: detect if bcftools is on PATH (`findExe("bcftools")`); if yes, insert `bcftools view -O<fmt>` as an implicit final stage after the terminal operator; if no, exit 1 with a clear message. Write unit tests for each `-O` case: same-format compress, same-format decompress, cross-format with bcftools mock, cross-format without bcftools (error). Run `nimble test`.

- [x] **I8** — Add `-d`/`--decompress` flag to `run` and `scatter`. In sequential mode: decompress BGZF blocks before writing to subprocess stdin rather than raw-copying. In interleaved mode (future): accept silently as no-op. Update `scatter.nim` to support the decompress path alongside the existing raw-copy path. Write tests: `-d` with VCF produces valid uncompressed VCF shard files; `-d` with BCF produces valid uncompressed BCF; `-d` with `+collect+` all records present. Run `nimble test`.

- [x] **I9** — Full integration test pass. Write `test_interface.nim` covering the complete new CLI surface: all terminal operators, `-O` flag, `-d` flag, `-i`/`-s` flags, `gather --concat`/`gather --merge` flags, retired `--gather` and `-j` errors. Run full `nimble test` — all suites green.

---

### Milestone 2: `+merge+` and `+collect+` (sequential) ✅ complete

Implements the merge sorter for `+merge+` using sequential (contiguous) scatter. This is limited — it will block on the slowest shard — but gets the merge sorter working and tested before interleaved scatter is added in Milestone 3.

#### Design

The merge sorter reads the current head record from each subprocess stdout stream, compares by `(contig_rank, pos)`, and emits the minimum. One record per stream held in a priority queue — O(1) memory per stream.

- **BCF**: read `chrom_id` (int32 LE at uncompressed byte offset 8 of each record), look up contig rank from the header contig list
- **VCF**: parse CHROM from first `\t`-delimited field, look up in contig table built from `##contig` header lines
- Contig table is extracted at scatter time from the input file header and passed to the merge sorter

Requires uncompressed pipeline output (`-Ou` or `-Ov`) from the last stage. If BGZF output is detected, decompress transparently with a warning: `"warning: +merge+ works best with uncompressed output (-Ou/-Ov) from the last pipeline stage"`.

#### Steps

- [x] **M1** — Implement `extractContigTable(headerBytes: seq[byte]): seq[string]` in `gather.nim` — returns ordered contig names from VCF header `##contig` lines or BCF header blob. Unit test against `small.vcf.gz` and `small.bcf` headers. Run relevant test file.

- [x] **M2** — Implement `readNextVcfRecord(fd: cint): seq[byte]` and `readNextBcfRecord(fd: cint): seq[byte]` in `gather.nim` — read exactly one complete record from an fd (VCF: read until `\n`; BCF: read 8-byte header, then `l_shared + l_indiv` bytes). Return empty seq on EOF. Unit test with synthetic byte sequences. Run test file.

- [x] **M3** — Implement `extractSortKey(record: seq[byte], fmt: GatherFormat, contigTable: seq[string]): (int, int32)` in `gather.nim` — returns `(contig_rank, pos)` from a record. For BCF: read int32 at offset 0 (chrom_id) and int32 at offset 8 (pos). For VCF: split on `\t`, look up CHROM in contig table. Unit test on known records from `small.vcf.gz` and `small.bcf`. Run test file.

- [x] **M4** — Implement `kWayMerge(fds: seq[cint], outFd: cint, fmt: GatherFormat, contigTable: seq[string])` in `gather.nim` — priority queue merge. Initialise by reading first record from each fd. Loop: pop minimum, write to outFd, read next from that fd. Handle EOF per stream. Unit test: merge 2 synthetic sorted VCF streams → verify output is sorted and contains all records. Run test file.

- [x] **M5** — Wire `+merge+` into `run.nim`. After all N subprocess pipelines complete, call `kWayMerge` with each subprocess's stdout fd and the `-o` output fd. For sequential scatter: warn that merge may block on slowest shard (this is expected until interleaved scatter is implemented). Write `test_merge.nim`: 4 shards VCF, `+merge+`, output sorted and record-complete (sha256 vs sorted baseline); BCF equivalent; stdout output; BGZF pipeline output (triggers decompress warning). Run `nimble test`.

- [x] **M6** — Implement `gather --merge` in the `gather` subcommand: wire `kWayMerge` from existing shard files rather than live fds. Read each shard file, open as fd, pass to `kWayMerge`. Remove the "not yet implemented" warning added in I5. Unit test: `vcfparty gather --merge` on 4 shard files → sorted output matches `bcftools concat -a` output. Run `nimble test`.

---

### Milestone R: refactor and consolidation

This milestone is a structured cleanup pass before Milestone 3 adds new concurrency complexity. The goal is to reduce duplication, unify patterns, fix latent bugs, and leave the codebase in a state where Milestones 3 and 4 can be implemented without fighting the existing structure.

**Zero behaviour changes** (except latent bug fixes). All existing tests must pass unchanged throughout. No new features.

#### Step R0 — self-audit ✅ complete

Audit completed and approved. Findings below drive R1–R7.

**Confirmed dead code (verify in tests/ before removing):**
- `computeZeroBytes` and `computeDataBytes` in `gather.nim` — suspected dead; confirm no call sites in `tests/`
- `bcfFirstDataOffset` in `bgzf_utils.nim` — suspected dead; confirm no call sites in `tests/` or `src/`

**Confirmed issues to fix:**
- `doMergeFeeder` (`run.nim`) and `doFileFeeder` (`gather.nim`) are near-identical — unify into one proc in `gather.nim`
- `doCollectInterceptor` contains a third copy of the BGZF-accumulation pattern — consolidate
- `leU32At` in `gather.nim` duplicates `leU32` in `bgzf_utils.nim` — export `leU32*` and remove duplicate
- `writeShardZero`/`writeShardData` vs `computeZeroBytes`/`computeDataBytes` — dead compute variants to be removed; write variants survive if still called from `gatherFiles`
- All GC-safe globals (`gChromLine*`, `gMergeHeader*`, `gMergeFormat`, `gMergeBgzfWarned`) lack a shared abstraction; `gMergeBgzfWarned` lives in wrong file
- `FD_CLOEXEC` missing on pipe write-ends in `runShards`, `runShardsGather`, `runShardsCollect` (set correctly only in `runShardsMerge`)
- `runShardsMerge` missing `noKill` support and kill-all on failure — inconsistent with other orchestrators
- `killAllGather` and `killAllCollect` have identical bodies — consolidate
- `waitOne*` procs are structurally identical across orchestrator types — consolidate
- `readNextVcfRecord` reads one byte per syscall — **note in a comment, defer fix to Milestone 3**
- `optimiseBoundaries` O(n²) `notin` check on `seq[int64]` — **note in a comment, defer to later**

- [x] **R0** — Audit complete and approved.

#### Step R1 — call-site verification before cleanup

Before removing anything, search all files under `src/` and `tests/` for call sites of the suspected dead procs:

1. `computeZeroBytes` and `computeDataBytes` — if unused everywhere, mark for removal in R6
2. `bcfFirstDataOffset` — if unused everywhere, mark for removal in R6
3. Confirm `writeShardZero` and `writeShardData` are still called from `gatherFiles` (or wherever)
4. Confirm `leU32At` is only used within `gather.nim` and not re-exported

Show results. Update the CLAUDE.md M5 and M6 checkboxes to `[x]`. No code changes yet.

- [ ] **R1** — Call-site verification complete. Dead procs identified. M5/M6 checkboxes updated.

#### Step R2 — export `leU32` and unify the BGZF accumulation pattern

Two changes:

1. Add `*` to `leU32` in `bgzf_utils.nim`. Replace `leU32At` in `gather.nim` with calls to `bgzf_utils.leU32`. Remove the private duplicate.

2. The BGZF-accumulation pattern (`flushBgzf`/`appendRead` templates or equivalents) appears in `doMergeFeeder`, `doFileFeeder`, and `doCollectInterceptor`. Extract it into a shared proc or template in `gather.nim`. All three callers use the shared version.

Before implementing step 2: show the proposed shared abstraction signature and all three call sites. Wait for approval.

- [ ] **R2** — `leU32` exported from `bgzf_utils`. BGZF accumulation pattern unified. All tests pass.

#### Step R3 — unify GC-safe globals

The fixed-size-array pattern (`array[N, byte]` + `int32` length + `bool` spin flag) appears for `gChromLine*` and `gMergeHeader*`. A third instance will be needed in Milestone 3. Define a single reusable type, e.g.:

```nim
type SharedBuf* = object
  buf*: array[4 * 1024 * 1024, byte]
  len*: int32
  ready*: bool
```

Move all merge-related globals (`gMergeFormat`, `gMergeHeaderAvail`, `gMergeHeaderBuf`, `gMergeHeaderLen`, `gMergeBgzfWarned`) to `gather.nim` — they currently straddle both files. Apply `SharedBuf` to both `gChromLine*` and `gMergeHeader*`. Verify spin-wait pattern is consistently applied at all read sites.

Before implementing: confirm Nim version and whether `std/atomics` is available for a cleaner atomic flag. Check `vcfparty.nimble` for the Nim version constraint.

- [ ] **R3** — GC-safe globals unified under `SharedBuf`. All merge globals in `gather.nim`. All tests pass.

#### Step R4 — unify orchestrator fork/exec structure

Extract `spawnShardPipelines` from the common structure shared by `runShards`, `runShardsGather`, `runShardsMerge`, and `runShardsCollect`. The shared proc handles: shard computation, pipe creation, fork/exec, fd lifetime management, and `doWriteShard` spawning. Each orchestrator calls it and handles subprocess stdout differently.

Consolidate `killAllGather` and `killAllCollect` into a single `killAll(running: openArray[...])` proc. Consolidate `waitOneGather`, `waitOneMerge`, `waitOneCollect` into a shared `waitOne` proc parameterised on the in-flight type, or restructure `InFlight` into a single type with a variant field.

Add `noKill` support and kill-all on failure to `runShardsMerge` — it is currently missing both.

**Critically:** set `FD_CLOEXEC` on all pipe write-ends inside `spawnShardPipelines`. This fixes the latent fd inheritance issue in `runShards`, `runShardsGather`, and `runShardsCollect`.

Before implementing: show the proposed `spawnShardPipelines` signature and the refactored orchestrator skeletons. Wait for approval.

- [ ] **R4** — `spawnShardPipelines` extracted. All orchestrators use it. `FD_CLOEXEC` on all write-ends. `noKill` and kill-all in `runShardsMerge`. All tests pass.

#### Step R5 — FD inheritance regression test

Write a specific test that exercises `+concat+` with N=4 shards and a subprocess that pauses briefly before producing output (e.g. `sh -c 'sleep 0.05 && cat'`). This surfaces the fd inheritance bug if `FD_CLOEXEC` was not correctly applied. Confirm the test passes.

- [ ] **R5** — Regression test added and passing. `+concat+` FD inheritance verified clean.

#### Step R6 — dead code removal and comments

Remove all confirmed-dead procs from R1: `computeZeroBytes`, `computeDataBytes`, `bcfFirstDataOffset` (if confirmed unused). Remove unused imports exposed by the refactor.

Add comments to the two deferred performance issues:
- `readNextVcfRecord`: `# TODO: reads one byte per syscall; buffer in Milestone 3`
- `optimiseBoundaries` `notin` check: `# TODO: use HashSet[int64] to avoid O(n²) behaviour`

Run `nimble build -d:release` and confirm no warnings.

- [ ] **R6** — Dead code removed. Deferred issues commented. Clean release build. All tests pass.

#### Step R7 — final verification

Run `nimble test` in full. Confirm no behaviour changes from before Milestone R.

- [ ] **R7** — Full test suite green. Milestone R complete. Ready for Milestone 3.

---

### Milestone C: CLI audit and compression flags

This milestone audits the current CLI implementation for inconsistencies, then adds `-Oz`/`-Ou` (output compression) and `-Iu` (pipeline input decompression). `--decompress`/`-d` is retired.

#### Step C0 — CLI audit

Before implementing any new flags, read `src/vcfparty/main.nim` and `src/vcfparty/run.nim` in full and audit the current CLI surface:

1. **Flag inventory** — list every flag currently accepted by each subcommand (`scatter`, `run`, `gather`), its long form, default value, and where it is handled in code.
2. **`--decompress`/`-d`** — confirm it is still present and identify every code path that reads it in `main.nim`, `run.nim`, and `scatter.nim`.
3. **Output path handling** — trace how `-o` is used in each subcommand. Is extension inference for output format already implemented from Milestone 1 (`-O` work)? What format/compression is currently written when no `-O` is specified?
4. **Stdout detection** — how is stdout currently detected (omitted `-o`, `/dev/stdout`)? Is uncompressed output already enforced for stdout, or is this still to be implemented?
5. **Inconsistencies** — flag any inconsistencies between subcommands (e.g. flags present on `run` but missing on `scatter`, mismatched defaults, help text that doesn't match behaviour).

Present findings to the user. Wait for approval before C1.

- [ ] **C0** — CLI audit complete, inconsistencies identified, approved.

#### Step C1 — retire `--decompress`/`-d`, add `-Iu`

Remove `--decompress`/`-d` from `main.nim`, `run.nim`, and `scatter.nim`. Replace all call sites with the new `-Iu` flag:

- `-Iu`: valid on `run` only, sequential scatter only
- Semantics: decompress BGZF blocks before piping to subprocess stdin (same behaviour as old `--decompress`)
- If specified with interleaved scatter: warn `"warning: -Iu has no effect with interleaved scatter (always pipes uncompressed)"` and proceed
- If specified with `--stdin`: warn `"warning: -Iu has no effect with --stdin (always pipes uncompressed)"` and proceed
- Not valid on `scatter` or `gather`: error if passed

Update help text for all subcommands. Update `test_interface.nim` and any other tests that reference `--decompress`/`-d`. Run `nimble test` — all tests pass.

- [ ] **C1** — `--decompress` retired. `-Iu` implemented. All tests pass.

#### Step C2 — add `-Oz`/`-Ou` output compression flags

Add `-Oz` and `-Ou` to `scatter`, `run`, and `gather`:

| Flag | Meaning |
|---|---|
| `-Oz` | Force BGZF-compressed output |
| `-Ou` | Force uncompressed output |

**Default behaviour (no flag):**
- Infer from `-o` extension: `.vcf.gz`, `.bcf`, `.gz`, `.bgz` → BGZF; `.vcf`, `.bcf` uncompressed variant, anything else → uncompressed
- Stdout (no `-o` or `/dev/stdout`): uncompressed

**Warnings:**
- `-Oz` with an extension that implies uncompressed (e.g. `-o out.vcf -Oz`): warn `"warning: -Oz specified but output extension suggests uncompressed"`
- `-Ou` with an extension that implies compressed (e.g. `-o out.vcf.gz -Ou`): warn `"warning: -Ou specified but output extension suggests BGZF"`

**Error:**
- `-Oz` and `-Ou` together: error `"error: -Oz and -Ou are mutually exclusive"`

Implement by wiring `-Oz`/`-Ou` into the `GatherCompression` value passed to all output-writing paths. Where extension inference already exists (from Milestone I7), `-Oz`/`-Ou` overrides it. Where it does not exist yet, implement it now.

Write tests covering: `-Oz` produces valid BGZF output; `-Ou` produces uncompressed output; extension mismatch warnings; mutual exclusion error; stdout always uncompressed regardless of flag. Run `nimble test`.

- [ ] **C2** — `-Oz`/`-Ou` implemented on all subcommands. Extension inference consistent. All tests pass.

#### Step C3 — address audit inconsistencies

Fix any inconsistencies identified in C0 that are not already resolved by C1/C2. Likely candidates based on the audit findings:

- Flags present on `run` but missing on `scatter` or `gather` (or vice versa) where they should be symmetric
- Help text that does not match current behaviour
- Any subcommand that does not yet enforce the stdout-must-be-uncompressed rule

Each fix: minimal change, test added or updated, `nimble test` passes.

- [ ] **C3** — All audit inconsistencies resolved. Full `nimble test` green. Ready for Milestone 3.

---

### Milestone 3: interleaved splitting

Implements round-robin block assignment as the default scatter strategy for `+merge+`. Eliminates stalling on the slowest shard. No recompression at chunk boundaries — decompressed bytes are piped directly.

#### Design

Chunk size K defaults to `ceil(total_blocks / (n * 10))` — giving ~10 interleaved chunks per subprocess. Tunable: `-i K` takes an optional integer argument.

Block assignment:
```
chunk 0 (blocks 0..K-1)     → subprocess 0
chunk 1 (blocks K..2K-1)    → subprocess 1
...
chunk n-1                   → subprocess n-1
chunk n                     → subprocess 0  (wraps)
```

No recompression: each chunk's BGZF blocks are decompressed and piped as raw bytes. The subprocess receives a continuous uncompressed stream. This is correct because `+merge+` requires uncompressed output from the subprocess anyway.

#### Steps

- [ ] **L1** — Implement `interleavedBlockAssignment(starts: seq[int64], n: int, K: int): seq[seq[int64]]` in `scatter.nim` — returns N sequences of block start offsets, one per subprocess, in round-robin order. Unit test: known block list, verify each block assigned exactly once and round-robin order is correct. Run test file.

- [ ] **L2** — Implement `writeInterleavedShard(path: string, blockOffsets: seq[int64], headerBytes: seq[byte], outFd: cint)` in `scatter.nim` — for each block in the shard's offset list: read raw BGZF block, decompress, write raw bytes to outFd. Prepend recompressed header to the first chunk only (subprocess 0's first chunk = shard 0's header, all others = header also since each subprocess gets a full header). No BGZF EOF block — subprocess receives a raw byte stream. Unit test: two-shard interleaved scatter, concatenate decompressed output, verify all records present. Run test file.

- [ ] **L3** — Wire interleaved scatter into `run.nim` for `+merge+`. When `+merge+` is the terminal operator and `-s` is not set, use `interleavedBlockAssignment` and `writeInterleavedShard` rather than the sequential scatter path. Each subprocess receives decompressed bytes. After all subprocesses complete, call `kWayMerge`. Write integration tests in `test_merge.nim`: interleaved `+merge+` on `small.vcf.gz` 8 shards, output sorted and record-complete; BCF equivalent; `-s` with `+merge+` errors cleanly; warn message for `-i` with `+concat+`. Run `nimble test`.

- [ ] **L4** — Implement chunk size tuning. Parse optional integer after `-i`: `-i` alone uses default K; `-i 20` sets K=20. Validate K ≥ 1. Add test for explicit K: interleaved scatter with K=5 produces same records as K=default. Run `nimble test`.

- [ ] **L5** — Implement interleaved scatter for `scatter` subcommand (not just `run`). When `-i` is passed to `scatter`, use `interleavedBlockAssignment` and write decompressed shard files (or BGZF if `-d` is not passed — wait, interleaved scatter always decompresses, so shard files are uncompressed raw VCF/BCF). Emit warning about overlapping ranges. Write tests: `scatter -i` on `small.vcf.gz`, verify shard files are uncompressed VCF, all records present across shards, overlapping warning emitted. Run `nimble test`.

- [ ] **L6** — Update `concat --merge` to handle interleaved shard files (uncompressed VCF/BCF rather than BGZF). Detect format from first bytes. Run `nimble test` — full suite green.

---

### Milestone 4: stdin splitting

Implements `--stdin` flag. Accepts non-seekable stream as input, splits on the fly using a BGZF decompressor thread pool, pipes shards to subprocesses.

#### Design

```
stdin
  |
[block reader thread] → raw BGZF blocks → [decompressor pool, N threads]
                                                    |
                                          decompressed ring buffer
                                                    |
                               [demux thread] → subprocess 0 stdin pipe
                                            |→ subprocess 1 stdin pipe
                                            |→ subprocess N stdin pipe
                                                    |
                                        [+merge+ or +collect+]
```

Ring buffer: bounded, blocking. Block reader fills; decompressor threads drain and write decompressed chunks; demux thread reads decompressed chunks and routes to the current shard's subprocess pipe. Shard switching happens at record boundaries (VCF: `\n`; BCF: record length bytes).

`+concat+` is not valid with `--stdin` (no ordering guarantee). Error if attempted.

Format auto-detected from first bytes: BGZF magic → decompress → check for `BCF\x02\x02` or `##fileformat`.

#### Steps

- [ ] **X1** — Implement `RingBuffer` in a new `src/vcfparty/stdin_split.nim`. Fixed-size bounded byte buffer with blocking `write` (blocks if full) and blocking `read` (blocks until data available). Unit test: producer thread writes, consumer thread reads, verify all bytes received in order. Run test file.

- [ ] **X2** — Implement block reader thread: reads raw BGZF blocks from stdin fd into the ring buffer. Handles EOF (closes buffer). Unit test: pipe a known BGZF file through the reader, verify block boundaries are preserved. Run test file.

- [ ] **X3** — Implement decompressor thread pool: N threads each pop a raw BGZF block from the ring buffer, decompress, and write decompressed bytes to a second ring buffer. Thread count configurable. Unit test: decompress known BGZF file via pool, verify decompressed output matches `bgzip -d`. Run test file.

- [ ] **X4** — Implement demux thread: reads decompressed bytes from the second ring buffer, scans for record boundaries (VCF: `\n`; BCF: read length prefix), routes complete records to the current subprocess's stdin pipe. Switches subprocess after every K records (K = total_estimated_records / n, re-estimated from block count). Write header to each subprocess's pipe before any records. Implement backpressure: if current subprocess pipe is full, try next available subprocess (non-blocking write with fallback). Unit test: synthetic decompressed VCF byte stream, verify correct record routing to N output fds. Run test file.

- [ ] **X5** — Wire `--stdin` into `run.nim`: detect `--stdin` flag, call `stdin_split.nim` orchestration instead of `scatter.nim`. Validate: `+concat+` with `--stdin` is an error. Integrate with `+merge+` and `+collect+`. Write `test_stdin.nim`: pipe `small.vcf.gz` through `vcfparty run --stdin -n 4 ::: cat +collect+`, verify all records present; pipe through `+merge+`, verify sorted output; BCF equivalent; `+concat+` error; backpressure test (slow subprocess with fast input). Run `nimble test` — full suite green.

- [ ] **X6** — Implement BCF stdin splitting in the demux thread: walk `l_shared + l_indiv` byte boundaries rather than scanning for `\n`. Unit test: pipe `small.bcf` through `--stdin`, verify all records present. Run `nimble test`.

- [ ] **X7** — Implement uncompressed VCF stdin: if no BGZF magic detected, skip block reader and decompressor pool entirely, read raw bytes from stdin and scan for `\n` boundaries in the demux thread. Unit test: pipe uncompressed VCF through `--stdin`, verify correct output. Run `nimble test` — full suite green.

---

## Workflow rules for Claude Code

### Before starting any task

1. Re-read this file in full
2. Read the relevant source files
3. Consult `example/scatter_vcf.py` for scatter behaviour questions
4. Check which milestone step is next — do not skip ahead

### Hard rules

| Rule | Detail |
|---|---|
| **No new dependencies** | Do not add to `vcfparty.nimble` without asking |
| **Test before done** | Run `nimble test` and show full output before declaring any step complete |
| **No commits** | Stage changes, propose commit message, wait for user |
| **No layout changes** | Do not restructure modules without asking |
| **One proc at a time** | Implement, test, then proceed — never write 200+ lines without a test checkpoint |
| **Ask when uncertain** | Behaviour not covered here → stop and ask |

---

## Build reference

```bash
nimble build
nimble build -d:release
nimble test
nim c -d:debug -r tests/test_merge.nim   # single test file
```