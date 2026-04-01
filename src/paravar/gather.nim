## gather — format inference, interceptor, and concatenation for --gather.
##
## Implements the gather pipeline:
##   inferGatherFormat → GatherConfig → runInterceptor (per shard) → concatenateShards

import std/[options, os, strformat, strutils]
import std/posix
import bgzf_utils

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

type
  GatherFormat* = enum
    gfVcf, gfBcf, gfText

  GatherCompression* = enum
    gcBgzf, gcUncompressed

  GatherConfig* = object
    format*:        GatherFormat
    compression*:   GatherCompression
    outputPath*:    string
    tmpDir*:        string
    headerPattern*: Option[string]   ## --header-pattern; None = not set
    headerN*:       Option[int]      ## --header-n; None = not set
    shardCount*:    int

proc `$`*(f: GatherFormat): string =
  ## Human-readable format name for messages.
  case f
  of gfVcf:  "VCF"
  of gfBcf:  "BCF"
  of gfText: "text"

# ---------------------------------------------------------------------------
# Format inference
# ---------------------------------------------------------------------------

proc inferGatherFormat*(path: string; fmtOverride: string): (GatherFormat, GatherCompression) =
  ## Infer format and compression from the gather output path extension.
  ## fmtOverride ("vcf", "bcf", "txt") overrides format when non-empty; exits 1 if invalid.
  ## Compression: .gz / .bgz / .bcf → gcBgzf; anything else → gcUncompressed.
  ## Format (when no override):
  ##   .vcf.gz / .vcf.bgz → VCF+BGZF
  ##   .vcf               → VCF+Uncompressed
  ##   .bcf               → BCF+BGZF
  ##   .gz / .bgz         → Text+BGZF
  ##   anything else      → Text+Uncompressed (no error)

  # Compression is solely determined by the output path extension.
  let compression =
    if path.endsWith(".gz") or path.endsWith(".bgz") or path.endsWith(".bcf"): gcBgzf
    else: gcUncompressed

  # Format is from override if supplied; otherwise inferred from extension.
  var fmt: GatherFormat
  if fmtOverride != "":
    case fmtOverride
    of "vcf": fmt = gfVcf
    of "bcf": fmt = gfBcf
    of "txt": fmt = gfText
    else:
      stderr.writeLine &"error: --gather-fmt: invalid value '{fmtOverride}'" &
        " (expected vcf, bcf, or txt)"
      quit(1)
  else:
    if path.endsWith(".vcf.gz") or path.endsWith(".vcf.bgz"):
      fmt = gfVcf
    elif path.endsWith(".vcf"):
      fmt = gfVcf
    elif path.endsWith(".bcf"):
      fmt = gfBcf
    elif path.endsWith(".gz") or path.endsWith(".bgz"):
      fmt = gfText
    else:
      fmt = gfText  # unknown extension → text, no error

  result = (fmt, compression)

# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

proc validateGatherConfig*(cfg: GatherConfig) =
  ## Exits 1 if both headerPattern and headerN are set (mutually exclusive flags).
  ## Exits 1 if headerPattern or headerN is used with VCF or BCF format (text-only flags).
  if cfg.headerPattern.isSome and cfg.headerN.isSome:
    stderr.writeLine "error: --header-pattern and --header-n are mutually exclusive"
    quit(1)
  if cfg.format in {gfVcf, gfBcf} and (cfg.headerPattern.isSome or cfg.headerN.isSome):
    stderr.writeLine "error: --header-pattern and --header-n are only valid for text " &
      "format; VCF and BCF headers are stripped automatically"
    quit(1)

# ---------------------------------------------------------------------------
# G2 — Format sniffing
# ---------------------------------------------------------------------------

proc isBgzfStream*(firstBytes: openArray[byte]): bool =
  ## Return true if firstBytes begins with a BGZF block header (magic 1f 8b 08 04).
  firstBytes.len >= 4 and
  firstBytes[0] == 0x1f'u8 and firstBytes[1] == 0x8b'u8 and
  firstBytes[2] == 0x08'u8 and firstBytes[3] == 0x04'u8

proc sniffFormat*(firstBytes: openArray[byte]): GatherFormat =
  ## Detect format from uncompressed first bytes of a stream.
  ## BCF\x02\x02 → gfBcf; ##fileformat → gfVcf; anything else → gfText.
  if firstBytes.len >= 5 and
     firstBytes[0] == byte('B') and firstBytes[1] == byte('C') and
     firstBytes[2] == byte('F') and firstBytes[3] == 0x02'u8 and
     firstBytes[4] == 0x02'u8:
    return gfBcf
  const vcfMagic = "##fileformat"
  if firstBytes.len >= vcfMagic.len:
    var match = true
    for i in 0 ..< vcfMagic.len:
      if firstBytes[i] != byte(vcfMagic[i]):
        match = false
        break
    if match:
      return gfVcf
  result = gfText

proc sniffStreamFormat*(rawHead: openArray[byte]): (GatherFormat, bool) =
  ## Detect format and stream compression from the first bytes of a pipeline stdout.
  ## rawHead must contain at least the first complete BGZF block if the stream is BGZF.
  ## Returns (format, isBgzf).
  if isBgzfStream(rawHead):
    let decompressed = decompressBgzf(rawHead)
    result = (sniffFormat(decompressed), true)
  else:
    result = (sniffFormat(rawHead), false)

# ---------------------------------------------------------------------------
# G2 — Global detected format (set by shard 0 interceptor, read by shards 1..N)
# ---------------------------------------------------------------------------

var gDetectedFormat*: GatherFormat   ## Format detected from shard 0 stream.
var gFormatDetected*: bool = false   ## Whether gDetectedFormat has been written.
var gStreamIsBgzf*:   bool = false   ## Whether shard 0 stream was BGZF-compressed.

# ---------------------------------------------------------------------------
# G3 — Header stripping helpers
# ---------------------------------------------------------------------------

proc leU32At(data: openArray[byte]; pos: int): uint32 {.inline.} =
  ## Read little-endian uint32 from data at pos.
  data[pos].uint32 or (data[pos+1].uint32 shl 8) or
  (data[pos+2].uint32 shl 16) or (data[pos+3].uint32 shl 24)

proc decompressAllBgzfBlocks*(data: openArray[byte]): seq[byte] =
  ## Decompress every BGZF block in data into a single contiguous byte sequence.
  ## EOF blocks (ISIZE = 0) contribute nothing to the result.
  result = @[]
  var pos = 0
  while pos + 18 <= data.len:
    let blkSize = bgzfBlockSize(data.toOpenArray(pos, data.high))
    if blkSize <= 0 or pos + blkSize > data.len: break
    result.add(decompressBgzf(data.toOpenArray(pos, pos + blkSize - 1)))
    pos += blkSize

proc stripBcfHeader*(data: seq[byte]): seq[byte] =
  ## Strip BCF header (magic + l_text + header text) from uncompressed BCF bytes.
  ## Returns only the binary record bytes that follow the header.
  ## Returns @[] if data is too short to contain the full header or has no records.
  if data.len < 9: return @[]
  let lText = leU32At(data, 5).int
  let headerSize = 5 + 4 + lText
  if headerSize >= data.len: return @[]
  result = data[headerSize ..< data.len]

proc stripLinesByPattern*(data: seq[byte]; pattern: string): seq[byte] =
  ## Return data with every line whose first bytes match pattern removed.
  ## Lines shorter than pattern are kept. An empty pattern strips nothing.
  result = @[]
  if pattern.len == 0:
    result = data
    return
  var lineStart = 0
  for i in 0 ..< data.len:
    if data[i] == byte('\n'):
      let lineEnd = i + 1
      var match = (lineEnd - lineStart) >= pattern.len
      if match:
        for j in 0 ..< pattern.len:
          if data[lineStart + j] != byte(pattern[j]):
            match = false
            break
      if not match:
        result.add(data[lineStart ..< lineEnd])
      lineStart = lineEnd
  # Handle a trailing partial line (no final newline).
  if lineStart < data.len:
    let partial = data[lineStart ..< data.len]
    var match = partial.len >= pattern.len
    if match:
      for j in 0 ..< pattern.len:
        if partial[j] != byte(pattern[j]):
          match = false
          break
    if not match:
      result.add(partial)

proc stripFirstNLines*(data: seq[byte]; n: int): seq[byte] =
  ## Return data with the first n lines (including their newline) removed.
  var linesSkipped = 0
  var i = 0
  while i < data.len and linesSkipped < n:
    if data[i] == byte('\n'):
      linesSkipped += 1
    i += 1
  if i < data.len:
    result = data[i ..< data.len]
  else:
    result = @[]

# ---------------------------------------------------------------------------
# G3 — Header-end finders (used by the optimised BGZF shard path)
# ---------------------------------------------------------------------------

proc findVcfHeaderEnd*(data: seq[byte]): int =
  ## Return the byte index of the first byte of the first non-'#' line in data.
  ## Returns -1 if every byte seen so far belongs to '#' lines (header not yet complete).
  var i = 0
  while i < data.len:
    if data[i] != byte('#'):
      return i
    while i < data.len and data[i] != byte('\n'):
      i += 1
    i += 1  # skip '\n'
  return -1

proc findBcfHeaderEnd*(data: seq[byte]): int =
  ## Return 5 + 4 + l_text if data contains the full BCF header blob, else -1.
  if data.len < 9: return -1
  let headerEnd = 5 + 4 + leU32At(data, 5).int
  if data.len >= headerEnd: return headerEnd
  return -1

# ---------------------------------------------------------------------------
# Interceptor thread proc
# ---------------------------------------------------------------------------

proc runInterceptor*(cfg: GatherConfig; shardIdx: int; inputFd: cint; tmpPath: string): int =
  ## Per-shard interceptor thread proc. Reads from inputFd, strips headers for
  ## shards 2..N, recompresses if needed, writes to tmpPath. Returns 0.
  ## G2: format sniffing on shard 0.
  ## G3: header stripping for shards 1..N.
  ## G4: recompression (uncompressed↔BGZF) for all shards.
  let outFile = open(tmpPath, fmWrite)
  defer:
    outFile.close()
    discard posix.close(inputFd)

  const ChunkSize = 65536
  var buf = newSeq[byte](ChunkSize)

  # Read the first chunk; used for format sniffing on shard 0.
  let initRead = posix.read(inputFd, cast[pointer](addr buf[0]), ChunkSize)
  if initRead <= 0:
    return
  let head = buf[0 ..< initRead]

  if shardIdx == 0:
    let (fmt, isBgzf) = sniffStreamFormat(head)
    gDetectedFormat = fmt
    gStreamIsBgzf   = isBgzf
    gFormatDetected = true
    if fmt != cfg.format:
      stderr.writeLine &"warning: stream format detected as {fmt} " &
        &"but --gather expects {cfg.format}; proceeding"

  # Buffer the full stream (head + remainder).
  var allBytes: seq[byte]
  allBytes.add(head)
  while true:
    let got = posix.read(inputFd, cast[pointer](addr buf[0]), ChunkSize)
    if got <= 0: break
    allBytes.add(buf[0 ..< got])

  if shardIdx == 0:
    # Shard 0: no header stripping; apply recompression only.
    let toWrite: seq[byte] =
      if gStreamIsBgzf and cfg.compression == gcUncompressed:
        # BGZF input → uncompressed output: decompress.
        decompressAllBgzfBlocks(allBytes)
      elif not gStreamIsBgzf and cfg.compression == gcBgzf:
        # Uncompressed input → BGZF output: compress.
        compressToBgzfMulti(allBytes)
      else:
        # BGZF→BGZF or uncompressed→uncompressed: pass raw bytes through.
        # For BGZF streams: strip the terminal EOF block appended by the pipeline —
        # concatenateShards writes a single EOF block once at the very end.
        if gStreamIsBgzf and allBytes.len >= BGZF_EOF.len and
           allBytes[allBytes.len - BGZF_EOF.len ..< allBytes.len] == @BGZF_EOF:
          allBytes[0 ..< allBytes.len - BGZF_EOF.len]
        else:
          allBytes
    discard outFile.writeBytes(toWrite, 0, toWrite.len)
  else:
    # Shards 1..N: wait until shard 0 has finished sniffing (sets gFormatDetected).
    # Small shards may buffer all their pipeline output before shard 0 reads its
    # first chunk — spin (1 ms sleep per iteration) until the flag is ready.
    while not gFormatDetected:
      sleep(1)
    let fmt = gDetectedFormat
    if gStreamIsBgzf and fmt in {gfVcf, gfBcf}:
      # Optimised path: decompress only the header-containing blocks, then
      # raw-copy (or decompress-and-write) the remaining data blocks.
      var blockPos = 0
      var decompAccum: seq[byte]
      var headerEnd = -1
      # Phase 1: decompress blocks until the full header is in decompAccum.
      while blockPos < allBytes.len and headerEnd < 0:
        let blkSize = bgzfBlockSize(allBytes.toOpenArray(blockPos, allBytes.high))
        if blkSize <= 0: break
        decompAccum.add(
          decompressBgzf(allBytes.toOpenArray(blockPos, blockPos + blkSize - 1)))
        blockPos += blkSize
        headerEnd =
          if fmt == gfBcf: findBcfHeaderEnd(decompAccum)
          else:             findVcfHeaderEnd(decompAccum)
      if headerEnd < 0: headerEnd = decompAccum.len  # edge: all-header shard
      # Phase 2: write the post-header tail from the last decompressed block.
      let tail = decompAccum[headerEnd ..< decompAccum.len]
      if tail.len > 0:
        let chunk =
          if cfg.compression == gcBgzf: compressToBgzfMulti(tail)
          else: tail
        discard outFile.writeBytes(chunk, 0, chunk.len)
      # Phase 3: stream remaining blocks — raw pass-through for BGZF output,
      # decompress-and-write for uncompressed output.
      while blockPos < allBytes.len:
        let blkSize = bgzfBlockSize(allBytes.toOpenArray(blockPos, allBytes.high))
        if blkSize <= 0: break
        if cfg.compression == gcBgzf:
          discard outFile.writeBytes(allBytes, blockPos, blkSize)
        else:
          let d = decompressBgzf(allBytes.toOpenArray(blockPos, blockPos + blkSize - 1))
          discard outFile.writeBytes(d, 0, d.len)
        blockPos += blkSize
    else:
      # General path: uncompressed input or text+BGZF — decompress all, strip, write.
      let data: seq[byte] =
        if gStreamIsBgzf: decompressAllBgzfBlocks(allBytes)
        else: allBytes
      let stripped: seq[byte] =
        case fmt
        of gfBcf:
          stripBcfHeader(data)
        of gfVcf:
          stripLinesByPattern(data, "#")
        of gfText:
          if cfg.headerPattern.isSome:
            stripLinesByPattern(data, cfg.headerPattern.get)
          elif cfg.headerN.isSome:
            stripFirstNLines(data, cfg.headerN.get)
          else:
            data
      let toWrite: seq[byte] =
        if cfg.compression == gcBgzf: compressToBgzfMulti(stripped)
        else: stripped
      discard outFile.writeBytes(toWrite, 0, toWrite.len)
  result = 0

# ---------------------------------------------------------------------------
# G5 — cleanup and concatenation
# ---------------------------------------------------------------------------

proc cleanupTempDir*(tmpDir: string; tmpPaths: seq[string]; success: bool) =
  ## On success: delete every temp shard file then remove the temp dir.
  ## On failure: print the path of each temp file to stderr and leave them on disk.
  if success:
    for p in tmpPaths:
      try: removeFile(p) except OSError: discard
    try: removeDir(tmpDir) except OSError: discard
  else:
    stderr.writeLine "gather: temp files left on disk for debugging:"
    for p in tmpPaths:
      stderr.writeLine "  " & p

proc concatenateShards*(cfg: GatherConfig; tmpPaths: seq[string]) =
  ## Raw-copy each temp shard file (in order) into cfg.outputPath.
  ## Appends a single BGZF EOF block at the end when cfg.compression == gcBgzf.
  ## Calls cleanupTempDir on success.
  let outFile = open(cfg.outputPath, fmAppend)
  for p in tmpPaths:
    rawCopyBytes(p, outFile, 0, getFileSize(p))
  if cfg.compression == gcBgzf:
    discard outFile.writeBytes(BGZF_EOF, 0, BGZF_EOF.len)
  outFile.close()
  cleanupTempDir(cfg.tmpDir, tmpPaths, true)

# ---------------------------------------------------------------------------
# C3 — Direct-file gather (no temp dir)
# ---------------------------------------------------------------------------

proc gatherFiles*(cfg: GatherConfig; inputPaths: seq[string]) =
  ## Concatenate pre-existing shard files into cfg.outputPath.
  ## Shard 0 is written with its header intact; shards 1..N have headers stripped.
  ## No temp files are created.  Resets the global format-detection state.
  if inputPaths.len == 0:
    stderr.writeLine "error: gather: no input files provided"
    quit(1)
  gFormatDetected = false
  gDetectedFormat = gfText
  gStreamIsBgzf   = false
  let outFile = open(cfg.outputPath, fmWrite)
  for i, path in inputPaths:
    let fileSize = getFileSize(path)
    var allBytes = newSeq[byte](fileSize.int)
    let f = open(path, fmRead)
    discard readBytes(f, allBytes, 0, allBytes.len)
    f.close()
    # Format detection from shard 0.
    if i == 0:
      let (fmt, isBgzf) = sniffStreamFormat(allBytes)
      gDetectedFormat = fmt
      gStreamIsBgzf   = isBgzf
      gFormatDetected = true
      if fmt != cfg.format:
        stderr.writeLine &"warning: shard 0 format detected as {fmt} " &
          &"but output expects {cfg.format}; proceeding"
    # Strip the terminal BGZF EOF block if present — we write a single EOF at the end.
    var bytes = allBytes
    if gStreamIsBgzf and bytes.len >= BGZF_EOF.len and
       bytes[bytes.len - BGZF_EOF.len ..< bytes.len] == @BGZF_EOF:
      bytes = bytes[0 ..< bytes.len - BGZF_EOF.len]
    if i == 0:
      # Shard 0: no header stripping; apply recompression if needed.
      let toWrite: seq[byte] =
        if gStreamIsBgzf and cfg.compression == gcUncompressed:
          decompressAllBgzfBlocks(bytes)
        elif not gStreamIsBgzf and cfg.compression == gcBgzf:
          compressToBgzfMulti(bytes)
        else:
          bytes
      discard outFile.writeBytes(toWrite, 0, toWrite.len)
    else:
      # Shards 1..N: strip headers.
      let fmt = gDetectedFormat
      if gStreamIsBgzf and fmt in {gfVcf, gfBcf}:
        # Optimised path: decompress only the header-containing blocks, then
        # stream the remaining data blocks (EOF already stripped above).
        var blockPos = 0
        var decompAccum: seq[byte]
        var headerEnd = -1
        while blockPos < bytes.len and headerEnd < 0:
          let blkSize = bgzfBlockSize(bytes.toOpenArray(blockPos, bytes.high))
          if blkSize <= 0: break
          decompAccum.add(
            decompressBgzf(bytes.toOpenArray(blockPos, blockPos + blkSize - 1)))
          blockPos += blkSize
          headerEnd =
            if fmt == gfBcf: findBcfHeaderEnd(decompAccum)
            else:             findVcfHeaderEnd(decompAccum)
        if headerEnd < 0: headerEnd = decompAccum.len
        let tail = decompAccum[headerEnd ..< decompAccum.len]
        if tail.len > 0:
          let chunk =
            if cfg.compression == gcBgzf: compressToBgzfMulti(tail)
            else: tail
          discard outFile.writeBytes(chunk, 0, chunk.len)
        # Stream remaining data blocks.
        while blockPos < bytes.len:
          let blkSize = bgzfBlockSize(bytes.toOpenArray(blockPos, bytes.high))
          if blkSize <= 0: break
          if cfg.compression == gcBgzf:
            discard outFile.writeBytes(bytes, blockPos, blkSize)
          else:
            let d = decompressBgzf(bytes.toOpenArray(blockPos, blockPos + blkSize - 1))
            discard outFile.writeBytes(d, 0, d.len)
          blockPos += blkSize
      else:
        # General path: decompress all, strip, write.
        let data: seq[byte] =
          if gStreamIsBgzf: decompressAllBgzfBlocks(bytes)
          else: bytes
        let stripped: seq[byte] =
          case fmt
          of gfBcf: stripBcfHeader(data)
          of gfVcf: stripLinesByPattern(data, "#")
          of gfText:
            if cfg.headerPattern.isSome:
              stripLinesByPattern(data, cfg.headerPattern.get)
            elif cfg.headerN.isSome:
              stripFirstNLines(data, cfg.headerN.get)
            else:
              data
        let toWrite: seq[byte] =
          if cfg.compression == gcBgzf: compressToBgzfMulti(stripped)
          else: stripped
        discard outFile.writeBytes(toWrite, 0, toWrite.len)
  if cfg.compression == gcBgzf:
    discard outFile.writeBytes(BGZF_EOF, 0, BGZF_EOF.len)
  outFile.close()
