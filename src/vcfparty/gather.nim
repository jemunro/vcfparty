## gather — format inference and concatenation for gather subcommand.

import std/[os, strformat, strutils]
import std/posix
import vcf_utils

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

type
  GatherConfig* = object
    format*:        FileFormat
    compression*:   Compression
    outputPath*:    string
    shardCount*:    int
    toStdout*:      bool             ## write output to stdout instead of outputPath

# ---------------------------------------------------------------------------
# Format inference
# ---------------------------------------------------------------------------

proc inferFileFormat*(path: string; fmtOverride: string): (FileFormat, Compression) =
  ## Infer format and compression from the gather output path extension.
  ## fmtOverride ("vcf", "bcf", "txt") overrides format when non-empty; exits 1 if invalid.
  ## Compression: .gz / .bgz / .bcf → compBgzf; anything else → compNone.
  ## Format (when no override):
  ##   .vcf.gz / .vcf.bgz → VCF+BGZF
  ##   .vcf               → VCF+Uncompressed
  ##   .bcf               → BCF+BGZF
  ##   .gz / .bgz         → Text+BGZF
  ##   anything else      → Text+Uncompressed (no error)

  # Compression is solely determined by the output path extension.
  let compression =
    if path.endsWith(".gz") or path.endsWith(".bgz") or path.endsWith(".bcf"): compBgzf
    else: compNone

  # Format is from override if supplied; otherwise inferred from extension.
  var fmt: FileFormat
  if fmtOverride != "":
    case fmtOverride
    of "vcf": fmt = ffVcf
    of "bcf": fmt = ffBcf
    of "txt": fmt = ffText
    else:
      stderr.writeLine &"error: --gather-fmt: invalid value '{fmtOverride}'" &
        " (expected vcf, bcf, or txt)"
      quit(1)
  else:
    if path.endsWith(".vcf.gz") or path.endsWith(".vcf.bgz"):
      fmt = ffVcf
    elif path.endsWith(".vcf"):
      fmt = ffVcf
    elif path.endsWith(".bcf"):
      fmt = ffBcf
    elif path.endsWith(".gz") or path.endsWith(".bgz"):
      fmt = ffText
    else:
      fmt = ffText  # unknown extension → text, no error

  result = (fmt, compression)

# ---------------------------------------------------------------------------
# Header stripping helpers
# ---------------------------------------------------------------------------

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
  let lText = leU32(data, 5).int
  let headerSize = 5 + 4 + lText
  if headerSize >= data.len: return @[]
  result = data[headerSize ..< data.len]

# ---------------------------------------------------------------------------
# Header-end finders
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
  let headerEnd = 5 + 4 + leU32(data, 5).int
  if data.len >= headerEnd: return headerEnd
  return -1

# ---------------------------------------------------------------------------
# #CHROM line extraction helpers
# ---------------------------------------------------------------------------

proc extractChromLine*(data: seq[byte]): string =
  ## Return the first line starting with "#CHROM" from uncompressed bytes.
  ## Returns "" if not found.
  const chromPrefix = "#CHROM"
  var i = 0
  while i < data.len:
    var j = i
    while j < data.len and data[j] != byte('\n'): j += 1
    let lineLen = j - i
    if lineLen >= chromPrefix.len:
      var match = true
      for k in 0 ..< chromPrefix.len:
        if data[i + k] != byte(chromPrefix[k]):
          match = false; break
      if match:
        var s = newString(lineLen)
        for k in 0 ..< lineLen: s[k] = char(data[i + k])
        return s
    i = j + 1
  return ""

proc chromLineFromBytes*(rawBytes: seq[byte]; fmt: FileFormat; isBgzf: bool): string =
  ## Extract the #CHROM line from raw (possibly BGZF-compressed) shard bytes.
  ## Decompresses only enough BGZF blocks to find the header boundary.
  ## Returns "" for text format or if not found.
  if fmt == ffText: return ""
  if isBgzf:
    var decompBuf: seq[byte]
    var pos = 0
    while pos < rawBytes.len:
      let blkSize = bgzfBlockSize(rawBytes.toOpenArray(pos, rawBytes.high))
      if blkSize <= 0: break
      decompBuf.add(decompressBgzf(rawBytes.toOpenArray(pos, pos + blkSize - 1)))
      pos += blkSize
      let hEnd =
        if fmt == ffBcf: findBcfHeaderEnd(decompBuf)
        else:            findVcfHeaderEnd(decompBuf)
      if hEnd >= 0:
        return extractChromLine(decompBuf[0 ..< hEnd])
    return extractChromLine(decompBuf)
  else:
    let hEnd =
      if fmt == ffBcf: findBcfHeaderEnd(rawBytes)
      else:            findVcfHeaderEnd(rawBytes)
    let limit = if hEnd >= 0: hEnd else: rawBytes.len
    return extractChromLine(rawBytes[0 ..< limit])

proc chromLineFromFile*(path: string; fmt: FileFormat; isBgzf: bool): string =
  ## Read just enough bytes from path to extract the #CHROM line.
  ## Returns "" for text format or if not found within the header.
  if fmt == ffText: return ""
  let f = open(path, fmRead)
  defer: f.close()
  const BufSize = 65536
  var buf = newSeqUninit[byte](BufSize)
  var rawBuf: seq[byte]
  var decompBuf: seq[byte]
  var blockPos = 0
  while decompBuf.len < 10 * 1024 * 1024:  # 10 MB safety limit
    let got = readBytes(f, buf, 0, BufSize)
    if got <= 0: break
    rawBuf.add(buf.toOpenArray(0, got.int - 1))
    while blockPos + 18 <= rawBuf.len:
      let blkSize = bgzfBlockSize(rawBuf.toOpenArray(blockPos, rawBuf.high))
      if blkSize <= 0 or blockPos + blkSize > rawBuf.len: break
      decompBuf.add(decompressBgzf(rawBuf.toOpenArray(blockPos, blockPos + blkSize - 1)))
      blockPos += blkSize
    let hEnd =
      if fmt == ffBcf: findBcfHeaderEnd(decompBuf)
      else:            findVcfHeaderEnd(decompBuf)
    if hEnd >= 0:
      return extractChromLine(decompBuf[0 ..< hEnd])
  return extractChromLine(decompBuf)

proc extractInputHeaderBytes*(path: string): seq[byte] =
  ## Read and return the decompressed header bytes (up to first data record)
  ## from a VCF.gz or BCF input file.  BGZF blocks are decompressed in chunks.
  let fmt = if path.endsWith(".bcf"): ffBcf else: ffVcf
  let f = open(path, fmRead)
  defer: f.close()
  const BufSize = 65536
  var buf      = newSeqUninit[byte](BufSize)
  var rawBuf:    seq[byte]
  var decompBuf: seq[byte]
  var blockPos = 0
  while decompBuf.len < 10 * 1024 * 1024:
    let got = readBytes(f, buf, 0, BufSize)
    if got <= 0: break
    rawBuf.add(buf.toOpenArray(0, got.int - 1))
    while blockPos + 18 <= rawBuf.len:
      let blkSize = bgzfBlockSize(rawBuf.toOpenArray(blockPos, rawBuf.high))
      if blkSize <= 0 or blockPos + blkSize > rawBuf.len: break
      decompBuf.add(decompressBgzf(rawBuf.toOpenArray(blockPos, blockPos + blkSize - 1)))
      blockPos += blkSize
    let hEnd =
      if fmt == ffBcf: findBcfHeaderEnd(decompBuf)
      else:            findVcfHeaderEnd(decompBuf)
    if hEnd >= 0: return decompBuf[0 ..< hEnd]
  result = decompBuf

# ---------------------------------------------------------------------------
# Shared shard-writing helpers
# ---------------------------------------------------------------------------

proc stripTrailingEof*(bytes: seq[byte]): seq[byte] =
  ## Return bytes with the 28-byte BGZF EOF block stripped from the end, if present.
  if bytes.len >= BGZF_EOF.len and
     bytes[bytes.len - BGZF_EOF.len ..< bytes.len] == @BGZF_EOF:
    bytes[0 ..< bytes.len - BGZF_EOF.len]
  else:
    bytes

proc writeShardZero*(outFile: File; bytes: seq[byte]; isBgzf: bool;
                     compression: Compression) =
  ## Write shard 0 to outFile: no header stripping, recompress as needed.
  ## bytes must already have the trailing BGZF EOF block removed.
  let toWrite: seq[byte] =
    if isBgzf and compression == compNone:
      decompressAllBgzfBlocks(bytes)
    elif not isBgzf and compression == compBgzf:
      compressToBgzfMulti(bytes)
    else:
      bytes
  discard outFile.writeBytes(toWrite, 0, toWrite.len)

proc writeShardData*(outFile: File; bytes: seq[byte]; fmt: FileFormat;
                     isBgzf: bool; compression: Compression) =
  ## Write one shard 1..N to outFile: strip headers and recompress as needed.
  ## bytes must already have the trailing BGZF EOF block removed.
  ## Headers: BCF binary header stripped; VCF/text #-prefixed lines stripped.
  if isBgzf:
    # Optimised path: decompress only the header-containing blocks, then
    # raw-copy (or decompress-and-write) the remaining data blocks.
    var blockPos = 0
    # Pre-allocate for header decompression. Large-sample VCF headers (e.g.
    # 2504 samples) decompress to several MB; 2 MB covers most cases and
    # avoids repeated doubling reallocs while scanning for the header end.
    var decompAccum = newSeqOfCap[byte](2 * 1024 * 1024)
    var headerEnd = -1
    while blockPos < bytes.len and headerEnd < 0:
      let blkSize = bgzfBlockSize(bytes.toOpenArray(blockPos, bytes.high))
      if blkSize <= 0: break
      decompAccum.add(
        decompressBgzf(bytes.toOpenArray(blockPos, blockPos + blkSize - 1)))
      blockPos += blkSize
      headerEnd =
        if fmt == ffBcf: findBcfHeaderEnd(decompAccum)
        else:            findVcfHeaderEnd(decompAccum)
    if headerEnd < 0: headerEnd = decompAccum.len  # edge: all-header shard
    let tail = decompAccum[headerEnd ..< decompAccum.len]
    if tail.len > 0:
      let chunk =
        if compression == compBgzf: compressToBgzfMulti(tail)
        else: tail
      discard outFile.writeBytes(chunk, 0, chunk.len)
    while blockPos < bytes.len:
      let blkSize = bgzfBlockSize(bytes.toOpenArray(blockPos, bytes.high))
      if blkSize <= 0: break
      if compression == compBgzf:
        discard outFile.writeBytes(bytes, blockPos, blkSize)
      else:
        let d = decompressBgzf(bytes.toOpenArray(blockPos, blockPos + blkSize - 1))
        discard outFile.writeBytes(d, 0, d.len)
      blockPos += blkSize
  else:
    # Uncompressed path: strip headers, recompress if needed.
    let data = bytes
    let headerEnd =
      case fmt
      of ffBcf:         findBcfHeaderEnd(data)
      of ffVcf, ffText: findVcfHeaderEnd(data)
    let stripped =
      if headerEnd > 0 and headerEnd < data.len: data[headerEnd ..< data.len]
      elif headerEnd == 0: data
      else: @[]
    let toWrite =
      if compression == compBgzf: compressToBgzfMulti(stripped)
      else: stripped
    discard outFile.writeBytes(toWrite, 0, toWrite.len)

# ---------------------------------------------------------------------------
# Direct-file gather
# ---------------------------------------------------------------------------

proc gatherFiles*(cfg: GatherConfig; inputPaths: seq[string]) =
  ## Concatenate pre-existing shard files into cfg.outputPath (or stdout).
  ## Shard 0 is written with its header intact; shards 1..N have headers stripped.
  ## For VCF/BCF: validates that #CHROM lines match before writing anything.
  ## No temp files are created.
  if inputPaths.len == 0:
    stderr.writeLine "error: gather: no input files provided"
    quit(1)

  # ── Phase 1: read shard 0, detect format, validate #CHROM ──────────────────
  let s0Size = getFileSize(inputPaths[0]).int
  var s0Bytes = newSeqUninit[byte](s0Size)
  block:
    let fs0 = open(inputPaths[0], fmRead)
    discard readBytes(fs0, s0Bytes, 0, s0Size)
    fs0.close()
  let (fmt, isBgzf) = sniffStreamFormat(s0Bytes)
  if fmt != cfg.format and not cfg.toStdout:
    stderr.writeLine &"warning: shard 0 format detected as {fmt} " &
      &"but output expects {cfg.format}; proceeding"

  if fmt in {ffVcf, ffBcf}:
    let chrom0 = chromLineFromBytes(s0Bytes, fmt, isBgzf)
    for j in 1 ..< inputPaths.len:
      let chromJ = chromLineFromFile(inputPaths[j], fmt, isBgzf)
      if chromJ != chrom0:
        stderr.writeLine &"error: gather: #CHROM line mismatch between shard 1 and shard {j+1} ({inputPaths[j]}):"
        stderr.writeLine &"  shard 1: {chrom0}"
        stderr.writeLine &"  shard {j+1}: {chromJ}"
        quit(1)

  # ── Phase 2: open output, write shard 0 then shards 1..N ───────────────────
  let outFile: File = if cfg.toStdout: stdout else: open(cfg.outputPath, fmWrite)

  # Write shard 0 (bytes already buffered in s0Bytes).
  let bytes0 = if isBgzf: stripTrailingEof(s0Bytes) else: s0Bytes
  writeShardZero(outFile, bytes0, isBgzf, cfg.compression)

  # Write shards 1..N: read fresh from disk, strip headers.
  for j in 1 ..< inputPaths.len:
    let jSize = getFileSize(inputPaths[j]).int
    var allBytes = newSeqUninit[byte](jSize)
    block:
      let fj = open(inputPaths[j], fmRead)
      discard readBytes(fj, allBytes, 0, jSize)
      fj.close()
    let bytes = if isBgzf: stripTrailingEof(allBytes) else: allBytes
    writeShardData(outFile, bytes, fmt, isBgzf, cfg.compression)

  if cfg.compression == compBgzf:
    discard outFile.writeBytes(BGZF_EOF, 0, BGZF_EOF.len)
  if not cfg.toStdout: outFile.close()
