## Tests for scatter.nim — index parsing (Step 3), boundary optimisation (Step 4),
## and shard writing (Step 5).
## Run from project root: nim c -r tests/test_scatter.nim

import std/[algorithm, os, strformat, strutils]
import "../src/paravar/bgzf_utils"
import "../src/paravar/scatter"

const DataDir  = "tests/data"
const SmallVcf = DataDir / "small.vcf.gz"     # TBI indexed
const CsiVcf   = DataDir / "small_csi.vcf.gz" # CSI indexed only (no .tbi)

proc readMagic(path: string; offset: int64): array[3, byte] =
  let f = open(path, fmRead)
  defer: f.close()
  f.setFilePos(offset)
  discard readBytes(f, result, 0, 3)

# ===========================================================================
# Step 3 — Index parsing: TBI
# ===========================================================================

block testParseTbi:
  let starts = parseTbiBlockStarts(SmallVcf & ".tbi")
  doAssert starts.len > 0, "parseTbiBlockStarts: no blocks"
  for i in 1 ..< starts.len:
    doAssert starts[i] > starts[i-1], "parseTbiBlockStarts: not strictly increasing"
  for off in starts:
    let magic = readMagic(SmallVcf, off)
    doAssert magic[0] == 0x1f and magic[1] == 0x8b,
      &"bad BGZF magic at offset {off}"
  echo &"PASS parseTbiBlockStarts ({starts.len} blocks)"

block testReadIndexBlockStartsTbi:
  let starts = readIndexBlockStarts(SmallVcf)
  doAssert starts.len > 0, "readIndexBlockStarts (TBI): no blocks"
  for i in 1 ..< starts.len:
    doAssert starts[i] > starts[i-1], "readIndexBlockStarts (TBI): not sorted"
  echo &"PASS readIndexBlockStarts via TBI ({starts.len} blocks)"

# ===========================================================================
# Step 3 — Index parsing: CSI
# ===========================================================================

block testParseCsi:
  doAssert fileExists(CsiVcf & ".csi"), "CSI fixture missing — run generate_fixtures.sh"
  doAssert not fileExists(CsiVcf & ".tbi"), "CSI fixture must not have a .tbi alongside it"
  let starts = parseCsiBlockStarts(CsiVcf & ".csi")
  doAssert starts.len > 0, "parseCsiBlockStarts: no blocks"
  for i in 1 ..< starts.len:
    doAssert starts[i] > starts[i-1], "parseCsiBlockStarts: not strictly increasing"
  for off in starts:
    let magic = readMagic(CsiVcf, off)
    doAssert magic[0] == 0x1f and magic[1] == 0x8b,
      &"bad BGZF magic at offset {off}"
  echo &"PASS parseCsiBlockStarts ({starts.len} blocks)"

block testReadIndexBlockStartsCsi:
  let starts = readIndexBlockStarts(CsiVcf)   # must fall through to .csi
  doAssert starts.len > 0, "readIndexBlockStarts (CSI): no blocks"
  for i in 1 ..< starts.len:
    doAssert starts[i] > starts[i-1], "readIndexBlockStarts (CSI): not sorted"
  echo &"PASS readIndexBlockStarts via CSI ({starts.len} blocks)"

# ===========================================================================
# Step 4 — Header extraction
# ===========================================================================

block testGetHeaderAndFirstBlock:
  let (hdrBytes, firstBlock) = getHeaderAndFirstBlock(SmallVcf)
  # Compressed header must be a valid BGZF block
  doAssert bgzfBlockSize(hdrBytes) > 0,
    "getHeaderAndFirstBlock: header not a valid BGZF block"
  # Decompress and verify it contains a VCF header line
  let hdrContent = decompressBgzf(hdrBytes)
  doAssert hdrContent.len > 0, "getHeaderAndFirstBlock: empty header"
  doAssert hdrContent[0] == byte('#'),
    "getHeaderAndFirstBlock: header does not start with '#'"
  # firstBlock must point to a valid BGZF block in the VCF
  let magic = readMagic(SmallVcf, firstBlock)
  doAssert magic[0] == 0x1f and magic[1] == 0x8b,
    &"getHeaderAndFirstBlock: firstBlock {firstBlock} has bad BGZF magic"
  echo &"PASS getHeaderAndFirstBlock (firstBlock={firstBlock})"

# ===========================================================================
# Step 4 — getLengths / partitionBoundaries
# ===========================================================================

block testGetLengths:
  let starts: seq[int64] = @[0'i64, 100, 300, 700]
  let lengths = getLengths(starts, 1000)
  doAssert lengths == @[100'i64, 200, 400, 300],
    &"getLengths: expected [100,200,400,300] got {lengths}"
  echo "PASS getLengths"

block testPartitionBoundaries2:
  # 4 equal blocks → split into 2 shards → boundary at index 1 (bisect_left on cumsum)
  let lengths: seq[int64] = @[100'i64, 100, 100, 100]
  let bounds = partitionBoundaries(lengths, 2)
  doAssert bounds.len == 1, &"partitionBoundaries 2: expected 1 bound, got {bounds.len}"
  doAssert bounds[0] == 1, &"partitionBoundaries 2: expected index 1, got {bounds[0]}"
  echo "PASS partitionBoundaries (2 shards)"

block testPartitionBoundaries4:
  # 8 equal blocks → 4 shards → boundaries at 1, 3, 5 (bisect_left on cumsum)
  let lengths: seq[int64] = @[100'i64, 100, 100, 100, 100, 100, 100, 100]
  let bounds = partitionBoundaries(lengths, 4)
  doAssert bounds.len == 3, &"partitionBoundaries 4: expected 3 bounds, got {bounds.len}"
  doAssert bounds == @[1, 3, 5], &"partitionBoundaries 4: expected [1,3,5] got {bounds}"
  echo "PASS partitionBoundaries (4 shards)"

# ===========================================================================
# Step 4 — isValidBoundary
# ===========================================================================

block testIsValidBoundary:
  # Every non-EOF data block in small.vcf.gz should be valid (contains >= 2 lines).
  let allStarts = scanBgzfBlockStarts(SmallVcf)
  var validCount = 0
  var buf = newSeq[byte](18)
  let f = open(SmallVcf, fmRead)
  for off in allStarts:
    f.setFilePos(off)
    discard readBytes(f, buf, 0, 18)
    let sz = bgzfBlockSize(buf)
    if sz == 28: continue   # skip EOF block
    let fileSize = getFileSize(SmallVcf)
    let blockLen = if off + sz.int64 < fileSize: sz.int64
                   else: fileSize - off
    if isValidBoundary(SmallVcf, off, blockLen):
      validCount += 1
  f.close()
  doAssert validCount > 0, "isValidBoundary: no valid blocks found"
  echo &"PASS isValidBoundary ({validCount} valid blocks)"

# ===========================================================================
# Step 4 — optimiseBoundaries end-to-end
# ===========================================================================

block testOptimiseBoundaries4:
  var starts = readIndexBlockStarts(SmallVcf)
  let (_, firstBlock) = getHeaderAndFirstBlock(SmallVcf)
  # Mirror Python: add first_block and scan fine-grained sub-blocks.
  if firstBlock notin starts: starts.add(firstBlock)
  starts.sort()
  if starts.len >= 2:
    for off in scanBgzfBlockStarts(SmallVcf, starts[0], starts[1]):
      if off notin starts: starts.add(off)
    starts.sort()
  let (bounds, finalStarts, lengths) = optimiseBoundaries(SmallVcf, starts, 4)
  doAssert bounds.len == 3, &"optimiseBoundaries: expected 3 bounds, got {bounds.len}"
  # Each boundary block must be valid
  for bi in bounds:
    doAssert isValidBoundary(SmallVcf, finalStarts[bi], lengths[bi]),
      &"optimiseBoundaries: boundary at {finalStarts[bi]} is invalid"
  # Lengths must be non-zero
  for l in lengths:
    doAssert l > 0, "optimiseBoundaries: zero-length block"
  echo &"PASS optimiseBoundaries 4-shard ({finalStarts.len} fine blocks)"

# ===========================================================================
# Step 5 — scatter end-to-end helpers
# ===========================================================================

proc collectRecords(data: seq[byte]): seq[string] =
  ## Return all non-header lines from decompressed VCF bytes as strings.
  result = @[]
  var lineStart = 0
  for i in 0 ..< data.len:
    if data[i] == byte('\n'):
      if i > lineStart and data[lineStart] != byte('#'):
        var s = newString(i - lineStart)
        for j in lineStart ..< i: s[j - lineStart] = char(data[j])
        result.add(s)
      lineStart = i + 1
  if lineStart < data.len and data[lineStart] != byte('#'):
    var s = newString(data.len - lineStart)
    for j in lineStart ..< data.len: s[j - lineStart] = char(data[j])
    result.add(s)

proc checkShards(vcfPath: string; prefix: string; n: int) =
  ## Verify BGZF structure, header presence, record completeness, and order.
  let nDigits = len($n)

  # 1 — each output file exists and has valid BGZF magic + EOF block
  for i in 1..n:
    let path = prefix & "." & align($i, nDigits, '0') & ".vcf.gz"
    doAssert fileExists(path), &"shard {i} missing: {path}"
    let fz = open(path, fmRead)
    var hdrBuf = newSeq[byte](3)
    discard readBytes(fz, hdrBuf, 0, 3)
    doAssert hdrBuf[0] == 0x1f'u8 and hdrBuf[1] == 0x8b'u8,
      &"shard {i}: bad BGZF magic"
    let sz = getFileSize(path)
    fz.setFilePos(sz - 28)
    var eofBuf = newSeq[byte](28)
    discard readBytes(fz, eofBuf, 0, 28)
    fz.close()
    doAssert eofBuf == @BGZF_EOF, &"shard {i}: EOF block mismatch"

  # 2 — each shard starts with a VCF header
  for i in 1..n:
    let path = prefix & "." & align($i, nDigits, '0') & ".vcf.gz"
    let content = decompressBgzfFile(path)
    doAssert content.len > 0, &"shard {i}: empty after decompression"
    doAssert content[0] == byte('#'), &"shard {i}: does not start with '#'"

  # 3 — all records present, no duplicates
  var shardRecords: seq[string]
  for i in 1..n:
    let path = prefix & "." & align($i, nDigits, '0') & ".vcf.gz"
    shardRecords.add(collectRecords(decompressBgzfFile(path)))
  let origRecords = collectRecords(decompressBgzfFile(vcfPath))
  doAssert shardRecords.len == origRecords.len,
    &"record count mismatch: shards={shardRecords.len} orig={origRecords.len}"
  doAssert sorted(shardRecords) == sorted(origRecords),
    "shard records do not match original"

  # 4 — records within each shard are in non-decreasing genomic order (CHROM:POS)
  for i in 1..n:
    let path = prefix & "." & align($i, nDigits, '0') & ".vcf.gz"
    let recs = collectRecords(decompressBgzfFile(path))
    for j in 1 ..< recs.len:
      let prevFields = recs[j-1].split('\t')
      let curFields  = recs[j].split('\t')
      if prevFields.len >= 2 and curFields.len >= 2:
        if prevFields[0] == curFields[0]:
          doAssert prevFields[1].parseInt <= curFields[1].parseInt,
            &"shard {i}: records out of order at line {j}"

# ===========================================================================
# Step 5 — scatter: TBI-indexed input
# ===========================================================================

block testScanAllBlockStarts:
  let (_, firstBlock) = getHeaderAndFirstBlock(SmallVcf)
  let starts = scanAllBlockStarts(SmallVcf, firstBlock)
  doAssert starts.len > 0, "scanAllBlockStarts: no data blocks found"
  for off in starts:
    let magic = readMagic(SmallVcf, off)
    doAssert magic[0] == 0x1f and magic[1] == 0x8b,
      &"scanAllBlockStarts: bad BGZF magic at offset {off}"
  for i in 1 ..< starts.len:
    doAssert starts[i] > starts[i-1], "scanAllBlockStarts: not strictly increasing"
  echo &"PASS scanAllBlockStarts ({starts.len} data blocks, firstBlock={firstBlock})"

block testScatter4ShardsTbi:
  let tmpDir = getTempDir() / "paravar_scatter_tbi_test"
  createDir(tmpDir)
  let prefix = tmpDir / "shard"
  scatter(SmallVcf, 4, prefix)
  checkShards(SmallVcf, prefix, 4)

  var sizes: seq[int64]
  for i in 1..4:
    sizes.add(getFileSize(prefix & "." & $i & ".vcf.gz"))
  let minSz = sizes.min(); let maxSz = sizes.max()
  doAssert minSz > 0, "scatter (TBI): at least one shard is empty"
  doAssert maxSz.float / minSz.float < 2.0,
    &"scatter (TBI): shard size imbalance: max={maxSz} min={minSz}"

  echo "PASS scatter TBI: BGZF structure, header, completeness, order, balance"
  removeDir(tmpDir)

# ===========================================================================
# Step 5 — scatter: CSI-indexed input
# ===========================================================================

block testScatterNoIndexAutoScan:
  ## With no index alongside the file, scatter should warn and auto-scan.
  let tmpDir = getTempDir() / "paravar_scatter_scan_test"
  createDir(tmpDir)
  let tmpVcf = tmpDir / "noindex.vcf.gz"
  copyFile(SmallVcf, tmpVcf)
  # No .tbi or .csi copied — must fall through to auto-scan.
  let prefix = tmpDir / "shard"
  scatter(tmpVcf, 4, prefix)
  checkShards(SmallVcf, prefix, 4)
  echo "PASS scatter no-index auto-scan: BGZF structure, header, completeness, order"
  removeDir(tmpDir)

block testScatterForceScan:
  ## scatter with forceScan=true on a fully indexed file — index is ignored.
  let tmpDir = getTempDir() / "paravar_scatter_forcescan_test"
  createDir(tmpDir)
  let prefix = tmpDir / "shard"
  scatter(SmallVcf, 4, prefix, 1, forceScan = true)
  checkShards(SmallVcf, prefix, 4)
  echo "PASS scatter --force-scan: BGZF structure, header, completeness, order"
  removeDir(tmpDir)

block testScatter4ShardsCsi:
  doAssert fileExists(CsiVcf & ".csi"), "CSI fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "paravar_scatter_csi_test"
  createDir(tmpDir)
  let prefix = tmpDir / "shard"
  scatter(CsiVcf, 4, prefix)
  checkShards(CsiVcf, prefix, 4)
  echo "PASS scatter CSI: BGZF structure, header, completeness, order"
  removeDir(tmpDir)

echo ""
echo "All scatter tests passed."
