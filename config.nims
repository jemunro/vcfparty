# config.nims — automatically loaded by nim for all compilations in this project.
# Adds nimble package paths so `nim c -r tests/...` works without `nimble test`.
when defined(nimscript):
  import std/[os, strutils]
  let htspath = gorge("nimble path hts 2>/dev/null")
  if htspath.len > 0 and htspath[0] == '/':
    switch("path", htspath & "/src")
  # Tests: panics off so AssertionDefect is raised (catchable) instead of
  # triggering rawQuit, letting the `timed` template print a FAIL line.
  if projectName().startsWith("test_"):
    switch("panics", "off")
# begin Nimble config (version 2)
when withDir(thisDir(), system.fileExists("nimble.paths")):
  include "nimble.paths"
# end Nimble config
