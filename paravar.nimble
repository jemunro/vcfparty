# Package
version     = "0.1.0"
author      = "munro.j"
description = "Parallel processing of VCF files"
license     = "MIT"
srcDir      = "src"
bin         = @["paravar"]

# Dependencies
requires "nim >= 2.0.0"

# Tasks
task test, "Run all tests":
  exec "nimble build"   # build once; test files skip their own nimble build
  # Clear test nimcaches so source changes in imported modules are picked up.
  exec "rm -rf nimcache/tests"
  exec "testament pattern 'tests/test_*.nim'"
