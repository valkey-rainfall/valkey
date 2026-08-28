# Door-2 bug reproducers

Statistical/system-level hammers for concurrency bugs found on the door-2
(thread-owned clients) experimental lineage. Each bug also has an in-suite
deterministic regression in tests/unit/ownership.tcl; these scripts are the
heavier revalidation tools for use after read-path or lifecycle changes.

- b11-resize-verify.sh -- runtime io-threads resize under owned traffic
  (SIGSEGV + lost-reply defects; fixed in 9dce806e).
- b12-crlf-soak.sh -- parser desync / lost-chunk soak, ~1MB multibulk under
  contention (two race windows; fixed in 3028b455 + 90a7ec21). Pre-fix rate
  ~0.5%/iteration; run >= 4000 iterations for a meaningful bound. Build the
  server with SERVER_CFLAGS=-DIO_LOOKUP_OFFLOAD_STATS to get the
  "D+ proto-desync" forensic line in the server log on any hit.

TSan note: run the B12 shape under the TSan differential with
  ./runtest --single unit/ownership --only '/EPOCH|B12' ...
(see the TSan recipe in the design-closure ledger / benchdev lessons).
