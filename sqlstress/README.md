SQL Source Correctness Stress Test
==================================

Source connectors for SQL databases (currently just PostgreSQL but others
are planned in the near future) have to solve a tricky problem: they need
to backfill the current contents of their tables, and then seamlessly
transition to consuming live change events as they occur. This transition
needs to be tested thoroughly to rule out the presence of subtle bugs.

This directory contains a small "stress test" program which writes generated
traffic into a database, launches the connector while continuing to write
more traffic, then brings the test to completion and validates the captured
sequence of events.

The generated traffic is a randomized sequence of updates to a simple
`(ID, SequenceNumber)` dataset which obeys the following properties:

  * Every ID from 0 to some N will exist.
  * Every sequence number will be 9 after the traffic generator finishes
    its orderly shutdown.
  * Each ID will experience the following changes, in order:
    * `Insert(id, 0)`
    * `Update(id, 1)`
    * `Update(id, 2)`
    * `Delete(id)`
    * `Insert(id, 4)`
    * `Update(id, 5)`
    * `Update(id, 6)`
    * `Delete(id)`
    * `Insert(id, 8)`
    * `Update(id, 9)`

These properties allow us to detect many (perhaps all?) realistic classes of
correctness violation solely by processing the capture output.

Since errors are most likely to occur during the scanning->replication switch
we need to exercise that repeatedly. The stress test repeats the entire traffic
generation, capture, and validation process until either an error is detected
or 100 iterations have completed without any errors.