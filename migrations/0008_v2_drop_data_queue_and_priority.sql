-- V2 Store & Forward redesign (spec.md "V2 scope"):
--
-- 1. The queue moved out of gateway.db into its own file (queue.db,
--    internal/queue), so data_queue and its indexes (0001, 0003, 0006,
--    0007) are dropped. Any rows still in it are NOT carried over: drain
--    the V1 queue (pending 0) before upgrading a device, or accept losing
--    whatever was unsent. Dropping frees pages inside gateway.db but does
--    not shrink the file; run VACUUM once after the upgrade (not possible
--    here — VACUUM cannot run inside the migration transaction).
--
-- 2. Data priority was removed. datapoint.priority's CHECK constraint is
--    attached to the column itself, so plain DROP COLUMN is allowed (no
--    table rebuild, unlike 0004).
--
-- gateway.last_sequence (V1's per-reading sequence counter) is left in
-- place, unused: dropping it gains nothing and the gateway row stays.

DROP TABLE IF EXISTS data_queue;
ALTER TABLE datapoint DROP COLUMN priority;
