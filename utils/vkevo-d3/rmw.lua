-- threadnarock North Star: 2-key read-modify-write (session / rate-limit shape), loaded with SCRIPT LOAD at prefill.
-- KEYS[1] = {e:<key>}:a, the state string. The counter {e:<key>}:b is derived from KEYS[1]: same hash tag, so the
-- same slot (memtier --cluster-mode sends one generated key per --command; Valkey lets a script touch an undeclared
-- key of the declared key's slot). ARGV[1] = TTL in seconds.
-- Calls per run, always the same: GET, INCRBY, EXPIRE, SET (ns_mix.py subtracts them from commandstats).
local b = string.sub(KEYS[1], 1, -2) .. 'b'
local state = redis.call('GET', KEYS[1])
local n = redis.call('INCRBY', b, 1)
redis.call('EXPIRE', b, ARGV[1])
local prev = 0
if state then prev = #state end
redis.call('SET', KEYS[1], n .. ':' .. prev, 'EX', ARGV[1])
return n
