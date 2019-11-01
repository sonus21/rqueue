-- get head of queue
local value = redis.call('LRANGE', KEYS[1], 0, 0);

-- push to processing set
if value[1] ~= nil then
    redis.call('ZADD', KEYS[2], ARGV[2], value[1]);
end
--if elements with lower priority are on the head of processing queue
local v = redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES');
if v[1] ~= nil and tonumber(v[2]) < tonumber(ARGV[1]) then
    redis.call('PUBLISH', KEYS[3], v[2]);
end
-- remove from the queue
value = redis.call('LPOP', KEYS[1])
return value;