local count = redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1]);
--if elements with lower priority are on head
local v = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES');
if v[1] ~= nil and tonumber(v[2]) < tonumber(ARGV[3]) then
    redis.call('PUBLISH', KEYS[2], v[2]);
end
return count;