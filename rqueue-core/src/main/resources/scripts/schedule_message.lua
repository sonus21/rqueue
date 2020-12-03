-- get current value
local value = redis.call('GET', KEYS[1])

if value then
    return 0
end

redis.call('SET', KEYS[1], "1", "EX", ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
return 1

