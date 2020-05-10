local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if score ~= nil then
    redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
    redis.call('ZREM', KEYS[1], ARGV[1])
end
return score