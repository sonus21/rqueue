local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if score then
    redis.call('ZADD', KEYS[1], tonumber(ARGV[2]) + tonumber(score), ARGV[1])
    return true
end
return false