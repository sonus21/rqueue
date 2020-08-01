for i = tonumber(ARGV[1]), 1, -1
do
    local msg = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')[1]
    if msg ~= nil then
        redis.call('RPUSH', KEYS[2], msg)
        redis.call('ZREM', KEYS[1], msg)
    else
        break
    end
end
local size = redis.call('ZCARD', KEYS[1])
return size