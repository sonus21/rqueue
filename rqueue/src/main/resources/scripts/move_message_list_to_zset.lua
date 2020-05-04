for i = tonumber(ARGV[1]), 1, -1
do
    local msg = redis.call('LRANGE', KEYS[1], 0, 0)[1]
    if msg ~= nil then
        redis.call("ZADD", KEYS[2], ARGV[2], msg)
        redis.call("LPOP", KEYS[1])
    else
        break
    end
end
local remainingMessage = redis.call("LLEN", KEYS[1])
return remainingMessage

