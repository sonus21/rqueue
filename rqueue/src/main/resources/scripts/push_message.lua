local expiredValues = redis.call('ZRANGEBYSCORE', KEYS[2], 0, ARGV[1], 'LIMIT', 0, ARGV[2])
if #expiredValues > 0 then
    for _, v in ipairs(expiredValues) do
        redis.call('RPUSH', KEYS[1], v)
    end;
    redis.call('ZREM', KEYS[2], unpack(expiredValues))
end
-- check head of the queue
local v = redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')
if v[1] ~= nil then
    local score = tonumber(v[2])
    return score
end
return nil;