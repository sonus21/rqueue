local srcValues = redis.call('ZRANGE', KEYS[1], 0, ARGV[1])
if #srcValues > 0 then
    local delta = tonumber(ARGV[2])
    local op = tonumber(ARGV[3])
    for existing_score, v in ipairs(srcValues) do
        local score = delta
        if op == 0 then
            score = score + existing_score
        end
        redis.call('ZADD', KEYS[2], score, v)
    end
    redis.call('ZREM', KEYS[1], unpack(srcValues))
end;
local size = redis.call('ZCARD', KEYS[1])
return size