-- get first N elements of the queue
local values = redis.call('LRANGE', KEYS[1], 0, tonumber(ARGV[3]) - 1)

-- push to processing set
if #values > 0 then
    for _, value in ipairs(values) do
        redis.call('ZADD', KEYS[2], ARGV[2], value)
    end
end;

-- if elements with lower priority are on the head of processing queue
local v = redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')
if v[1] ~= nil and tonumber(v[2]) < tonumber(ARGV[1]) then
    redis.call('PUBLISH', KEYS[3], v[2])
end

if #values > 0 then
    -- remove from the queue
    redis.call('LTRIM', KEYS[1], ARGV[3], -1)
end

return values;