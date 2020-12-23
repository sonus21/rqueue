-- get current value
local value = redis.call('GET', KEYS[1])
if not value then
    return true
end
if value == ARGV[1] then
    local val = redis.call('DEL', KEYS[1])
    if val == 1 then
        return true
    end
end
return false

