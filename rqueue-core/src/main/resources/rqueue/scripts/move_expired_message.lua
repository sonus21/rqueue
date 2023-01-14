--  Copyright (c) 2020-2023 Sonu Kumar
--
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--         https://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and limitations under the License.
--

local expiredValues = redis.call('ZRANGEBYSCORE', KEYS[2], 0, ARGV[1], 'LIMIT', 0, ARGV[2])
local updateFailureCount = tonumber(ARGV[3])
local function getValue(v)
    if updateFailureCount == 0 then
        return v
    end
    if string.sub(v, 59, 70) == 'failureCount' then
        -- ":
        local index = 73
        local c = string.sub(v, index, index)
        local retryCount = 0;
        while (c == '0' or c == '1' or c == '2' or c == '3' or c == '4' or
                c == '5' or c == '6' or c == '7' or c == '8' or c == '9')
        do
            retryCount = retryCount * 10 + c
            index = index + 1
            c = string.sub(v, index, index)
        end
        if index == 73 then
            return v
        end
        retryCount = retryCount + 1
        return string.sub(v, 1, 72) .. tostring(retryCount) .. string.sub(v, index)
    end
    return v
end

if #expiredValues > 0 then
    for _, v in ipairs(expiredValues) do
        redis.call('RPUSH', KEYS[1], getValue(v))
    end ;
    redis.call('ZREM', KEYS[2], unpack(expiredValues))
end
-- check head of the queue
local v = redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')
if v[1] ~= nil then
    local score = tonumber(v[2])
    return score
end
return nil;