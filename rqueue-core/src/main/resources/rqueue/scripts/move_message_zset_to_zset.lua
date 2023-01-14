--  Copyright (c) 2021-2023 Sonu Kumar
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
end ;
local size = redis.call('ZCARD', KEYS[1])
return size