--  Copyright 2021 Sonu Kumar
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

local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if score ~= nil then
    redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
    redis.call('ZREM', KEYS[1], ARGV[1])
end
score = tonumber(score)
return score