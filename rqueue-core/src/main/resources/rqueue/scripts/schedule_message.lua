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

-- get current value
local value = redis.call('GET', KEYS[1])
if value then
    return 0
end
redis.call('SET', KEYS[1], '1', 'EX', ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
return 1
