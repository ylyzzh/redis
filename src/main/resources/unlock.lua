---
--- Generated by EmmyLua(https://github.com/EmmyLua)
--- Created by 25357.
--- DateTime: 2022/11/18 17:19
---
-- 比较线程标识与锁中的标识是否一致
if(redis.call('get',KEYS[1])==ARGV[1]) then
    --释放锁 del key
    return redis.call('del',KEYS[1])
end
return 0