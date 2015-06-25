# QDB Command Support

 - Connection
   - AUTH
   ```
   AUTH password
   ```
   - ECHO
   ```
   ECHO text
   ```
   - PING
   - SELECT
   ```
   SELECT db
   ```
   - QUIT(Not)

 - Keys 
   - DEL
   ```
   DEL key [key ...]
   ```
   - EXISTS
   ```
   EXISTS key
   ```
   - EXPIRE
   ```
   EXPIRE key seconds
   ```
   - EXPIREAT
   ```
   EXPIREAT key timestamp
   ```
   - PERSIST
   ```
   PERSIST key
   ```
   - PEXPIRE
   ```
   PEXPIRE key milliseconds
   ```
   - PEXPIREAT
   ```
   PEXPIREAT key timestamp
   ```
   - PTTL
   ```
   PTTL key
   ```
   - RESTORE
   ```
   RESTORE key ttlms value
   ```
   - TTL
   ```
   TTL key
   ```
   - TYPE
   ```
   TYPE key
   ```
 - Strings
   - APPEND
   ```
   APPEND key value
   ```
   - DECR
   ```
   DECR key
   ```
   - DECRBY
   ```
   DECRBY key delta
   ```
   - GET
   ```
   GET key
   ```
   - GETSET
   ```
   GETSET key value
   ```
   - INCR
   ```
   INCR key
   ```
   - INCRBY
   ```
   INCRBY key delta
   ```
   - INCRBYFLOAT
   ```
   INCRBYFLOAT key delta
   ```
   - MGET
   ```
   MGET key [key ...]
   ```
   - MSET
   ```
   MSET key value [key value ...]
   ```
   - MSETNX
   ```
   MSETNX key value [key value ...]
   ```
   - PSETEX
   ```
   PSETEX key milliseconds value
   ```
   - SET
   ```
   SET key value [EX seconds] [PX milliseconds] [NX|XX]
   ```
   - SETBIT
   ```
   SETBIT key offset value
   ```
   - SETEX
   ```
   SETEX key seconds value
   ```
   - SETNX
   ```
   SETNX key value
   ```
   - SETRANGE
   ```
   SETRANGE key offset value
   ```
   - STRLEN
   ```
   STRLEN key
   ```

 - Hashes
   - HDEL
   ```
   HDEL key field [field ...]
   ```
   - HEXISTS
   ```
   HEXISTS key field
   ```
   - HGET
   ```
   HGET key field
   ```
   - HGETALL
   ```
   HGETALL key
   ```
   - HINCRBY
   ```
   HINCRBY key field delta
   ```
   - HINCRBYFLOAT
   ```
   HINCRBYFLOAT key field delta
   ```
   - HKEYS
   ```
   HKEYS key
   ```
   - HLEN
   ```
   HLEN key
   ```
   - HMGET
   ```
   HMGET key field [field ...]
   ```
   - HMSET
   ```
   HMSET key field value [field value ...]
   ```
   - HSET
   ```
   HSET key field value
   ```
   - HSETNX
   ```
   HSETNX key field value
   ```
   - HVALS
   ```
   HVALS key
   ```

 - Lists
   - LINDEX
   ```
   LINDEX key index
   ```
   - LLEN
   ```
   LLEN key
   ```
   - LPOP
   ```
   LPOP key
   ```
   - LPUSH
   ```
   LPUSH key value [value ...]
   ```
   - LPUSHX
   ```
   LPUSHX key value [value ...]
   ```
   - LRANGE
   ```
   LRANGE key beg end
   ```
   - LREM(Not)
   - LSET
   ```
   LSET key index value
   ```
   - LTRIM
   ```
   LTRIM key beg end
   ```
   - RPOP
   ```
   RPOP key
   ```
   - RPUSH
   ```
   RPUSH key value [value ...]
   ```
   - RPUSHX
   ```
   RPUSHX key value [value ...]
   ```

 - Sets
   - SADD
   ```
   SADD key member [member ...]
   ```
   - SCARD
   ```
   SCARD key
   ```
   - SISMEMBER
   ```
   SISMEMBER key member
   ```
   - SMEMBERS
   ```
   SMEMBERS key
   ```
   - SPOP
   ```
   SPOP key
   ```
   - SRANDMEMBER
   ```
   SRANDMEMBER key [count]
   ```
   - SREM
   ```
   SREM key member [member ...]
   ```

 - Sorted Sets
   - ZADD
   ```
   ZADD key score member [score member ...]
   ```
   - ZCARD
   ```
   ZCARD key
   ```
   - ZCOUNT
   ```
   ZCOUNT key min max
   ```
   - ZINCRBY
   ```
   ZINCRBY key delta member
   ```
   - ZLEXCOUNT
   ```
   ZLEXCOUNT key min max
   ```
   - ZRANGE
   ```
   ZRANGE key start stop [WITHSCORES]
   ```
   - ZRANGEBYLEX
   ```
   ZRANGEBYLEX key start stop [LIMIT offset count]
   ```
   - ZRANGEBYSCORE
   ```
   ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
   ```
   - ZRANK
   ```
   ZRANK key member
   ```
   - ZREM
   ```
   ZREM key member [member ...]
   ```
   - ZREMRANGEBYLEX
   ```
   ZREMRANGEBYLEX key min max
   ```
   - ZREMRANGEBYRANK
   ```
   ZREMRANGEBYRANK key start stop
   ```
   - ZREMRANGEBYSCORE
   ```
   ZREMRANGEBYSCORE key min max
   ```
   - ZREVRANGE
   ```
   ZREVRANGE key start stop [WITHSCORES]
   ```
   - ZREVRANGEBYLEX
   ```
   ZREVRANGEBYLEX key start stop [LIMIT offset count]
   ```
   - ZREVRANGEBYSCORE
   ```
   ZREVRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
   ```
   - ZREVRANK
   ```
   ZREVRANK key member
   ```
   - ZSCORE
   ```
   ZSCORE key member
   ```

 - Server
   - BGSAVE
   - BGSAVETO
   ```
   BGSAVETO path
   ```
   - CONFIG GET
   ```
   CONFIG GET key
   ```
   - CONFIG SET
   ```
   CONFIG SET key value
   ```
   - FLUSHALL
   - INFO
   ```
   INFO [section]
   ```
   - PSYNC
   ```
   PSYNC run-id sync-offset
   ```
   - ROLE
   - SHUTDOWN
   - SLAVEOF
   ```
   SLAVEOF host port
   ```
   - SYNC

 - Slots
   - SLOTSHASHKEY
   ```
   SLOTSHASHKEY key [key...]
   ```
   - SLOTSINFO
   ```
   SLOTSINFO [start [count]]
   ```
   - SLOTSMGRTONE
   ```
   SLOTSMGRTONE host port timeout key
   ```
   - SLOTSMGRTSLOT
   ```
   SLOTSMGRTSLOT host port timeout slot
   ```
   - SLOTSMGRTTAGONE
   ```
   SLOTSMGRTTAGONE host port timeout key
   ```
   - SLOTSMGRTTAGSLOT
   ```
   SLOTSMGRTTAGSLOT host port timeout slot
   ```
   - SLOTSRESTORE
   ```
   SLOTSRESTORE key ttlms value [key ttlms value ...]
   ```

 - Other
    - COMPACTALL
    - REPLCONF
    ```
    REPLCONF listening-port port / ack sync-offset
    ```
