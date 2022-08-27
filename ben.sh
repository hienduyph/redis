#!/bin/bash
# set,get,incr
# lpush,rpush,lpop,rpop,lrange
# hget,hset,hgetall
# sadd,spop
redis-benchmark -q -t mset,mget, -n 1000000 -P 5  $@
