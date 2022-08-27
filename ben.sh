#!/bin/bash
# set,get,incr
# lpush,rpush,lpop,rpop,lrange
# hget,hset,hgetall
# sadd,spop
# mset,mget,
redis-benchmark -q -t set,get -n 1000000 -P 5  $@
