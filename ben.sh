#!/bin/bash

redis-benchmark -q -t set,get -n 1000000 -P 5  $@
# sadd,hset,spop,mset,,incr,lpush,rpush,lpop,rpop,lrange
