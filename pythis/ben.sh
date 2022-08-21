#!/bin/bash

redis-benchmark -q -t set,get -n 500000 -P 5  $@
# sadd,hset,spop,mset,,incr,lpush,rpush,lpop,rpop,lrange
