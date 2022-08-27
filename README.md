# Rebuild Redis from Scratch

## Benchmark

**Script to run `./ben.sh`**

### Redis

```
$ redis-server
SET: 528262.00 requests per second, p50=0.255 msec
GET: 513083.62 requests per second, p50=0.255 msec
```

### Python

```bash
./main.py --handle protocol
SET: 168548.80 requests per second, p50=1.463 msec
GET: 190294.95 requests per second, p50=1.263 msec
```


```bash
./main.py --handle server
SET: 151998.78 requests per second, p50=1.639 msec
GET: 167140.23 requests per second, p50=1.487 msec
```

```bash
./main.py --handle plain_socket
SET: 117247.04 requests per second, p50=2.111 msec
GET: 125691.30 requests per second, p50=1.967 msec
```

### Rust

```bash
$ cargo run --bin redus-server --release

SET: 475059.38 requests per second, p50=0.279 msec
GET: 467726.84 requests per second, p50=0.279 msec
```
