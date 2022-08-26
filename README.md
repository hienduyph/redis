# Implement Redis For Fun

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
SET: 164826.11 requests per second, p50=1.535 msec
GET: 191864.94 requests per second, p50=1.303 msec
```


```bash
./main.py --handle server
SET: 145264.39 requests per second, p50=1.791 msec
GET: 161864.69 requests per second, p50=1.583 msec
```

```bash
./main.py --handle plain_socket
# too slow
```

### Rust

```bash
$ cargo run --bin redus-server --release

SET: 475059.38 requests per second, p50=0.279 msec
GET: 467726.84 requests per second, p50=0.279 msec
```
