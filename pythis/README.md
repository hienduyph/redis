# An incomplete Redis Written In Python

Just for fun and learn how redis works

```bash
pip install -r ./requirements.txt
./main.py
```

```bash
redis-cli -p 6380
```

## Benchmark

**Script to run `./ben.sh`**

```
$ redis-server

SET: 102480.02 requests per second, p50=0.247 msec
GET: 102040.81 requests per second, p50=0.247 msec
```

```bash
./main.py --handle uvloop
SET: 81967.21 requests per second, p50=0.495 msec
GET: 88636.77 requests per second, p50=0.447 msec
```


```bash
./main.py --handle non_blocking_socket
SET: 71890.73 requests per second, p50=0.671 msec
GET: 78173.86 requests per second, p50=0.623 msec
```

```bash
./main.py --handle asyncio

SET: 61743.64 requests per second, p50=0.783 msec
GET: 65487.88 requests per second, p50=0.743 msec
```

```bash
$ cargo run --bin redus-server --release

SET: 97370.98 requests per second, p50=0.263 msec
GET: 96711.80 requests per second, p50=0.271 msec
```
