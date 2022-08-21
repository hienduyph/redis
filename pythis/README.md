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
./main.py --handle protocol
SET: 177242.11 requests per second, p50=1.407 msec
GET: 202101.86 requests per second, p50=1.223 msec
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

```bash
$ cargo run --bin redus-server --release

SET: 97370.98 requests per second, p50=0.263 msec
GET: 96711.80 requests per second, p50=0.271 msec
```
