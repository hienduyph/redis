#!/usr/bin/env python3

import asyncio
from asyncio.transports import Transport
import collections
import itertools
import socket
import sys
import selectors
from typing import Any, List, Optional, Union

expirations: dict[bytes, Any] = collections.defaultdict(lambda: float("inf"))
data: dict[bytes, Any] = {}

PORT = 6380
HOST = "127.0.0.1"

TERMINATE = b"\r\n"
TYPE_ARRAY = ord(b"*")
TYPE_BULK_STRING = ord(b"$")

def find_closed(buf: bytes, cursor: int, next_char=TERMINATE) -> int:
    c = cursor
    char_len = len(next_char)
    for i in range(len(buf) - cursor - char_len):
        _slice = buf[c+i:c+i+char_len]
        if next_char == _slice:
            return c+i # index of the start char
    return -1

class Reader:
    def __init__(self) -> None:
        self.cmds = collections.deque()

    def feed(self, buf: bytes):
        # assume we received full frames
        cursor = 0
        cmds = []
        while cursor < len(buf):
            type_ =  buf[cursor]
            cursor += 1
            if type_ == TYPE_ARRAY:
                array_size = int(chr(buf[cursor]))
                cursor += 3
                for _ in range(array_size):
                    _type = buf[cursor]
                    cursor += 1
                    if _type == TYPE_BULK_STRING:
                        # find next \r\n
                        end = find_closed(buf, cursor, TERMINATE)
                        if end == -1:
                            raise Exception(f"Invalid length at pos {cursor}")
                        string_length = int(buf[cursor:end])
                        cursor += (2 + end - cursor)
                        cmds.extend(buf[cursor:cursor+string_length].split(b" "))
                        cursor += (string_length + 2) # plus term
        self.cmds.append(cmds)

    def gets(self) -> Union[bool, List[bytes]]:
        if len(self.cmds) == 0:
            return False
        return self.cmds.popleft()


class Core:
    def __init__(self) -> None:
        self.parser = Reader()
        self.commands = {
            b"COMMAND": self.com_command,
            b"GET": self.com_get,
            b"SET": self.com_set,
            b"PING": self.com_ping,
            b"INCR": self.com_incr,
            b"LPUSH": self.com_lpush,
            b"RPUSH": self.com_rpush,
            b"LRANGE": self.com_lrange,
            b"LPOP": self.com_lpop,
            b"RPOP": self.com_rpop,
            b"CONFIG": self.com_config,
        }

    def process(self, buf: bytes) -> list[bytes]:
        self.parser.feed(buf)
        resp = []
        while True:
            req = self.parser.gets()
            if req is False:
                break
            cmd = req[0].upper()
            # print(f"Handle comand {cmd}")
            res = self.commands[cmd](*req[1:])
            resp.append(res)
        return resp

    def com_command(self):
        # just enough to pleasure the redis-cli
        return b"+OK\r\n"

    def com_set(self, *args) -> bytes:
        key = args[0]
        value = args[1]
        expires_at = None
        cond = b""
        self._evit_if_expired(key)

        if len(args) == 3:
            cond = args[2]  # SET key value NX|XX
        elif len(args) >= 4:
            # set key [NX|XX] [EX seconds | PX millis | EXAT unix_time_secs | PXAT unix_time_millis ]
            duration = 0
            try:
                if args[2] == b"EX":
                    duration = int(args[3])
                elif args[2] == b"PX":
                    duration = int(args[3]) / 1000
                else:
                    return b"-ERR syntax error\r\n"

            except ValueError:
                return b"-value is not an integer or out of range"

            if duration <= 0:
                return b"-ERR invalid expire time in set\r\n"

            expires_at = time.monotonic() + duration
            if len(args) == 5:
                cond = args[4]

        if cond == b"":
            pass
        elif cond == b"NX":
            if key in data:
                return b"$-1\r\n"
        elif cond == b"XX":
            if key not in data:
                return b"$-1\r\n"
        else:
            return b"-ERR syntax error\r\n"
        self._set(key, value, expires_at)
        return b"+OK\r\n"

    def com_get(self, key: bytes) -> bytes:
        value = self._get(key)
        if not value:
            return b"-1\r\n"
        if not isinstance(value, bytes):
            return b"-WRONGTYPE Operation against a key holding the wrong kind of value"
        return b"$%d\r\n%s\r\n" % (len(value), value)

    def com_ping(self, msg=b"PONG"):
        return b"$%d\r\n%s" % (len(msg), msg)

    def com_config(self, *_: bytes):
        return b"*0\r\n"

    def com_incr(self, key):
        value = self._get(key) or 0
        if type(value) is bytes:
            try:
                value = int(value)
            except ValueError:
                return "b-value is not an integer or out of range"
        value += 1
        self._set(key, str(value).encode())
        return b":%d\r\n" % value

    def com_lpush(self, key: bytes, *values: list[bytes]):
        deque = self._get(key, collections.deque())
        if not isinstance(deque, collections.deque):
            return b"-WRONGTYPE Operation against a key holding the wrong kind of value"

        deque.extendleft(values[::-1])
        self._set(key, deque)
        return b":%d\r\n" % (len(deque),)

    def com_rpush(self, key: bytes, *values: list[bytes]):
        deque = self._get(key, collections.deque())
        if not isinstance(deque, collections.deque):
            return b"-WRONGTYPE Operation against a key holding the wrong kind of value"
        deque.extend(values)
        self._set(key, deque)
        return b":%d\r\n" % (len(deque),)

    def com_lrange(self, key: bytes, *args: bytes):
        deque: Optional[collections.deque] = self._get(key, None)
        if deque is None:
            return b"*0\r\n"
        if not isinstance(deque, collections.deque):
            return (
                b"-WRONGTYPE operations against a key holding the wrong kind of value"
            )
        start = 0
        end = len(deque)
        if len(args) > 0:
            start = int(args[0])
        if len(args) >= 1:
            end = int(args[1])

        if start < 0:
            start = start + len(deque)
        if end < 0:
            end = end + len(deque)

        if end > len(deque):
            end = len(deque)

        if start >= len(deque):  # out of range
            return b"*0\r\n"

        items = [
            b"$%d\r\n%s\r\n" % (len(v), v) for v in itertools.islice(deque, start, end)
        ]
        prefix = b"*%d\r\n" % len(deque)
        return prefix + b"".join(items)

    def com_lpop(self, key: bytes, count: Optional[bytes] = None):
        deque = self._get(key, None)
        if deque is None:
            return b"*0\r\n"
        if not isinstance(deque, collections.deque):
            return b"-WRONGTYPE invalid type"

        size = 1
        if count is not None:
            size = int(count)
        if size >= len(deque):
            size = len(deque)

        items = []
        for _ in range(0, size):
            item = deque.popleft()
            items.append(b"$%d\r\n%s\r\n" % (len(item), item))

        self._set(key, deque)
        size = b"*%d\r\n" % len(items)
        return size + b"".join(items)

    def com_rpop(self, key: bytes, count: Optional[int] = None):
        deque = self._get(key, None)
        if not deque:
            return b"*0\r\n"
        if not isinstance(deque, collections.deque):
            return b"-WRONGTYPE invalid type"
        size = 1
        if count is not None:
            size = int(count)
        if size > len(deque):
            size = len(deque)
        items = []
        for _ in range(size):
            item = deque.pop()
            items.append(b"$%d\r\n%s\r\n" % (len(item), item))
        self._set(key, deque)
        size = b"*%d\r\n" % len(items)
        return size + b"".join(items)

    def _get(self, key, default=None):
        self._evit_if_expired(key)
        return data.get(key, default)

    def _set(self, key, value, expires_at: Optional[float] = None):
        data[key] = value
        if expires_at is not None:
            expirations[key] = expires_at
        else:
            expirations.pop(key, None)

    def _evit_if_expired(self, key):
        if key in expirations and expirations[key] < time.monotonic():
            del expirations[key]
            del data[key]


class AsyncRedisProtocolImpl(asyncio.Protocol):
    """
    State machine of calls:

      start -> CM [-> DR*] [-> ER?] -> CL -> end

    * CM: connection_made()
    * DR: data_received()
    * ER: eof_received()
    * CL: connection_lost()
    """

    def __init__(self) -> None:
        self.parser = Reader()
        self.transport: Optional[Transport] = None
        self._funs = Core()

    def connection_made(self, transport: Transport):
        self.transport = transport

    def data_received(self, data: bytes) -> None:
        resp = self._funs.process(data)
        if self.transport:
            self.transport.writelines(resp)


class NonBlockingSocket:
    def __init__(self):
        self._funcs = Core()
        self.run = True

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        # implement a non blocking server using selectors
        # https://docs.python.org/3/library/selectors.html#examples
        print("Handle using no blocking socket")
        import signal

        def _stop(*args: Any):
            self.run = False
            print("Received graceful shutdown")

        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)

        sel = selectors.DefaultSelector()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((HOST, PORT))
        sock.listen()
        sock.setblocking(False)
        print(f"Wating for conn at {PORT}")
        sel.register(sock, selectors.EVENT_READ, self.accept(sel, self.run))
        while self.run:
            events = sel.select(timeout=5)
            for key, mask in events:
                if key.data is None:
                    continue
                key.data(key.fileobj, mask)

        print("Shutdown, good bye ...")

    def accept(self, sel: selectors.BaseSelector, run: bool):
        def _h(key: socket.socket, mask: int):
            conn, addr = key.accept()
            # print(f"connected {addr}")
            conn.setblocking(False)
            sel.register(conn, selectors.EVENT_READ, self.handle(addr, sel, run))

        return _h

    def handle(self, addr: str, sel: selectors.BaseSelector, run: bool):
        def _h(
            conn: socket.socket,
            mask: int,
        ):
            try:
                buf = conn.recv(1024)
                if not buf:
                    # print(f"{addr} disconnected")
                    sel.unregister(conn)
                    conn.close()
                    return
                res = self._funcs.process(buf)
                for r in res:
                    conn.sendall(r)
            except Exception as e:
                print(f"{addr} handle error: {e}")
                sel.unregister(conn)
                conn.close()

        return _h


def asyncmain(uv = False) -> int:
    if uv:
        import uvloop
        print("Using uvloop")
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    loop = asyncio.new_event_loop()
    coro = loop.create_server(AsyncRedisProtocolImpl, HOST, PORT)
    server = loop.run_until_complete(coro)
    print(f"Listening on {PORT}")
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # graceful shutdown
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()
    return 0


if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--handle", type=str, default="non_blocking_socket", help="Choose mode type to run non_blocking_socket/asyncio/uvloop"
    )
    args = parser.parse_args()
    handles = {
        "non_blocking_socket": NonBlockingSocket(),
        "asyncio": asyncmain,
        "uvloop": lambda: asyncmain(uv=True),
    }
    sys.exit(handles[args.handle]())
