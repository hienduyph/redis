#!/usr/bin/env python3

import asyncio
from asyncio.transports import Transport
import collections
import sys
import time
from typing import Any, Optional

import hiredis
import uvloop
from loguru import logger

expirations: dict[bytes, Any] = collections.defaultdict(lambda: float("inf"))
data: dict[bytes, Any] = {}


class Core:
    def __init__(self) -> None:
        self.commands = {
            b"GET": self.com_get,
            b"SET": self.com_set,
            b"COMMAND": self.com_command,
        }

    def com_command(self, *args):
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


class RedisProtocol(asyncio.Protocol, Core):
    """
    State machine of calls:

      start -> CM [-> DR*] [-> ER?] -> CL -> end

    * CM: connection_made()
    * DR: data_received()
    * ER: eof_received()
    * CL: connection_lost()
    """

    def __init__(self) -> None:
        self.parser = hiredis.Reader()
        self.transport: Optional[Transport] = None
        super(Core, self).__init__()

    def connection_made(self, transport: Transport):
        self.transport = transport

    def data_received(self, data: bytes) -> None:
        resp = []
        self.parser.feed(data)
        while True:
            req = self.parser.gets()
            if req is False:
                break
            cmd = self.commands[req[0].upper()]
            resp.append(cmd(*req[1:]))

        if self.transport:
            self.transport.writelines(resp)


def main() -> int:
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.new_event_loop()
    port = 6380
    coro = loop.create_server(RedisProtocol, "127.0.0.1", port)
    server = loop.run_until_complete(coro)
    logger.info("Listening on {}", port)
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

    sys.exit(main())
