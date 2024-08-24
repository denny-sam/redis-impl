import typing
from time import time
import logging

from utils import (
    create_response,
    delimited_resp,
    get_command_from_response,
    is_command_persistable,
    persist_to_aof,
)


class Redis:
    db = {}
    expiry = {}

    async def handle_command(self, resp: str, write_to_aof: bool = True):
        if not resp:
            return
        cmd_len, cmd, args = get_command_from_response(resp)
        cmd = cmd.upper()
        print(f"Command received {cmd_len, cmd, args}")

        self.expire_keys()

        if is_command_persistable(cmd) and write_to_aof:
            persist_to_aof(resp)

        # Command: PING
        if resp == delimited_resp("PING"):
            return create_response("PONG")

        # Command: FLUSHDB
        elif resp == delimited_resp("FLUSHDB"):
            self.flush()
            return create_response("OK")

        # Command: SET key value
        elif cmd == "SET":
            key = args[0]
            value = args[1]
            self.set(key, value)
            return create_response(f"SET {key} {value}")

        # Command: GET key value
        elif cmd == "GET":
            key = args[0]
            return create_response(self.get(key))

        # Command: INCR key
        elif cmd == "INCR":
            key = args[0]
            return create_response(self.incr(key))

        # Command: RPUSH list value
        elif cmd == "RPUSH":
            list_name = args[0]
            key = args[1]
            return create_response(self.rpush(list_name, key))

        # Command: LPUSH list value
        elif cmd == "LPUSH":
            list_name = args[0]
            key = args[1]
            return create_response(self.lpush(list_name, key))

        # Command: RPOP list
        elif cmd == "RPOP":
            list_name = args[0]
            return create_response(self.rpop(list_name))

        # Command: EXPIRE key seconds
        elif cmd == "EXPIRE":
            key = args[0]
            time_in_sec = float(args[1])
            self.set_expiry(key, time_in_sec)
            return create_response("OK")

        else:
            return "+\r\n"

    def __init__(self):
        self.db = {}
        self.expiry = {}

    def set(self, key: str, value: str) -> None:
        self.db[key] = value

    def get(self, key: str) -> typing.Optional[str]:
        return self.db.get(key)

    def flush(self):
        self.db = {}

    def incr(self, key: str) -> int:
        self.db[key] = int(self.db.get(key)) + 1
        return self.db[key]

    def rpush(self, list_name: str, value: str) -> typing.List:
        if not self.get(list_name):
            self.set(list_name, [])

        self.db[list_name].append(value)
        return self.db[list_name]

    def lpush(self, list_name: str, value: str) -> typing.List:
        if not self.get(list_name):
            self.set(list_name, [])

        self.db[list_name].insert(0, value)
        return self.db[list_name]

    def rpop(self, list_name: str) -> typing.Optional[str]:
        if not self.get(list_name):
            raise ValueError("List is empty")
        return self.db[list_name].pop()

    def set_expiry(self, key: str, time_in_secs: int):
        if not self.get(key):
            raise ValueError("Key does not exist")

        self.expiry[key] = time() + time_in_secs

    def expire_keys(self):
        for key, _ in self.expiry.items():
            if self.db.get(key) and self.expiry[key] < time():
                del self.db[key]

    async def load_from_file(self):
        file_path = "aof.txt"
        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read()
            commands = text.split("----\n")
            for cmd in commands:
                cmd = "\r\n".join(cmd.split("\n"))
                if cmd:
                    await self.handle_command(cmd, False)
        logging.info("file loaded")
        return
