commands = ["SET", "GET"]
commands_to_persist = ["SET", "FLUSHDB", "INCR", "RPUSH", "LPUSH", "RPOP", "EXPIRE"]


def delimited_resp(resp: str):
    delimiter = "\r\n"
    return f"*1{delimiter}${len(resp)}{delimiter}{resp}{delimiter}"


def create_response(resp: str):
    # check if resp is a list
    if isinstance(resp, list):
        return create_array_response(resp)
    delimiter = "\r\n"
    return f"+{resp}{delimiter}"


def create_array_response(resp: list):
    delimiter = "\r\n"
    r = f"*{len(resp)}{delimiter}" + "".join(
        [f"${len(i)}{delimiter}{i}{delimiter}" for i in resp]
    )
    return r


def get_command_from_response(resp: str):
    resp = [k for k in resp.split("\r\n") if k != ""]
    if not resp:
        return
    print("resp", resp)
    cmd_len = resp[0].split("*")[1]
    cmd = resp[2]
    args = [r for i, r in enumerate(resp[3:]) if i % 2 != 0]
    return cmd_len, cmd, args


def is_command_persistable(cmd: str) -> bool:
    return cmd in commands_to_persist


def persist_to_aof(resp: str) -> None:
    with open("aof.txt", "a+") as aof:
        aof.write(resp + "----\n")
