import socket
from redis import Redis

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

commands = ['SET', 'GET']

def recv(client: socket):
    return client.recv(1024).decode('utf-8')

def delimited_resp(resp: str):
    delimiter = '\r\n'
    return f"*1{delimiter}${len(resp)}{delimiter}{resp}{delimiter}"

def create_response(resp: str):
    # check if resp is a list
    if isinstance(resp, list):
        return create_array_response(resp)
    delimiter = '\r\n'
    return f"+{resp}{delimiter}"

def create_array_response(resp: list):
    delimiter = '\r\n'
    r = f"*{len(resp)}{delimiter}" + ''.join([f"${len(i)}{delimiter}{i}{delimiter}" for i in resp])
    print(r)
    return r

def get_command_from_response(resp: str):
    resp = resp.split('\r\n')
    cmd_len = resp[0].split('*')[1]
    cmd = resp[2]
    args = [r for i, r in enumerate(resp[3:]) if i%2 != 0]
    return cmd_len, cmd, args

def expire_keys(db):
    db.expire_keys()


def handle_command(resp: str, db: Redis):
    cmd_len, cmd, args = get_command_from_response(resp)
    print(f'Command received {cmd_len, cmd, args}')

    expire_keys(db)
     
    # Command: PING
    if resp == delimited_resp('PING'):
        return create_response('PONG')
    
    # Command: FLUSHDB
    elif resp == delimited_resp('FLUSHDB'):
        db.flush()
        return create_response('OK')
    
    # Command: SET key value
    elif cmd == 'SET':
        key = args[0]
        value = args[1]
        redis_db.set(key, value)
        return create_response(f'SET {key} {value}')
    
    # Command: GET key value
    elif cmd == 'GET':
        key = args[0]
        return create_response(redis_db.get(key))
    
    # Command: INCR key
    elif cmd == 'INCR':
        key = args[0]
        return create_response(redis_db.incr(key))
    
    # Command: RPUSH list value
    elif cmd == 'RPUSH':
        list = args[0]
        key = args[1]
        return create_response(redis_db.rpush(list, key))
    
    # Command: LPUSH list value
    elif cmd == 'LPUSH':
        list = args[0]
        key = args[1]
        return create_response(redis_db.lpush(list, key))
    
    # Command: RPOP list
    elif cmd == 'RPOP':
        list = args[0]
        return create_response(redis_db.rpop(list))
    
    elif cmd == 'EXPIRE':
        key = args[0]
        time = float(args[1])
        redis_db.set_expiry(key, time)
        return create_response('OK')
    
    else:
        return '+\r\n'


host = ''
port = 8885

server_socket =  socket.create_server(("localhost", port), reuse_port=True)
print("Waiting for connections")

client, addr = server_socket.accept()
print("Connection established with", addr)

redis_db = Redis()

print("Waiting for message...")
while True:
    resp = recv(client)
    print(resp)

    msg = handle_command(resp, redis_db)
    client.send(bytes(msg, 'utf-8'))


