import socket
from redis import Redis

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

def recv(c: socket):
    return c.recv(1024).decode('utf-8')


host = ''
port = 8885

redis_db = Redis()
redis_db.load_from_file()
server_socket =  socket.create_server(("localhost", port), reuse_port=True)

print("Waiting for connections")

client, addr = server_socket.accept()
print("Connection established with", addr)


print("Waiting for message...")
while True:
    resp = recv(client)
    print(resp)

    msg = redis_db.handle_command(resp)
    client.send(bytes(msg, 'utf-8'))


