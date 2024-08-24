import asyncio
import logging
import socket

from redis import Redis

redis_db = Redis()


def recv(c: socket):
    return c.recv(1024).decode("utf-8")


class Server:
    def __init__(self):
        self.host = "localhost"
        self.port = 8885

    async def start(self):
        server = await asyncio.start_server(
            self.handle_connections, self.host, self.port
        )

        addr = server.sockets[0].getsockname()
        print(f"Serving on {addr}")
        async with server:
            await server.serve_forever()

    async def handle_connections(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        addr = writer.get_extra_info("peername")
        logging.info(f"Connected to {addr}")
        await RequestHandler(reader, writer).process_request()


class RequestHandler:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer

    async def process_request(self):
        request = await self.reader.read(2048)
        request = request.decode("utf-8")
        logging.info(f"Request {request}")
        await self.handle_request(request)

    async def handle_request(self, request):
        msg = await redis_db.handle_command(request)
        self.writer.write(msg.encode())
        await self.writer.drain()
        self.writer.close()


async def main():
    await redis_db.load_from_file()
    server = Server()
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
