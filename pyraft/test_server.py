import asyncio
import time
"""
Server Implementation for Connecting with TCP Connection
"""


async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    print(f"received from {addr!r}")
    while True:
        data = await reader.readline()

        message = data.decode().strip()
        if message == "exit":
            break
        print(f"{addr!r}> {message}")
        time.sleep(1)
        writer.write(data)
        await writer.drain()
    print("connection closed by client")
    writer.close()


async def main():
    server = await asyncio.start_server(_handle, '127.0.0.1', 8888)

    addr = server.sockets[0].getsockname()
    print(f"Serving on {addr}")

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
