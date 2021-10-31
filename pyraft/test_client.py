from json import encoder
from collections import defaultdict
from pyraft.client import NodeClient
import unittest
import uuid
import asyncio
import logging
import json
import multiprocessing


DELIMITER = "$"


def runner(coro):
    asyncio.run(coro())


class DummyServer:

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.map = defaultdict(str)

    async def serve(self):
        server = await asyncio.start_server(self.handler, self.host, self.port)

        addr = server.sockets[0].getsockname()
        logging.info(f"serving on {addr}")

        async with server:
            await server.serve_forever()

    async def shutdown(self):
        logging.info("shutting down the server")
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

        logging.info(f"cancelling {len(tasks)}")
        [task.cancel() for task in tasks]

        logging.info(f"let the pending tasks finish")
        done, pending = await asyncio.wait(tasks)
        [task.cancel() for task in pending]

    async def handler(self, reader, writer):
        print("received")
        breakpoint()
        data = await reader.readline()
        message = data.decode().strip()
        operation, entry = message.split(DELIMITER)

        if operation == "get":
            writer.write(json.dumps(dict(value=self.get(entry)), "utf-8"))
        elif operation == "set":
            k, v = entry.split(" ")
            self.set(k, v)
            writer.write("done!\n")
        else:
            writer.write("unknown\toperation\n")
        await writer.drain()

    def set(self, key, value):
        self.map[key] = value

    def get(self, key):
        return self.map[key]


class NodeClientTestSuite(unittest.IsolatedAsyncioTestCase):

    def __init__(self, *args, **kwargs):
        super(NodeClientTestSuite, self).__init__(*args, **kwargs)
        self.id = uuid.uuid1()
        self.server_process = None

    def setUp(self):
        self.client = NodeClient("127.0.0.1", 4040, self.id)
        self.server = DummyServer("127.0.0.1", 4040)

    def tearDown(self):
        self.client.disconnect()

    async def test_set(self):
        loop = asyncio.get_event_loop()
        tasks = []
        tasks.append(asyncio.create_task(await loop.run_in_executor(None, asyncio.run(await self.server.serve()))))

        async def assert_val():
            await self.client.set("yash", 2)
            data = await self.client.get("yash")

            data.decode().strip()
            self.assertEqual(data, 1)
        tasks.append(asyncio.create_task(assert_val))
        tasks.append(self.server.shutdown())

        asyncio.wait(tasks)

    # def test_get(self):
    #     pass

    # def test_join(self):
    #     pass

    # def test_vote(self):
    #     pass


if __name__ == "__main__":

    async def runner():
        srv = DummyServer('127.0.0.1', 4041)
        await srv.serve()

    asyncio.run(runner())
