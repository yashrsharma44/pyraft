import asyncio
import json


class Connection:
    """
    Base Class for implementing the interface agnostic
    reader and writer methods.
    """

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._reader = None
        self._writer = None
        self.open = False

    async def connect(self):
        """
        Initialise the connection to the given host and port
        """
        if not self.open:
            self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
            self.open = True

    async def read(self, n=-1):
        """
        read api, read atmost n lines
        """
        await self.connect()
        data = await self._reader.read()
        return data

    async def readline(self):
        """
        read bytes till we encounter '\n'
        """
        await self.connect()
        data = await self._reader.readline()
        return data

    def write(self, data):
        """
        write data using the writer
        """
        return self._writer.write(data)

    def writelines(self, data):
        """
        write list of bytes 
        """
        return self._writer.writelines(data)

    def close(self):
        """
        close the stream, and underlying socket
        """
        return self._writer.close()

    async def drain(self):
        """
        Drain the buffer
        """
        await self._writer.drain()


class NodeClient:
    """
    Client Class for interacting with the Node Object
    """

    def __init__(self, host, port, id, **kwargs):
        self.remote_host = host
        self.remote_port = port
        self.conn = Connection(self.remote_host, self.remote_port)
        self.timeout = kwargs.pop["timeout"]

        node = kwargs.pop["node"]
        self.node_host = node.host
        self.node_port = node.port
        self.id = id

    async def set(self, key, value, timeout=None):
        """
        Set key with value to the remote server
        Timeout for exiting the connection after given time
        """
        timeout = timeout if timeout is not None else self.timeout
        data = await asyncio.wait_for(self._set(key, value), timeout)
        return data

    async def _set(self, key, value):
        await self.conn.connect()
        self.conn.writelines([
            b"set\n",
            bytes(json.dumps(dict(key=key, value=value)), "utf-8"),
            b"\n"
        ])
        await self.conn.drain()
        _ = await self.conn.readline()
        data = await self.conn.readline()
        return data

    async def get(self, key, timeout=None):
        """
        Get the key value from the remote server
        Timeout for exiting the connection after given time
        """
        timeout = timeout if timeout is not None else self.timeout
        data = await asyncio.wait_for(self._get(key), timeout)
        return data

    async def _get(self, key):
        await self.conn.connect()
        self.conn.writelines([
            b"get\n",
            bytes(json.dumps(dict(key=key)), "utf-8"),
            b"\n"
        ])
        await self.conn.drain()
        _ = await self.conn.readline()
        data = await self.conn.readline()
        return data

    async def join(self, host, port, timeout=None):
        """
        Join the server with given host:port to the remote server
        If the remote server is not leader, the connection is
        forwarded to Leader.
        """
        timeout = timeout if timeout is not None else self.timeout
        data = await asyncio.wait_for(self._join(host, port), timeout)
        return data

    async def _join(self, host, port):
        await self.conn.connect()
        self.conn.writelines([
            b"join\n",
            bytes(json.dumps(dict(host=host, port=port)), "utf-8"),
            b"\n"
        ])
        await self.conn.drain()
        _ = await self.conn.readline()
        data = await self.conn.readline()
        return data

    async def replicate(self, timeout=None, **kwargs):
        """
        FILL THIS UP!
        """
        timeout = timeout if timeout is not None else self.timeout
        data = await asyncio.wait_for(self._replicate(**kwargs), timeout)
        return data

    async def _replicate(self, kwargs):
        await self.conn.connect()
        self.conn.writelines([
            b"replicate\n",
            bytes(json.dumps(dict(kwargs)), "utf-8"),
            b"\n"
        ])
        await self.conn.drain()
        _ = await self.conn.readline()
        data = await self.conn.readline()
        return data

    async def vote(self, *args, **kwargs):
        await self.conn.connect()
        self.conn.writelines([
            b"vote\n",
            bytes(dict(kwargs)),
            b"\n"
        ])
        await self.conn.drain()
        data = await self.conn.readline()
        data = data.decode().strip()
        if data is None:
            return dict()
        data = json.loads(data)
        return data


if __name__ == "__main__":
    async def main():
        addr = "127.0.0.1:8888"

        conn = NodeClient(addr.split(':')[0], addr.split(':')[1])

        print("Sending messages..")
        await conn.get("yas")
        await conn.set("yasho", 1)
        await conn.join("127.9.2.2", 9090)
        await conn.replicate(timeout=None, kwargs={"a": "b", "v": "c"})

        print("closing the connection")
        conn.conn.close()

    asyncio.run(main())
