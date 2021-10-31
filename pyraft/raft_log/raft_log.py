
import aiofiles
import asyncio


DELIMITER = "$"


class RaftLogException(Exception):
    pass


class RaftLog:
    """
    Class for storing the raft state of a Node
    in a logs
    """

    def __init__(self, file_name):
        self.log = []
        self.file_name = file_name
        self.commit_len = 0

        # load the previous log entries
        asyncio.run(self._load_logs())

    async def _load_logs(self):
        """
        Load the log file into the log array
        """
        try:
            async with aiofiles.open(self.file_name, 'r+') as f:
                for line in f:
                    msg, term = line.split(DELIMITER)
                    self.log.append((msg, term))
        except FileNotFoundError:
            pass

    def append(self, data) -> None:
        """
        data -> msg, term
        """
        self.log.append(data)

    async def commit(self):
        """
        Commit the current logs
        """

        if self.commit_len > self.length():
            raise RaftLogException(
                f"entries in the committed files: ${self.commit_len} are greater than the log entries :${self.length()}")

        # Open the file for logs and append the log entries
        async with aiofiles.open(self.file_name, 'a') as f:
            for entry in self.log[self.commit_len:]:
                await f.write(entry[0] + DELIMITER + str(entry[1]) + "\n")

        self.commit_len = len(self.log)

    def length(self):
        return len(self.log)

    def get(self, start: int, end: int):
        """
        start is inclusive
        """
        return self.log[start:end]

    def get_term(self, index: int):
        """
        Return the term of the log entry
        """
        return self.log[index][1]

    def get_msg(self, index: int):
        """
        Return the msg of the log entry
        """
        return self.log[index][0]

    def truncate(self, index: int):
        """
        Truncate the log entry uptil that index
        """
        if index < self.commit_len:
            raise RaftLogException(
                f"provided index: {index} is less than the commit index")
        self.log = self.log[:index-1]
