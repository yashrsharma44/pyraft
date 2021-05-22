from aiofiles import os as aio_os
import aiofiles
from pyraft.raft_log.raft_log import RaftLog, RaftLogException
import unittest
import asyncio


class RaftLogTestSuite(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(RaftLogTestSuite, self).__init__(*args, **kwargs)
        self.file_name = "test_log_file"

    def setUp(self):
        self.logger = RaftLog(self.file_name)
        # Create a file named "test_log_file"
        asyncio.run(self.create_file())

    def tearDown(self):
        asyncio.run(aio_os.remove(self.file_name))
        # pass

    async def create_file(self, ):
        async with aiofiles.open(self.file_name, 'w'):
            pass

    def append_logs(self):
        # append logs and check if entries are correctly appended
        entries = [("add 1", 1), ("rem 1", 2), ("sub 2", 3)]
        for entry in entries:
            self.logger.append(entry)
        return entries

    def test_append_logs(self):
        entries = self.append_logs()

        # assert the entries
        stored = self.logger.get(0, 3)
        for i in range(3):
            self.assertEqual(entries[i][0], stored[i][0])
            self.assertEqual(entries[i][1], stored[i][1])

        self.assertEqual(self.logger.length(), 3)

    def test_truncate_logs(self):
        self.append_logs()

        # lets delete the last log
        self.logger.truncate(3)
        # breakpoint()
        self.assertEqual(self.logger.length(), 2)

        # lets delete an invalid index
        self.assertRaises(RaftLogException, self.logger.truncate, -1)

    def test_commit_logs(self):
        entries = self.append_logs()

        # Lets commit the logs
        self.assertEqual(self.logger.commit_len, 0)
        asyncio.run(self.logger.commit())
        self.assertEqual(self.logger.commit_len, 3)

        # assert the entries
        stored = self.logger.get(0, 3)
        for i in range(3):
            self.assertEqual(entries[i][0], stored[i][0])
            self.assertEqual(entries[i][1], stored[i][1])

        self.assertEqual(self.logger.length(), 3)


if __name__ == "__main__":
    unittest.main()
