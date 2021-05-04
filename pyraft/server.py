import asyncio
from datetime import timedelta
import uuid

QUORUM_COUNT = 0.5


class NodeServer:
    """
    Class for Raft Node representation
    """

    def __init__(self, host, port, peers=None):
        self.host = host
        self.port = port
        self.peers = set(peers) if peers is not None else set()

        # Taken from Martin Kleppmann's YT Lectures
        self.raft_index = 0
        self.current_term = 0
        self.last_term = 0
        self.voted_for = None
        self.data_log = RaftLog()
        self.leader = None

        # Wait for 5 seconds before sending in a heartbeat
        # if current Node is a leader
        self.heartbeat = timedelta(seconds=5)

        # Timeout for each request
        self.timeout = 1
        self._clients = [NodeClient(*peer, timeout=self.timeout, node=self)
                         for peer in self.peers]

        self.id = uuid.uuid1()

        # acked and sent length of logs
        self.sent_length = defaultdict(int)
        self.acked_length = defaultdict(int)

    async def broadcast(self, writer, data, ping=False):
        """
        If we receive a message from a client, we forward the request
        to the leader, and if we are leader, we consider the operation
        to be replicated with other followers.
        We also send in regular heartbeat to all follower Nodes, and ask
        them to replicate the logs.
        """
        if self.leader != self.id:
            # If we are follower
            # forward the request to Leader
            return

        # We are Leader :P
        self.log.append((data['msg'], data['term']))
        self.acked_length[self.id] = self.log.length()

        # We try to replicate a given prefix of log among all the
        # followers. If Quorum of nodes are successful, we commit
        # the log entries. Else, we decrease the prefix size and
        # try again

        # This is a typical producer-consumer problem, where we have bunch
        # of replicatelog calls to different followers, and we want to queue
        # in more replicatelog calls in case the larger prefix call fails, so
        # we retry with a smaller prefix call.

        queue = asyncio.Queue()

        # Lets prepare the initial list of producers
        for follower in self._clients:
            await queue.put((follower, self.sent_length[follower.id]))

        # Lets run the consumer, and replicate the logs
        consumer = [asyncio.create_task(
            self._consumer(queue)) for _ in range(5)]

        # block till the all the calls are consumed
        await queue.join()

        # cancel the consumers which are idle
        _ = [c.cancel() for c in consumer]

        # Commit the log entries which are acknowledged
        # by Quorum of Nodes
        resp = self.commit_logs()

        # Write the response
        writer.writelines([bytes(b) for b in resp])
        await writer.drain()

    async def _consumer(self, queue):
        """
        For each replicate call, try to check if the follower
        has same log length, if not, try to reduce the length
        and try again
        """

        while True:
            follower, prefix_length = await queue.get()
            idx = prefix_length
            entries = self.log.get(start=idx, end=self.length()-1)

            prev_log_term = 0
            if idx > 0:
                prev_log_term = self.log.get_term(index=idx-1)

            data = await follower.replicate(dict(leader_id=self.id, current_term=self.current_term,
                                                 idx=idx, prev_log_term=prev_log_term, commit_length=self.raft_index,
                                                 entries=entries))

            if data['term'] > self.current_term:
                self.change_state(Follower, data['term'])
            else:
                if data['success']:
                    self.sent_length[follower.id] = data['ack']
                    self.acked_length[follower.id] = data['ack']
                else:
                    # decrease the prefix and try again
                    queue.put((follower, prefix_length-1))
            queue.task_done()

    def acks(self, length):
        """
        For each peer, return if acked_length >= length
        """
        return [self.acked_length[follower.id] >= length for follower in self._clients]

    def commit_logs(self):
        """
        Commit Log entries which are acknowledged by Quorum
        of Nodes
        """
        ready = [acks(length) >= QUORUM_COUNT for length in range(
            0, self.log.length())]
        if len(ready) > 0 and max(ready) > self.raft_index and self.log.get_term(index=max(ready) - 1) == self.current_term:
            resp = [self.log.get_msg(i)
                    for i in range(self.raft_index, max(ready))]
            self.raft_index = max(ready)
            return resp
        return []

    async def receive(self, writer: asyncio.StreamWriter, data):
        """
        Placeholder for receiving message from other Nodes, we forward
        the message to respective coroutines, based on actions required
        """
        if data['leader_term'] > self.current_term:
            self.current_term = data['leader_term']
            self.voted_for = None

        logOk = False if data['log_length'] >= self.raft_index and self.raft_index > 0 and self.log.get_term(
            index=self.raft_index) == data['log_term'] else True

        ack = 0
        if logOk:
            self.leader = data['leader_id']
            self.append_entries(data['leader_log_length'],
                                data['leader_commit'], data['entries'])
            ack = self.raft_index + len(data['entries'])

        # send response
        writer.writelines(
            bytes(dict(node_id=self.id, term=self.current_term, ack=ack, success=logOk)))
        await writer.drain()

    def append_entries(self, leader_log_length, leader_commit, entries):
        """
        Append entries suffix of leader's logs to the Node
        Note that we can have inconsistencies in log index and term,
        so we need to truncate the nodes logs and match the terms.
        """

        if len(entries) > 0 and self.log.length() > leader_log_length:
            if self.log.get_term(leader_log_length) != entries[0][1]:
                self.log.truncate(leader_log_length)

        # In case the log length of leader + entry size > node's log entry size
        # we need to rewrite the values, to be consistent with leader
        if leader_log_length + len(entries) > self.log.length():
            for i in range(self.log.length() - leader_log_length, len(entries)):
                self.log.append(entries[i])

        # if the leader's commit > commit_length of node
        if leader_commit > self.raft_index:
            for i in range(self.raft_index, leader_commit):
                # deliver the msg, self.log.get_msg(i)
                # but how???
                pass
            self.raft_index = leader_commit

    def quorum(self, votes):
        """
        Check if Quorum can be achieved, 
        should be greater than 0.5 for majority
        """
        return ((votes + 1) / len(self._clients)) >= 0.5

    def change_state(self, new_state, new_term):
        """
        Change the state of the Raft Node to anyone of them
        """
        self.__class__ = new_state
        self.voted_for = None
        self.current_term = new_term

    def getlogterm(self):
        """
        Return the last term committed in the raft logs
        """
        # TODO: Fill me
        pass
