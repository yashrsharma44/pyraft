from collections import defaultdict
import asyncio


class Follower(NodeServer):
    """
    Follower Node follows the Leader for the consensus,
    and forwards any request to the Leader Node.
    """

    def __init__(self, *args, **kwargs):
        super(Follower, self).__init__(*args, **kwargs)

    async def vote(self, data: dict, writer: asyncio.StreamWriter):
        """
        We might want to vote for a Node, who is contesting for
        an election. Once voted, we cannot vote again.
        Make sure the candidate's term and logs are not outdated
        """

        logOk = (data['log_term'] > self.getlogterm()) or (
            data['log_term'] > self.getlogterm() and data['log_index'] >= self.raft_index)

        termOk = (data['term'] >= self.current_term)

        # TODO(yashrsharma44): Make this operation idempotent
        if (self.voted_for is not None) or (not logOk) or (not termOk):
            writer.writelines([
                bytes(dict(node_id=self.id, term=self.current_term,
                      voted=False, raft_index=self.raft_index), "utf-8")
            ])
            await writer.drain()
            return

        # Candidate's term and raft index is valid, vote for it
        self.current_term = data['term']
        self.voted_for = data['voted_for']
        writer.writelines([
            bytes(dict(node_id=self.id, term=self.current_term,
                       voted=True, raft_index=self.raft_index), "utf-8")
        ])
        await writer.drain()

    async def updatelogs(self):
        """
        Update the logs received by the leader and confirm, if the
        leader logs are consistent with the node's logs
        """


class Candidate(NodeServer):
    """
    Candidate Node runs for an election, and waits for Quorum
    of votes
    """

    def __init__(self, *args, **kwargs):
        super(Candidate, self).__init__(*args, **kwargs)

    async def election(self):
        """
        If the Leader doesnt respond with a heartbeat, or election
        has timed out because we didnt reach a majority, try for an election.
        Get Votes from all other Follower, and see if we reach a Quorum
        of votes
        """
        # Ask all peer clients to vote for us
        # We receive all votes till timeout
        # If timeout, we retry again
        # else we check votes in favor of us
        # If majority, we declare ourselves as Leader
        while True:

            # we increase our current term while we contest as a candidate
            self.current_term += 1
            self.voted_for = self.id
            # Set up tasks for asking votes
            coros = [asyncio.create_task(follower.vote(dict(
                term=self.current_term, log_term=self.getlogterm(), log_index=self.raft_index, vote_for=self.id)))
                for follower in self._clients]

            # Get votes from all followers
            # We cut the connection after timeout
            done, pending = await asyncio.wait(
                coros, timeout=self.timeout, return_when=asyncio.FIRST_EXCEPTION)

            # Once we have the done results, we cancel the pending ones
            _ = [task.cancel() for task in pending]

            # For each vote received, count all votes received in favor of us
            votes_received = set()

            for task in done:
                try:
                    resp = task.result()

                    # If follower's term > currentTerm or followers raft_index > our raft_index
                    # we cancel our candidature and return back to as follower
                    if resp['term'] > self.current_term or resp['raft_index'] > self.raft_index:
                        return self.change_state(Follower)
                    if resp['voted']:
                        votes_received.add(resp['node_id'])

                except Exception:
                    # exception with some client calls
                    # break out of loop, and check for quorum, if achieved
                    # we move on with this, else we send message again
                    break

            # Add our own vote
            votes_received.add(self.id)
            self.voted_for = self.id
            if self.quorum(len(votes_received)):
                # declare ourselves as leader

                # propagate the message
                return self.change_state(Leader)

            # Otherwise, we need to try again
            self.current_term -= 1
            await asyncio.sleep(1)


class Leader(NodeServer):
    """
    Leader Node arrives at consensus, and commits log entries
    It also replicates logs to Nodes, and sends in a heartbeat
    to all other Follower Nodes
    """

    def __init__(self, *args, **kwargs):
        super(Leader, self).__init__(*args, **kwargs)

        # Since the Leader is initialised, we start a
        # non-blocking heartbeat to all the peers
        # TODO(yash): Fill me

    async def heartbeat(self, writer):
        """
        Send in a periodic heartbeat to all the Follower Nodes
        """
        if not self.heartbeat_initialised:
            # Set up a non-blocking ping to all the peer clients
            while True:
                self.broadcast(writer, None, ping=True)
                asyncio.sleep(self.heartbeat.total_seconds())

    async def replicatelog(self):
        """
        We try to replicate our current logs with other Follower
        Nodes, and once we receive a Quorum, we commit the logs
        If no Quorum is reached, we try to replicate again with 
        smaller prefix of logs
        """

    async def commitlog(self):
        """
        We try to commit min prefix of logs which are valid
        """
