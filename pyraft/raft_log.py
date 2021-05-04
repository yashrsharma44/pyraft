

class RaftLog:

    def __init__(self):
        self.log = []

    def append(self, data: (int, int)):
        """
        data -> msg, term
        """
        self.log.append(data)

    def commit(self):
        pass

    def length(self):
        return len(self.log)

    def get(self, start, end):
        """
        start and end are all inclusive
        """
        return self.log[start:end+1]

    def get_term(self, index):
        """
        Return the term of the log entry
        """
        return self.log[index][1]

    def get_msg(self, index):
        """
        Return the msg of the log entry
        """

    def truncate(self, index):
        """
        Truncate the log entry uptil that index
        """
        self.log = self.log[:index-1]
