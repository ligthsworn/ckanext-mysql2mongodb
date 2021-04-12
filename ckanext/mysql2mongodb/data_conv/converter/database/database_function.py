from abc import ABCMeta, abstractmethod


class DatabaseFunctionsOptions:

    def __init__(self, host, username, password, port, dbname):
        super(DatabaseFunctionsOptions, self).__init__()
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.dbname = dbname


class DatabaseFunctions:
    __metaclass__ = ABCMeta

    def __init__(self, options: DatabaseFunctionsOptions):
        super(DatabaseFunctions, self).__init__()
        self.options = options

    @abstractmethod
    def restore(self, filePath): raise NotImplementedError

    @abstractmethod
    def backup(self, filePath): raise NotImplementedError
