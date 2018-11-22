from contextlib import ExitStack
from castaway_cffi import ffi, lib


class ArrayUintPtr(object):
    def __init__(self, values):
        self.__obj = ffi.new("uintptr_t[]", values)
        self.__len = len(values)


    def ptr(self):
        return self.__obj


    def len(self):
        return self.__len


class RaftServer(object):
    def __init__(self, id, servers):
        servers = ArrayUintPtr(servers)
        self.__obj = lib.raft_server_new(id, servers.ptr(), servers.len())


    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        lib.raft_server_free(self.__obj)
        self.__obj = None


def main():
    server_ids = [i for i in range(0, 5)]
    with ExitStack() as stack:
        servers = [RaftServer(i, server_ids) for i in server_ids]
        [stack.enter_context(mgr) for mgr in servers]

        print("Initialized servers: {}".format(server_ids))


if __name__ == "__main__":
    main()
