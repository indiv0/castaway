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


def send_request_vote(udata, peer, message):
    s = ffi.from_handle(udata)
    print("callback 'send_request_vote' issued to {p} from {s}".format(p=peer,s=s.id))


def verify_option(ptr, errno):
    if errno == lib.CASTAWAY_OPT_SOME:
        assert ptr != ffi.NULL
        return ptr[0]
    elif errno == lib.CASTAWAY_OPT_NONE:
        return None
    else:
        raise WrapperException.last_error()


class RaftServer(object):
    def __init__(self, id, servers):
        servers = ArrayUintPtr(servers)
        self.__obj = lib.raft_server_new(id, servers.ptr(), servers.len())
        self.id = id

        user_data = ffi.new_handle(self)
        self._user_data = user_data

        self.load_callbacks()
        callbacks = ffi.new("Callbacks *")
        callbacks.send_request_vote = self.send_request_vote
        lib.raft_server_register_callbacks(self.__obj, callbacks, user_data)
        self._callbacks = callbacks


    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        lib.raft_server_free(self.__obj)
        self.__obj = None
        # TODO: is releasing these resources necessary?
        self._user_data = None
        self._callbacks = None


    def load_callbacks(self):
        self.send_request_vote = ffi.callback("void (const void *, Id, MessageRequestVote*)", send_request_vote)


    def periodic(self, ms_since_last_period):
        lib.raft_server_periodic(self.__obj, ms_since_last_period)


    def voted_for(self):
        voted_for_p = ffi.new("Id *")
        res = lib.raft_server_voted_for(self.__obj, voted_for_p)
        return verify_option(voted_for_p, res)


def main():
    server_ids = [i for i in range(0, 5)]
    with ExitStack() as stack:
        servers = [RaftServer(i, server_ids) for i in server_ids]
        [stack.enter_context(mgr) for mgr in servers]

        print("Initialized servers: {}".format(server_ids))

        for s in servers:
            print("server {s} voted for: {p}".format(s=s.id, p=s.voted_for()))
            assert s.voted_for() is None

        for s in servers:
            s.periodic(500)

        for s in servers:
            print("server {s} voted for: {p}".format(s=s.id, p=s.voted_for()))
            assert s.voted_for() == s.id


if __name__ == "__main__":
    main()
