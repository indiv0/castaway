#import sys
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

    msg_type = ffi.getctyle(ffi.typeof(message))
    msg_size = ffi.sizeof(msg[0])
    msg_handle = ffi.cast(ffi.typeof(msg), lib.malloc(msg_size))
    ffi.memmove(msg_handle, message, msg_size)

    if msg_type == "msg_appendentries_t *":
        entry_size = ffi.sizeof(ffi.getctype("msg_entry_t")) * msg_handle.

    print("callback 'send_request_vote' issued to {p} from {s}".format(p=peer,s=s.id))
    #s.outbox.push((peer, message))
    #print(
    #    "send_request_vote: {{peer: {p}, term: {t}, candidate_id: {c}, last_log_index: {i}, last_log_term: {l}}}".format(
    #        p=peer,
    #        t=message.term,
    #        c=message.candidate_id,
    #        i=message.last_log_index,
    #        l=message.last_log_term))


#class MessageRequestVote(object):
#    def __init__(self, obj):
#        self.__obj = obj


#class MessageQueue(object):
#    def __init__(self):
#        self.__inner = []
#
#
#    def push(self, item):
#        self.__inner.append(item)
#
#
#    def pop(self):
#        if self.__inner:
#            return self.__inner.pop(0)
#        else:
#            return None
#
#
#    def peek(self):
#        return self.__inner[-1]


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

        #self.inbox = MessageQueue()
        #self.outbox = MessageQueue()


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


    #def receive_msg(self, peer, msg):
    #    msg_type = ffi.getctype(ffi.typeof(msg))

    #    print(msg_type)
    #    if msg_type == "MessageRequestVote *":
    #        #peer = ffi.new("Id *", peer)
    #        #res = lib.raft_server_receive(self.__obj, peer, msg)
    #        pass


#class WrapperException(Exception):
#    """Base class for exceptions which wrap libcastaway exceptions"""
#
#    @staticmethod
#    def last_error():
#        message = last_error_message()
#
#        if message is None:
#            return WrapperException("(no error available)")
#        else:
#            return WrapperException(message)


#def last_error_message():
#    last_error_len = lib.last_error_length()
#    if last_error_len == 0:
#        return None
#
#    array = ffi.new("char[]", last_error_len)
#    ret = lib.last_error_message(array, last_error_len)
#    if ret <= 0:
#        # This code should be unreachable
#        print("Error while reading last error message")
#        sys.exit(1)
#
#    error_msg_buf = ffi.buffer(array)
#    message = bytes(error_msg_buf).decode("utf-8")
#    return message


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

        #for server in servers:
        #    msg = server.outbox.pop()
        #    if msg:
        #        peer = msg[0]
        #        if msg:
        #            servers[peer].inbox.push(msg)

        #for server in servers:
        #    msg = server.inbox.pop()
        #    if msg:
        #        server.receive_msg(msg[0], msg[1])


if __name__ == "__main__":
    main()
