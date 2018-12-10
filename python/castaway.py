#import sys
from contextlib import ExitStack
from castaway_cffi import ffi, lib

NUM_SERVERS = 2
NUM_PERIODS = 100
MS_PER_PERIOD = 10

class ArrayUintPtr(object):
    def __init__(self, values):
        self.__obj = ffi.new("uintptr_t[]", values)
        self.__len = len(values)


    def ptr(self):
        return self.__obj


    def len(self):
        return self.__len


class Simulator(object):
    def __init__(self):
        self.period = 0
        self.messages = []
        self.servers = []
        self.stack = ExitStack()


    def add_server(self, id, server_ids):
        new_server = RaftServer(id, server_ids)
        for server in self.servers:
            new_server.add_peer(server)
            server.add_peer(new_server)
        self.servers.append(new_server)
        self.stack.enter_context(new_server)
        print("CAST: Initialized server: {}".format(id))


    def enqueue_message(self, message):
        self.messages.append(message)
        print("CAST: enqueued ({}) message from {} to {}".format(
            ffi.getctype(ffi.typeof(message.handle)),
            message.sender.id,
            message.recipient.id))


    def poll_message(self, message):
        msg_type = ffi.getctype(ffi.typeof(message.handle))

        if msg_type == "MessageRequestVote *":
            print("CAST: polling message from {} to {}: ({})".format(
                message.sender.id,
                message.recipient.id,
                ffi.getctype(ffi.typeof(message.handle))))
            rvr = lib.raft_server_handle_request_vote_request(
                    message.recipient.raft,
                    message.sender.id,
                    message.handle)
            response = Message.from_raw(ffi.addressof(rvr), message.recipient, message.sender)
            self.enqueue_message(response)
        elif msg_type == "MessageRequestVoteResponse *":
            print("CAST: polling message from {} to {}: ({})".format(
                message.sender.id,
                message.recipient.id,
                ffi.getctype(ffi.typeof(message.handle))))
            if message.handle.vote_granted:
                print("CAST: server {} received vote from peer {}".format(
                    message.recipient.id,
                    message.sender.id))
            lib.raft_server_handle_request_vote_response(
                    message.recipient.raft,
                    message.sender.id,
                    message.handle)
            # FIXME
            #print("CAST: message: (term: {}, vote_granted: {})".format(
            #    message.handle.term,
            #    message.handle.vote_granted))
        elif msg_type == "MessageAppendEntriesRaw *":
            print("CAST: polling message from {} to {}: ({})".format(
                message.sender.id,
                message.recipient.id,
                ffi.getctype(ffi.typeof(message.handle))))
            aer = lib.raft_server_handle_append_entries_request(
                    message.recipient.raft,
                    message.sender.id,
                    message.handle)
            response = Message.from_raw(
                    ffi.addressof(aer),
                    message.recipient,
                    message.sender)
            self.enqueue_message(response)
        elif msg_type == "MessageAppendEntriesResponse *":
            print("CAST: polling message from {} to {}: ({}; success: {})".format(
                message.sender.id,
                message.recipient.id,
                ffi.getctype(ffi.typeof(message.handle)),
                message.handle.success))
            lib.raft_server_handle_append_entries_response(
                    message.recipient.raft,
                    message.sender.id,
                    message.handle)
        else:
            print("CAST: unexpected message type: {}".format(msg_type))
            raise Exception


    def poll_messages(self):
        messages = self.messages
        self.messages = []

        for message in messages:
            self.poll_message(message)


    def run(self):
        with self.stack as stack:
            #for s in self.servers:
            #    print("CAST: server {s} voted for: {p}".format(s=s.id, p=s.voted_for()))
            #    assert s.voted_for() is None

            for i in range(0, NUM_PERIODS):
                print("CAST: --------------- running period {} ---------------------".format(self.period))
                for s in self.servers:
                    s.periodic(MS_PER_PERIOD)
                self.period += 1
                self.poll_messages()

            #for s in self.servers:
            #    print("CAST: server {s} voted for: {p}".format(s=s.id, p=s.voted_for()))
            #    assert s.voted_for() == s.id

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


class Message(object):
    def __init__(self, handle, sender, recipient):
        self.handle = handle
        self.sender = sender
        self.recipient = recipient

    def from_raw(message, sender, recipient):
        msg_type = ffi.getctype(ffi.typeof(message))
        msg_size = ffi.sizeof(message[0])
        msg_handle = ffi.cast(ffi.typeof(message), lib.malloc(msg_size))
        ffi.memmove(msg_handle, message, msg_size)
        return Message(msg_handle, sender, recipient)


def send_append_entries(raft, udata, peer_id, message):
    server = ffi.from_handle(udata)
    peer_node_info = server.get_node_info_by_id(peer_id)
    peer_udata = peer_node_info[0].user_data
    peer = ffi.from_handle(peer_udata)

    message = Message.from_raw(message, server, peer)
    server.simulator.enqueue_message(message)


def send_request_vote(raft, udata, peer_id, message):
    server = ffi.from_handle(udata)
    peer_node_info = server.get_node_info_by_id(peer_id)
    peer_udata = peer_node_info[0].user_data
    peer = ffi.from_handle(peer_udata)

    message = Message.from_raw(message, server, peer)
    server.simulator.enqueue_message(message)

    #print("CAST: callback 'send_request_vote' issued to {p} from {s}".format(p=peer.id,s=server.id))

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
    def __init__(self, id, simulator):
        # FIXME: remove this
        #servers = ArrayUintPtr(servers)
        #self.raft = lib.raft_server_new(id, servers.ptr(), servers.len())
        self.raft = lib.raft_server_new(id)
        self.id = id
        self.simulator = simulator

        user_data = ffi.new_handle(self)
        self._user_data = user_data

        self.load_callbacks()
        callbacks = ffi.new("Callbacks *")
        callbacks.send_request_vote = self.send_request_vote
        callbacks.send_append_entries = self.send_append_entries
        lib.raft_server_register_callbacks(self.raft, callbacks, user_data)
        self._callbacks = callbacks

        #self.message_queue = MessageQueue()
        #self.inbox = MessageQueue()
        #self.outbox = MessageQueue()


    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        lib.raft_server_free(self.raft)
        self.raft = None
        # TODO: is releasing these resources necessary?
        self._user_data = None
        self._callbacks = None


    def load_callbacks(self):
        self.send_append_entries = ffi.callback("void (RaftServer*, const void *, Id, MessageAppendEntriesRaw*)", send_append_entries)
        self.send_request_vote = ffi.callback("void (RaftServer*, const void *, Id, MessageRequestVote*)", send_request_vote)


    def periodic(self, ms_since_last_period):
        lib.raft_server_periodic(self.raft, ms_since_last_period)


    def voted_for(self):
        voted_for_p = ffi.new("Id *")
        res = lib.raft_server_voted_for(self.raft, voted_for_p)
        return verify_option(voted_for_p, res)


    def get_node_info_by_id(self, id):
        node_info_p = ffi.new("NodeInfo**")
        res = lib.raft_server_get_node_info_by_id(self.raft, id, node_info_p)
        return verify_option(node_info_p, res)


    def add_peer(self, server):
        print("CAST: adding peer {} to server {}".format(server.id, self.id))
        lib.raft_server_add_peer(self.raft, server.id, server._user_data)
    #def receive_msg(self, peer, msg):
    #    msg_type = ffi.getctype(ffi.typeof(msg))

    #    print(msg_type)
    #    if msg_type == "MessageRequestVote *":
    #        #peer = ffi.new("Id *", peer)
    #        #res = lib.raft_server_receive(self.raft, peer, msg)
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
#        print("CAST: Error while reading last error message")
#        sys.exit(1)
#
#    error_msg_buf = ffi.buffer(array)
#    message = bytes(error_msg_buf).decode("utf-8")
#    return message

def main():
    server_ids = list(range(0, NUM_SERVERS))
    simulator = Simulator()
    for id in server_ids:
        simulator.add_server(id, simulator)
    simulator.run()


if __name__ == "__main__":
    main()
