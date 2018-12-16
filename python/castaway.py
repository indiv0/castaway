#import sys
from contextlib import ExitStack
from castaway_cffi import ffi, lib
import random
import datetime

NUM_ELEMENTS = 10000
NUM_SERVERS = 5
NUM_PERIODS = 100000
MS_PER_PERIOD = 10

class ReplicationDone(Exception): pass


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
        #print("CAST: Initialized server: {}".format(id))


    def enqueue_message(self, message):
        self.messages.append(message)
        #print("CAST: enqueued ({}) message from {} to {}".format(
        #    ffi.getctype(ffi.typeof(message.handle)),
        #    message.sender.id,
        #    message.recipient.id))


    def poll_message(self, message):
        msg_type = ffi.getctype(ffi.typeof(message.handle))

        if msg_type == "MessageRequestVote *":
            #print("CAST: polling message from {} to {}: ({})".format(
            #    message.sender.id,
            #    message.recipient.id,
            #    ffi.getctype(ffi.typeof(message.handle))))
            rvr = lib.raft_server_handle_request_vote_request(
                    message.recipient.raft,
                    message.sender.id,
                    message.handle)
            response = Message.from_raw(ffi.addressof(rvr), message.recipient, message.sender)
            self.enqueue_message(response)
        elif msg_type == "MessageRequestVoteResponse *":
            #print("CAST: polling message from {} to {}: ({})".format(
            #    message.sender.id,
            #    message.recipient.id,
            #    ffi.getctype(ffi.typeof(message.handle))))
            if message.handle.vote_granted:
                pass
                #print("CAST: server {} received vote from peer {}".format(
                #    message.recipient.id,
                #    message.sender.id))
            lib.raft_server_handle_request_vote_response(
                    message.recipient.raft,
                    message.sender.id,
                    message.handle)
            # FIXME
            #print("CAST: message: (term: {}, vote_granted: {})".format(
            #    message.handle.term,
            #    message.handle.vote_granted))
        elif msg_type == "MessageAppendEntriesRaw *":
            #print("CAST: polling message from {} to {}: ({})".format(
            #    message.sender.id,
            #    message.recipient.id,
            #    ffi.getctype(ffi.typeof(message.handle))))
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
            #print("CAST: polling message from {} to {}: ({}; success: {})".format(
            #    message.sender.id,
            #    message.recipient.id,
            #    ffi.getctype(ffi.typeof(message.handle)),
            #    message.handle.success))
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
            for i in range(0, NUM_PERIODS):
                #print("CAST: --------------- running period {} ---------------------".format(self.period))
                for s in self.servers:
                    s.periodic(MS_PER_PERIOD)
                self.period += 1
                self.poll_messages()

            start_time = datetime.datetime.now()
            # Add random entries to the log
            for server in self.servers:
                if server.is_leader():
                    for i in range(0, NUM_ELEMENTS):
                        self.servers[0].client_request(i)

            for i in range(0, NUM_PERIODS):
                #print("CAST: --------------- running period {} ---------------------".format(self.period))
                all_servers_replicated = True
                for s in self.servers:
                    s.periodic(MS_PER_PERIOD)
                    if s.current_index() != NUM_ELEMENTS:
                        all_servers_replicated = False
                self.period += 1
                self.poll_messages()
                if all_servers_replicated:
                    break
            end_time = datetime.datetime.now()

            for s in self.servers:
                entries = [(e.command, e.term) for e in server.get_log()]
                print("server {} entries: {}".format(s.id, entries))

            dt = end_time - start_time
            print("elapsed time: {}".format(dt.total_seconds()))


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
        #print("CAST: adding peer {} to server {}".format(server.id, self.id))
        lib.raft_server_add_peer(self.raft, server.id, server._user_data)


    def is_leader(self):
        return lib.raft_server_is_leader(self.raft)


    def client_request(self, command):
        if not self.is_leader():
            return None

        #print("CAST: client request with command: {}".format(command))
        index_p = ffi.new("uintptr_t**")
        term_p = ffi.new("Term**")
        lib.raft_server_client_request(self.raft, command, index_p, term_p)
        index = index_p[0][0]
        term = term_p[0][0]
        #print("CAST: client request successful: ({}, {})".format(index, term))
        return (index, term)


    def current_index(self):
        return lib.raft_server_current_index(self.raft)


    def get_log(self):
        log_p = ffi.new("LogEntry**")
        len_p = ffi.new("size_t*")
        lib.raft_server_get_log(self.raft, log_p, len_p)
        entries = []
        for i in range(0, len_p[0]):
            entry = LogEntry(log_p[0][i].command, log_p[0][i].term)
            entries.append(entry)
        return entries


class LogEntry(object):
    def __init__(self, command, term):
        self.command = command
        self.term = term


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
