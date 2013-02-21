# server.py
# a simple json-based rpc server

import SocketServer
import json
import struct
import time
import logging

logging.basicConfig(level=logging.INFO)

METHODS = {}

def rpc(name) :
    def _rpc(f) :
        METHODS[name] = f
        return f
    return _rpc

class RPCHandler(SocketServer.StreamRequestHandler) :
    def handle(self):
        self.request.settimeout(5)
        ident = None
        action = None
        params = None
        try :
            message = self.read_json()
            logging.info("Got message %r" % message)
            ident = message.get("id", None)
            action = message.get("action", None)
            params = message.get("params", None)

            result = METHODS[action](**params)
            self.write_result(ident, result)
        except Exception as x :
            logging.error("Exception %r" % x)
            self.write_exception(ident, x)
    def read_json(self) :
        size = struct.unpack("<I", self.request.recv(4))
        data = self.request.recv(size[0])
        return json.loads(data)
    def write_json(self, o) :
        ostring = json.dumps(o)
        self.request.sendall(struct.pack("<I", len(ostring)))
        self.request.sendall(ostring)
    def write_result(self, ident, result) :
        msg = {"id" : ident,
               "result" : result}
        self.write_json(msg)
    def write_exception(self, ident, exception) :
        msg = {"id" : ident,
               "error" : {"type" : exception.__class__.__name__,
                          "args" : exception.args }}
        self.write_json(msg)

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer) :
    allow_reuse_address = True
    pass

@rpc("hello")
def rpc_hello(name) :
    return "Hello, " + name
@rpc("failure")
def rpc_failure() :
    return 1/0

if __name__ == "__main__" :
    HOST, PORT = "localhost", 22322
    print "Serving at %s:%s" % (HOST, PORT)
    server = ThreadedTCPServer((HOST, PORT), RPCHandler)
    try :
        server.serve_forever()
    finally :
        server.shutdown()
