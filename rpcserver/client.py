import socket
import struct
import json
import time

class RPCException(Exception) :
    pass

class RPCClient(object) :
    def __init__(self, ip, port) :
        self.__data__ = (ip, port)
    def __send_request__(self, object) :
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ip, port = self.__data__
        sock.settimeout(222)
        try :
            sock.connect((ip, port))
            ostring = json.dumps(object)
            sock.sendall(struct.pack("<I", len(ostring)))
            sock.sendall(ostring)
            
            size = struct.unpack("<I", sock.recv(4))
            data = sock.recv(size[0])
            return json.loads(data)
        finally:
            sock.close()
    def __getattr__(self, name) :
        return RPCFunction(self, name)

class RPCFunction(object) :
    def __init__(self, client, funcname) :
        self.client = client
        self.funcname = funcname
    def __call__(self, **kwargs) :
        msg = {"action" : self.funcname,
               "params" : kwargs}
        res = self.client.__send_request__(msg)
        if "result" in res :
            return res["result"]
        elif "error" in res :
            error = res["error"]
            raise RPCException(error["type"], error["args"])
        else :
            raise RPCException("Malformed result")

client = RPCClient("127.0.0.1", 22322)
print client.hello(name="Kyle")
print client.failure()
