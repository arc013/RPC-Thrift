#
# Autogenerated by Thrift Compiler (0.10.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TFrozenDict, TException, TApplicationException
from thrift.protocol.TProtocol import TProtocolException
import sys
import shared.ttypes

from thrift.transport import TTransport


class hashBlock(object):
    """
    Attributes:
     - hash
     - block
     - status
    """

    thrift_spec = (
        None,  # 0
        (1, TType.STRING, 'hash', 'UTF8', None, ),  # 1
        (2, TType.STRING, 'block', 'BINARY', None, ),  # 2
        (3, TType.STRING, 'status', 'UTF8', None, ),  # 3
    )

    def __init__(self, hash=None, block=None, status=None,):
        self.hash = hash
        self.block = block
        self.status = status

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, (self.__class__, self.thrift_spec))
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.hash = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.block = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.STRING:
                    self.status = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, (self.__class__, self.thrift_spec)))
            return
        oprot.writeStructBegin('hashBlock')
        if self.hash is not None:
            oprot.writeFieldBegin('hash', TType.STRING, 1)
            oprot.writeString(self.hash.encode('utf-8') if sys.version_info[0] == 2 else self.hash)
            oprot.writeFieldEnd()
        if self.block is not None:
            oprot.writeFieldBegin('block', TType.STRING, 2)
            oprot.writeBinary(self.block)
            oprot.writeFieldEnd()
        if self.status is not None:
            oprot.writeFieldBegin('status', TType.STRING, 3)
            oprot.writeString(self.status.encode('utf-8') if sys.version_info[0] == 2 else self.status)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class hashBlocks(object):
    """
    Attributes:
     - blocks
