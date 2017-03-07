#!/usr/bin/env python

import sys
import glob
import os

sys.path.append('gen-py')

# Thrift specific imports
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from shared.ttypes import *
from metadataServer.ttypes import *
from blockServer.ttypes import *

# Add classes / functions as required here
def getBlockServerPort(config_path):
    # This function reads config file and gets the port for block server

    print "Checking validity of the config path"
    if not os.path.exists(config_path):
        print "ERROR: Config path is invalid"
        exit(1)
    if not os.path.isfile(config_path):
        print "ERROR: Config path is not a file"
        exit(1)

    print "Reading config file"
    with open(config_path, 'r') as conffile:
        lines = conffile.readlines()
        for line in lines:
            if 'block' in line:
                # Important to make port as an integer
                return int(line.split()[1].lstrip().rstrip())

    # Exit if you did not get blockserver information
    print "ERROR: blockserver information not found in config file"
    exit(1)


def getMetadataServerPort(config_path):
    # This function reads config file and gets the port for block server

    print "Checking validity of the config path"
    if not os.path.exists(config_path):
        print "ERROR: Config path is invalid"
        exit(1)
    if not os.path.isfile(config_path):
        print "ERROR: Config path is not a file"
        exit(1)

    print "Reading config file"
    with open(config_path, 'r') as conffile:
        lines = conffile.readlines()
        for line in lines:
            if 'metadata1' in line:
                # Important to make port as an integer
                return int(line.split()[1].lstrip().rstrip())

    # Exit if you did not get blockserver information
    print "ERROR: blockserver information not found in config file"
    exit(1)



def getBlockServerSocket(port):
    # This function creates a socket to block server and returns it

    # Make socket
    transport = TSocket.TSocket('localhost', port)
    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)
    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    # Create a client to use the protocol encoder
    client = BlockServerService.Client(protocol)

    # Connect!
    print "Connecting to block server on port", port
    try:
        transport.open()
    except Exception as e:
        print "ERROR: Exception while connecting to block server, check if server is running on port", port
        print e
        exit(1)

    return client


def getMetaServerSocket(port):
    # This function creates a socket to block server and returns it

    # Make socket
    transport = TSocket.TSocket('localhost', port)
    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)
    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    # Create a client to use the protocol encoder
    client = MetadataServerService.Client(protocol)

    # Connect!
    print "Connecting to block server on port", port
    try:
        transport.open()
    except Exception as e:
        print "ERROR: Exception while connecting to block server, check if server is running on port", port
        print e
        exit(1)

    return client


def scan_base_dir(base_dir):
    local_block_list = {}
    for file in files os.listdir(base_dir):
        f    = open (file, "rb")
        data = fread(4096)
        while data is not None:
            #hash
            m = hashlib.sha256()
            m.update(data)
            hashString = m.hexdigest()
            local_block_list[hashString] = data
            data = fread(4096)
    return local_block_list

def do_operations(sock, meta_sock, local_block_list, command, filename):
    # create a local dict to know which blocks are locally present
    if command == "download":
        write_file = open(filename, "rb")
        # data = fread(4096)
        f          = meta_sock.getFile(filename)
        if f.status == responseType.OK:
            print "Meta Server said OK, block list retrieve successful"
            for hashString in f.hashList:
                #m = hashlib.sha256()
                #m.update(data)
                #hashString = m.hexdigest()
                if hashString is not in local_block_list:
                    #getblock from socket
                    try:
                        hb = sock.getBlock(hashstring)]
                    except Exception as e:
                        print "Received exception while trying getBlock"
                        print e
                        exit(1)
                    if hb.status == "ERROR":
                        print "ERROR status while retrieving block, looks like block server does`nt have it"
                        return
                    else:
                        print "Block status OK"
                    m = hashlib.sha256()
                    m.update(hb.block)
                    hashString_dwnld = m.hexdigest()
                    if hashString == hashString_dwnld:
                        print "Blocks match"
                    else:
                        print "Blocks does not match"
                    write_file.write(hb.block)

                else:
                    write_file.write(local_block_list[hashstring])
           # data = fread(4096)

        else:
            print "Server said ERROR,  Meta server get list unsuccessful"

    elif command == "upload":





    elif command == "delete":
        f = meta_sock.getFile(filename)
        if f.status == responseType.OK:
            try:
                resp = deleteFile(f)
            except Exception as e:
                print "ERROR while calling deleteFile"
                print e
            if resp.message == responseType.OK:
                print "Deletion of block successful"
            else:
                print "Deletion of block not successful"

            print "Done"
        else:
            print "Server said ERROR,  Meta server get list unsuccessful"



    else:
        print "ERROR: not supported command"
    


if __name__ == "__main__":

    if len(sys.argv) < 5:
        print "Invocation : <executable> <config_file> <base_dir> <command> <filename>"
        exit(-1)
    config_path = sys.argv[1]
    base_dir    = sys.argv[2]
    command     = sys.argv[3]
    filename    = sys.argv[4]
    print "Configuration file path : ", config_path

    print "Starting client"
    print "Creating socket to Block Server"
    servPort  = getBlockServerPort(config_path)
    sock      = getBlockServerSocket(servPort)

    meta_port = getMetadataServerPort(config_path)
    meta_sock = getMetaServerSocket(meta_port)



    local_block_list = scan_base_dir(base_dir)
    # Time to do some operations!
    do_operations(sock, meta_sock, local_block_list, command, filename)




    print "Starting client"

    '''
    Server information can be parsed from the config file

    connections can be created as follows

    Eg:

    # Make socket
    transport = TSocket.TSocket('serverip', serverport)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = HelloService.Client(protocol)

    # Connect!
    try:
        transport.open()
    except Exception as e:
        print "Error while opening socket to server\n", e
        exit(1)

    # Create custom data structure object
    m = message()

    # Fill in data
    m.data = "Hello From Client!!!"

    # Call via RPC
    try:
        dataFromServer = client.HelloServiceMethod(m)
    except Exception as e:
        print "Caught an exception while calling RPC"
        # Add handling code
        exit(1)

    '''
