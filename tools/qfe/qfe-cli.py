#!/usr/bin/python
'''
Created on May 1, 2013

@author: rtw
'''

import struct
import binascii
import socket
import sys

QFE_HOST = 'localhost'
#QFE_HOST = 'winsvr2008r2.calpont.com'
QFE_SOCK = 9198

def do_query(query):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((QFE_HOST, QFE_SOCK))

    defschem = "tpch1";
    lenstruct = struct.pack("I", len(defschem))
    client_socket.send(lenstruct)
    client_socket.send(defschem)
    
    lenstruct = struct.pack("I", len(query))
    client_socket.send(lenstruct)
    client_socket.send(query)
    
    rowlen = 1
    while rowlen:
        rowlen = client_socket.recv(4)
        if len(rowlen) < 4:
            break
        rowval = struct.unpack("I",rowlen)[0]
	if rowval == 0:
		lenstruct = struct.pack("I", 0);
		client_socket.send(lenstruct)
		break
        row = client_socket.recv(rowval)
        print row

    client_socket.close()
        
if __name__ == '__main__':
    if len(sys.argv) < 2:
        query = raw_input ( "QFE-cli > " )
    else:
        query = sys.argv[1]
        
    sys.exit(do_query(query))
