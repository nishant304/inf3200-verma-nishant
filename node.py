#!/usr/bin/python
# vim: set sts=4 sw=4 et:

import BaseHTTPServer
import time
import threading
import signal
import sys
import os
import getopt
import md5
import socket
from itertools import cycle
import httplib

MAX_CONTENT_LENGHT = 1024		# Maximum length of the content of the http request (1 kilobyte)
MAX_STORAGE_SIZE = 104857600	# Maximum total storage allowed (100 megabytes)
map = dict()
storageBackendNodes = list()

class SlaveHttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    global node
    # sys.argv[0] --> num_hosts
    # sys.argv[1] --> rank
    # sys.argv[2] --> next_node

    # Returns the
    def do_GET(self):
        key = self.path

        # TODO: distributed stores
        #   Divide key space --> hashed_node = md5(key) % number_of_nodes
        #   If this node is responsible for key, give response.
        #   If not, query next node.

        if(node_index  == hash(key)%number_of_nodes):         

            value = map.get(key)

        # if key not found, forward the request to the next node

        # Write header

            print "nishant get request processed and map size",len(map)                     
            self.send_response(200)
            self.send_header("Content-type", "application/octet-stream")
            self.end_headers()

        # Write Body
            self.wfile.write(value)    
        else : 
            print "trying in neighbour now"                     
            conn = httplib.HTTPConnection(neighbour, 7000)
            conn.request("GET", key)
            response = conn.getresponse()
	    if response.status == 200:	
                 self.send_response(200)
                 self.send_header("Content-type", "application/octet-stream")
                 self.end_headers()

        # Write Body
                 self.wfile.write(response.read())    
                      

    def do_PUT(self):
        contentLength = int(self.headers['Content-Length'])
        key = self.path

        if contentLength <= 0 or contentLength > MAX_CONTENT_LENGHT:
            self.sendErrorResponse(400, "Content body to large")
            return
        print "nishant before put request  the hash",hash(key)%number_of_nodes
        print "nishant before put request the node index ",node_index

        if(node_index  == hash(key)%number_of_nodes):

        # TODO: distributed stores
        #   Divide key space.
        #   If this node is responsible for key, give response.
        #   If not, query next node.
            print "nishant put request successful"                     
 
            map[self.path]=self.rfile.read(contentLength)   
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
        
        else : 
            print "nishant forwarding to neighbour",neighbour                   
            conn = httplib.HTTPConnection(neighbour, 7000)
   	    conn.request("PUT", key, self.rfile.read(contentLength))
         

    def sendErrorResponse(self, code, msg):
        self.send_response(code)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(msg)

class SlaveServer(BaseHTTPServer.HTTPServer):

    def server_bind(self):
        BaseHTTPServer.HTTPServer.server_bind(self)
        self.socket.settimeout(1)
        self.run = True

    def get_request(self):
        while self.run == True:
            try:
                sock, addr = self.socket.accept()
                sock.settimeout(None)
                return (sock, addr)
            except socket.timeout:
                if not self.run:
                    raise socket.error

    def stop(self):
        self.run = False

    def serve(self):
        while self.run == True:
            self.handle_request()


if __name__ == '__main__':

    httpserver_port = 7000
    name = socket.gethostname() 
    finalName = name[:11]   
    print "found host name",finalName
    
    global neighbour
    global number_of_nodes
    global node_index 
 
    number_of_nodes =   len(sys.argv)-1

    for node in sys.argv:
         index =   sys.argv.index(node) 
         if(index!=0):
		      print "Added", node, "to the list of nodes",index
                      if(node==finalName):
                          print "found our node in list of nodes"  
                          node_index = index-1 
                        
                          if(index==len(sys.argv)-1): 
                               neighbour = sys.argv[1] 
                               print  "neightbour is",neighbour 
                          else :
                               neighbour = sys.argv[index+1]
                               print  "neightbour is",neighbour  



			
			

    # Start the webserver which handles incomming requests
    # TODO: accept parameters from command line:
    #   total number of nodes, rank of this node, name of next node
    try:
        print "Starting HTTP server on port %d" % httpserver_port
        print "nishant host name",(socket.gethostname())              
  
        httpd = SlaveServer(("",httpserver_port), SlaveHttpHandler)
        server_thread = threading.Thread(target = httpd.serve)
        server_thread.daemon = True
        server_thread.start()

        def handler(signum, frame):
            print "Stopping http server..."
            httpd.stop()
        signal.signal(signal.SIGINT, handler)

    except Exception as e:
        print "Error: unable to start http server thread"
        raise e

    # Wait for server thread to exit
    server_thread.join(100)
			
			
