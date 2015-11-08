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
from collections import namedtuple
import Queue

MAX_CONTENT_LENGHT = 1024    	# Maximum length of the content of the http request (1 kilobyte)
MAX_STORAGE_SIZE = 104857600	# Maximum total storage allowed (100 megabytes)
map = dict()
storageBackendNodes = list()
next


class SlaveHttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    global node
    
    def do_GET(self):
        key = self.path
        if(self.path =="/getNodes"):  
            print "get request and path is",next           
            self.send_response(200)
            self.end_headers()
            self.wfile.write(next)  
        elif(self.path =="/getCurrentLeader"):  
            print "get request and leader is",leader           
            self.send_response(200)
            self.end_headers()
            self.wfile.write(leader)  


                      

    def do_PUT(self):
        contentLength = int(self.headers['Content-Length'])
        global next
        global previous  
        global leader

        if contentLength <= 0 or contentLength > MAX_CONTENT_LENGHT:
            self.sendErrorResponse(400, "Content body to large")
            return
        

        if(self.path=="/join"):

            print "nishant join request successful",next                     
            response_body = next          
            next=self.rfile.read(contentLength)
            print"join request is ",next
            print" corresponding join response is ", response_body    
            self.send_response(200)
            self.end_headers()
            self.wfile.write(response_body)
        
        elif(self.path=="/updatePrevious") : 
            previous=self.rfile.read(contentLength)
            print"update request is ",previous
            self.send_response(200)
            self.end_headers()                   
            self.wfile.write(previous)
        elif(self.path =="/election"):  
            contenders=self.rfile.read(contentLength)
            print "election ",contenders
            list = contenders.split(":")
            if(str(os.getpid()) in list) :
                leader = node_ip;
                leader += ":"   
                leader += str(httpserver_port)
                #announcResult();
                self.wfile.write(leader)
                print"leader is this node" 
            else :
                startElection(contenders)                    
                self.wfile.write("fail")
            self.send_response(200)
            self.end_headers()
        elif(self.path =="/result"):  
            leader_response=self.rfile.read(contentLength)
            print "election result here  ",leader_response
            if(leader != ""):
                leader = leader_response
                announcResult(leader);
            print "election result  ",leader
            self.send_response(200)
            self.end_headers()


         

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

def requestServer(msg):
        global previous 
        ip ,port= msg.split(":")   
        conn = httplib.HTTPConnection(ip, int(port))
        conn.request("PUT", "/updatePrevious",previous)
        response = conn.getresponse()            
	if response.status == 200:	
            prev = join_ip 
            prev += ":"
            prev = str(join_port)
            previous= prev      
            print"previous node is",previous

        conn.close()   
        startElection("")

def startElection(req):
        ip ,port= next.split(":")   
        conn = httplib.HTTPConnection(ip, int(port))
        req += ":" 
        req += str(os.getpid())
        conn.request("PUT", "/election",req)
        response = conn.getresponse()            
        reply = response.read()    
        print"election response",reply

	if response.status == 200:	
            print"election started"
            if( not "fail" in str(reply)):
                print"use message queue",reply                
                queue_wrapper.enque(str(reply))   

        conn.close()   
        
def announcResult(message):
        ip ,port= next.split(":")   
        conn = httplib.HTTPConnection(ip, int(port))
        conn.request("PUT", "/result",message)
        response = conn.getresponse()            
	if response.status == 200:	
            print"election result"
        conn.close()   


class QueueWrapper : 
    def __init__(self): 
        self.queue = Queue.Queue()

    def start(self): 
        def messenger(): 
            while True : 
                msg= self.queue.get()
                print"message from queue is",msg 
                announcResult(msg)                    
                self.queue.task_done()

        server_thread = threading.Thread(target = messenger)
        server_thread.daemon = True
        server_thread.start()

    def enque(self,msg):
        self.queue.put(msg)          
        
                                  


           


if __name__ == '__main__':

    global httpserver_port
    global node_ip
    name = socket.gethostname() 
    node_ip = name[:11]   
    httpserver_port =8030
    global neighbour
    global current
    global number_of_nodes
    global node_index 
    global join_ip
    global join_port
    Node = namedtuple("Node",["ip","port"])
    join_ip =""
    join_port =0    
    global next
    global previous

    global leader
    leader= ""
 


 
    number_of_nodes =   len(sys.argv)-1
    #node_ip = node_port = None
     

    try:
        optlist, args = getopt.getopt(sys.argv[1:], 'x', ['port=', 'ip=','joinip=','joinport='])
    except getopt.GetoptError:
        print sys.argv[0] + ' [--ip ipaddress] [--port portnumber]'
        sys.exit(2)
    
       
    for opt, arg in optlist:
        print" arg is",arg 
        if opt in ("-ip", "--ip"):
            node_ip = arg
        elif opt in ("-port", "--port"):
            httpserver_port = int(arg)
        elif opt in ("-joinip", "--joinip"):
            join_ip = str(arg)
        elif opt in ("-joinport", "--joinport"):
            join_port = int(arg)



    next= node_ip
    next += ":"
    next += str(httpserver_port)
    previous =next  
    queue_wrapper = QueueWrapper()           	 
    queue_wrapper.start() 
    print "node ip",node_ip 
    print "node port",httpserver_port
    print "join ip",join_ip
    print "join port",join_port
                   					
    try:
  
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
    print"join port is",join_port
    if(join_port !=0 and  join_ip!=node_ip):
        conn = httplib.HTTPConnection(join_ip, join_port)
        conn.request("PUT", "/join",next)
        response = conn.getresponse()
	if response.status == 200:	
            next= response.read()
            print"join response" , next

        conn.close()   
        startElection("")

    

    server_thread.join(100)

  

