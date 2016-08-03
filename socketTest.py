import socket
import threading
import time
from random import uniform

#-74.2589, 40.4774, -73.7004, 40.9176
def tcplink(sock, addr):
    print 'Accept new connection from %s:%s...' % addr
    #sock.send('Welcome! \n')
    while True:
    	"""
        data = sock.recv(1024)
        time.sleep(1)
        if data == 'exit' or not data:
            break
        sock.send('Hello, %s!' % data)
        """
        """
        for i in range(200):
            sock.send("Hello%s \n" % str(i%10))
            time.sleep(0.1)
    	sock.close()
        """
        lat = uniform(-74.2589, -73.7004)
        lon = uniform(40.4774, 40.9176)
        userRequest = str(lat) + ',' + str(lon) + ',' + str(time.time()) + "\n"
        #print userRequest
        sock.send(userRequest)
        time.sleep(0.05)
    print 'Connection from %s:%s closed.' % addr

if __name__ == "__main__":
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind(('10.0.2.15', 9997))
	s.listen(5)
	print "Waiting for connection..."

	while True:
		sock, addr = s.accept()
		t = threading.Thread(target=tcplink, args=(sock, addr))
		t.start()