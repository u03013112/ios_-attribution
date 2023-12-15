import rpyc

conn = rpyc.connect("192.168.40.62", 10001)
x = conn.root.sendMessageDebug("Hello, World!")