from socket import *     #补
from threading import Thread    #补
import sys                             #补
def udpsend():
    sendaddr = ("192.168.40.62",8877)  # 确定发送ip地址和端口
    while True:
        senddata = input('<<')  # 确定要发送的信息
        udpsocket.sendto(senddata.encode("gb2312"), sendaddr)  # 发送信息,这里用Gb2312编码是因为网络调试助手是用GB2312编码的。通常用utf-8编码。
def udprecv():
    while True:
        result = udpsocket.recvfrom(1024)
        sys.stdout.write('\r%s%s\n' % (">>", result[0].decode("gb2312")))
        sys.stdout.flush()
        sys.stdout.write("\r%s" % ('<<'))
        sys.stdout.flush()
udpsocket = None                 #补
def main():
    global udpsocket           #补，因为接受和发送都需要用到套接字，所以要全局化
    udpsocket = socket(AF_INET, SOCK_DGRAM)  # 创建套接字，现在用的是udp，还有一种tcp
    udpsocket.bind(("", 5566))     #补，绑定端口，接收消息
    thread_send = Thread(target=udpsend)
    thread_recv = Thread(target=udprecv)
    thread_send.start()
    thread_recv.start()
    thread_send.join()
    thread_recv.join()
if __name__ == '__main__':
    main()