# 简单的低时延摄像头流测试

from flask import Flask, Response
import cv2
import time
from threading import Thread
import queue

app = Flask(__name__)

class LowLatencyCamera:
    def __init__(self):
        self.camera = cv2.VideoCapture(0)
        # 关键设置：减少缓冲
        self.camera.set(cv2.CAP_PROP_BUFFERSIZE, 1)  # 最小缓冲
        self.camera.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
        self.camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
        self.camera.set(cv2.CAP_PROP_FPS, 15)  # 降低帧率减少延时
        
        # 用队列保存最新帧
        self.frame_queue = queue.Queue(maxsize=2)  # 只保留最新的2帧
        self.running = True
        
        # 启动读取线程
        self.thread = Thread(target=self.update_frame)
        self.thread.daemon = True
        self.thread.start()
        
    def update_frame(self):
        while self.running:
            ret, frame = self.camera.read()
            if ret:
                # 清空队列，只保留最新帧
                while not self.frame_queue.empty():
                    try:
                        self.frame_queue.get_nowait()
                    except queue.Empty:
                        break
                
                # 立即压缩编码（低质量，快速）
                ret, buffer = cv2.imencode('.jpg', frame, [
                    cv2.IMWRITE_JPEG_QUALITY, 30,  # 低质量
                    cv2.IMWRITE_JPEG_OPTIMIZE, 0,   # 不优化，快速编码
                ])
                
                if ret:
                    try:
                        self.frame_queue.put_nowait(buffer.tobytes())
                    except queue.Full:
                        pass
    
    def get_frame(self):
        try:
            return self.frame_queue.get_nowait()
        except queue.Empty:
            return None
    
    def __del__(self):
        self.running = False
        if hasattr(self, 'camera'):
            self.camera.release()

# 全局摄像头对象
camera = LowLatencyCamera()

def generate_frames():
    while True:
        frame = camera.get_frame()
        if frame is not None:
            yield (b'--frame\r\n'
                   b'Content-Type: image/jpeg\r\n\r\n' + frame + b'\r\n')
        else:
            time.sleep(0.01)  # 短暂等待

@app.route('/')
def index():
    return '''
    <html>
    <head>
        <title>低延时摄像头</title>
        <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
        <meta http-equiv="Pragma" content="no-cache">
        <meta http-equiv="Expires" content="0">
    </head>
    <body>
        <h2>低延时摄像头流</h2>
        <img src="/video_feed" width="640" height="480" style="image-rendering: -webkit-optimize-contrast;">
    </body>
    </html>
    '''

@app.route('/video_feed')
def video_feed():
    return Response(generate_frames(),
                    mimetype='multipart/x-mixed-replace; boundary=frame',
                    headers={'Cache-Control': 'no-cache, no-store, must-revalidate',
                            'Pragma': 'no-cache',
                            'Expires': '0'})

if __name__ == '__main__':
    print("低延时摄像头服务器启动")
    print("访问: http://树莓派IP:8080")
    app.run(host='0.0.0.0', port=8080, threaded=True, debug=False)