# 视频截图

import cv2

def video2image(video_path, image_path,time_interval=1.5):
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)  # 获取视频的帧率
    frame_skip = int(fps * time_interval)
    count = 0
    frame_count = 0
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        if frame_count % frame_skip == 0:  # 每1.5秒保存一张图片
            cv2.imwrite(image_path + '/%d.jpg' % count, frame)
            count += 1
        frame_count += 1
    cap.release()

if __name__ == '__main__':
    video2image('/Users/u03013112/Downloads/LW_20240507_CFC0570EN-CFC原版_TT_TT_1080X1350_EN_无水印.mp4', 'images', 1.5)
