import cv2
import os

def video2image(video_path, image_path, time_interval=1.5):
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

def process_videos(video_dir='videos', image_dir='images', time_interval=1.5):
    # 获取视频目录下的所有mp4文件
    video_files = [f for f in os.listdir(video_dir) if f.endswith('.mp4')]
    for video_file in video_files:
        video_path = os.path.join(video_dir, video_file)
        # 为每个视频在图片目录下创建一个同名文件夹
        image_path = os.path.join(image_dir, video_file[:-4])  # 去掉.mp4后缀
        if not os.path.exists(image_path):
            os.makedirs(image_path)
        # 将视频拆分为图片
        video2image(video_path, image_path, time_interval)

if __name__ == '__main__':
    process_videos()
