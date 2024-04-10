import cv2

def video2img(videoFileName, timeIntval=1.0):
    # 打开视频文件
    video = cv2.VideoCapture(videoFileName)

    # 检查是否成功打开
    if not video.isOpened():
        print(f"Error: Could not open video file {videoFileName}")
        return

    # 获取帧速率
    fps = int(video.get(cv2.CAP_PROP_FPS))

    # 计算每隔多少帧保存一张图片
    frame_interval = int(fps * timeIntval)

    # 初始化帧计数器
    frame_count = 0

    while True:
        # 读取一帧
        ret, frame = video.read()

        # 检查是否到达视频末尾
        if not ret:
            break

        # 如果当前帧计数器是间隔的倍数，保存图片
        if frame_count % frame_interval == 0:
            img_filename = f"images/pic_{frame_count // frame_interval * timeIntval}.png"
            cv2.imwrite(img_filename, frame)
            print(f"Saved frame as {img_filename}")

        # 更新帧计数器
        frame_count += 1

    # 释放视频资源
    video.release()

    print("Finished processing video")

# 示例用法
video2img("/Users/u03013112/Downloads/日韩AI测试素材/TH_20240227_KOL0009JP-KOL日语_JTT_LIVEDGE_1080X1350_JA_无水印.mp4", timeIntval=0.5)

