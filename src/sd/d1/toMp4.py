import cv2
import os

# 图像文件名列表
image_files = [f"{i}.png" for i in range(7)]

# 读取第一张图像以获取帧的尺寸
frame = cv2.imread(image_files[0])
height, width, layers = frame.shape

# 定义视频编解码器和创建VideoWriter对象
fourcc = cv2.VideoWriter_fourcc(*'mp4v')  # 使用mp4v编解码器
video = cv2.VideoWriter('run.mp4', fourcc, 12, (width, height))

# 读取图像并写入视频
for image_file in image_files:
    frame = cv2.imread(image_file)
    video.write(frame)

# 释放VideoWriter对象
video.release()

print("视频创建完成。")
