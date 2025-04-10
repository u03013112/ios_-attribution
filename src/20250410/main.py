# 视频获取
# Google 
# US
# 视频
# 上传时间：2025-01-01~2025-02-28
# 消耗时间：2025-03-01~2025-03-31
# 消耗金额前10名
import pandas as pd
import os
import cv2

videoInfoDf = pd.read_csv('video.csv')

# 定义颜色范围
color_ranges = {
    '赤': [(0, 50, 50), (15, 255, 255)],
    '赤2':[(170, 50, 50), (180, 255, 255)],
    '橙': [(16, 50, 50), (25, 255, 255)],
    '黄': [(26, 50, 50), (35, 255, 255)],
    '绿': [(36, 50, 50), (95, 255, 255)],    # 扩展至青绿色
    '蓝': [(96, 50, 50), (135, 255, 255)],   # 覆盖蓝青色过渡区
    '紫': [(136, 50, 50), (169, 255, 255)],  # 包含蓝紫色到品红色
    '黑': [(0, 0, 0), (180, 255, 50)],
    '白': [(0, 0, 200), (180, 30, 255)],
    '灰': [(0, 0, 51), (180, 30, 199)]       # 新增灰度过渡带
}

# 为视频文件找到颜色标签3s版本
def findColorTag3s(filename):
    # 读取视频文件，每一秒一张图
    cap = cv2.VideoCapture(filename)
    frames = []
    for i in range(3):
        ret, frame = cap.read()
        if ret:
            frames.append(frame)
    cap.release()

    # 然后将这3张图进行颜色分类
    color_tags = list(color_ranges.keys())
    color_ratios = {color: 0 for color in color_tags}

    for frame in frames:
        # 将图像转换为HSV
        hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)

        # 计算每种颜色的像素占比
        for color, (lower, upper) in color_ranges.items():
            mask = cv2.inRange(hsv, lower, upper)
            ratio = cv2.countNonZero(mask) / (frame.shape[0] * frame.shape[1])
            color_ratios[color] += ratio / 3

    # 计算其他颜色的占比
    other_ratio = 1 - sum(color_ratios.values())

    # 输出dataframe，列名为：filename, color, ratio
    result_df = pd.DataFrame([
        {'filename': filename, 'color': color, 'ratio': ratio}
        for color, ratio in color_ratios.items()
    ] + [{'filename': filename, 'color': 'other', 'ratio': other_ratio}])
    
    return result_df


def checkColor(color_ranges):
    import numpy as np
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 8))
    for i, (color, (lower, upper)) in enumerate(color_ranges.items()):
        gradient = np.zeros((50, 256, 3), dtype=np.uint8)
        for j in range(3):
            gradient[:, :, j] = np.linspace(lower[j], upper[j], 256, dtype=np.uint8)
        ax.imshow(cv2.cvtColor(gradient, cv2.COLOR_HSV2RGB), aspect='auto', extent=[0, 256, i, i+1])
        # ax.text(128, i+0.5, color, va='center', ha='center', fontsize=12, color='black')
    ax.set_xlim(0, 256)
    ax.set_ylim(0, len(color_ranges))
    ax.axis('off')
    plt.show()

def test():
    filename = '1.mp4'
    result_df = findColorTag3s(filename)
    print(result_df)

if __name__ == "__main__":
    test()

    checkColor(color_ranges)
