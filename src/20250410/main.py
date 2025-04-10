# 视频获取
# Google 
# US
# 视频
# 上传时间：2025-01-01~2025-02-28
# 消耗时间：2025-03-01~2025-03-31
# 消耗金额前10名
import pandas as pd
import os

videoInfoDf = pd.read_csv('video.csv')

# 为视频文件找到颜色标签3s版本
def findColorTag3s(filename):
    # 读取视频文件，每一秒一张图
    # 前3秒即3张图
    # 然后将这3张图进行颜色分类
    # 将rgb归类为：赤、橙、黄、绿、蓝、紫、黑、白 这8种颜色
    # 并将颜色像素进行占比统计
    # 输出dataframe，列名为：filename, color, ratio