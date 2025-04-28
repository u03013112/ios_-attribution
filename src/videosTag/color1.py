# 简单颜色标签1

import os
import cv2
import pandas as pd
import numpy as np

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

# 为视频帧找到颜色标签
def findColorTagfromFrame(frame_dir,filename):
    # 遍历文件夹中的所有图片
    frames = []
    sorted_filenames = sorted(os.listdir(frame_dir))
    for frame_filename in sorted_filenames:
        if frame_filename.endswith('.jpg'):
            # print('frame_filename:', frame_filename)
            frame_path = os.path.join(frame_dir, frame_filename)
            frame = cv2.imread(frame_path)
            frames.append(frame)

    color_tags = list(color_ranges.keys())
    color_ratios = {color: 0 for color in color_tags}

    for frame in frames:
        # 将图像转换为HSV
        hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)

        # 计算每种颜色的像素占比
        for color, (lower, upper) in color_ranges.items():
            mask = cv2.inRange(hsv, lower, upper)
            ratio = cv2.countNonZero(mask) / (frame.shape[0] * frame.shape[1])
            color_ratios[color] += ratio / len(frames)

    # 计算其他颜色的占比
    other_ratio = 1 - sum(color_ratios.values())

    # 输出dataframe，列名为：filename, color, ratio
    result_df = pd.DataFrame([
        {'filename': filename, 'color': color, 'ratio': ratio}
        for color, ratio in color_ratios.items()
    ] + [{'filename': filename, 'color': 'other', 'ratio': other_ratio}])
    
    return result_df

def addTag(videoInfoDf):
    colorTagDf = pd.DataFrame()
    for i in range(len(videoInfoDf)):
        filename = videoInfoDf.loc[i, 'filename']
        frames_dir = videoInfoDf.loc[i, 'frames_dir']

        # colorTagDf0 = findColorTag3s(filename)
        colorTagDf0 = findColorTagfromFrame(frames_dir, filename)
        # print('colorTagDf0:')
        # print(colorTagDf0)
        # 将colorTagDf0 pivot成列,filename,'赤ratio','赤2ratio' 等
        colorTagDf0 = colorTagDf0.pivot(index='filename', columns='color', values='ratio').reset_index()
        colorTagDf = pd.concat([colorTagDf, colorTagDf0], ignore_index=True)

    videoInfoDf = pd.merge(videoInfoDf, colorTagDf, on='filename', how='left')

    return videoInfoDf

from sklearn.tree import DecisionTreeRegressor, plot_tree
import matplotlib.pyplot as plt
def visualize_tree(model, feature_names):
    plt.figure(figsize=(40, 40))
    plt.rcParams['font.sans-serif'] = ['Hiragino Sans GB'] 
    plot_tree(model, feature_names=feature_names, filled=True, rounded=True)
    plt.savefig("decision_tree.png")  # 保存为 decision_tree.png

from sklearn.metrics import precision_score, recall_score, r2_score
from sklearn.tree import DecisionTreeClassifier
def fit_predict_cost_with_decision_tree2():
    # 读取数据
    data = pd.read_csv('videoWithColorTag.csv')
    
    # 提取颜色比例特征
    color_features = data.columns[5:-1]  # 颜色比例特征列名
    print('color_features:')
    print(color_features)

    X = data[color_features]  # 输入特征

    y = np.zeros(data.shape[0])
    y[data['cost'] > 1000000] = 1
    
    # 初始化决策树分类模型
    model = DecisionTreeClassifier(
        max_depth=2,
        # max_leaf_nodes=20  # 限制叶子节点的最大数量
    )
    
    # 拟合模型
    model.fit(X, y)
    
    # 预测畅销素材
    data['predicted_class'] = model.predict(X)
    
    # 计算查准率和查全率
    precision = precision_score(y, data['predicted_class'])
    recall = recall_score(y, data['predicted_class'])
    
    # 计算 R2
    r2 = r2_score(y, data['predicted_class'])
    
    # print(f"Precision: {precision}")
    # print(f"Recall: {recall}")
    # print(f"R2: {r2}")

    visualize_tree(model, color_features)
    
    return model, data, precision, recall, r2
