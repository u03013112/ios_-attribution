import pandas as pd
import numpy as np
import os
import cv2
import json

def getVideoInfoDfFromJson(jsonStr):
    # 解析JSON字符串
    jsonObj = json.loads(jsonStr)
    dataList = jsonObj['data']['first_level']['list']

    videoInfoDf = pd.DataFrame()

    for i in range(len(dataList)):
        index = i + 1

        # for test
        if index >= 200:
            break

        filename = f'{index}.mp4'
        name = dataList[i]['material_name']
        cost = dataList[i]['cost_value_usd']
        # cost_rate = dataList[i]['cost_rate']

        video_url = dataList[i]['video_url']
        # 下载视频到本地 videos目录下
        video_dir = 'videos'
        if not os.path.exists(video_dir):
            os.makedirs(video_dir)

        video_path = os.path.join(video_dir, filename)
        video_frames_dir = os.path.join(video_dir, f'{index}_frames')
        # if not os.path.exists(video_path):
        # 改为如果帧目录不存在，则下载视频，重新制作帧
        if not os.path.exists(video_frames_dir):
            # 下载视频
            os.system(f'curl -o {video_path} {video_url}')
            os.makedirs(video_frames_dir)
            cap = cv2.VideoCapture(video_path)
            fps = cap.get(cv2.CAP_PROP_FPS)
            fps = int(fps)
            print('fps:',fps)
            frame_count = 0
            frame_name_count = 1
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                if frame_count % fps == 0:
                    frame_filename = os.path.join(video_frames_dir, f'frame_{frame_name_count}.jpg')
                    frame_name_count += 1
                    cv2.imwrite(frame_filename, frame,[cv2.IMWRITE_JPEG_QUALITY, 60])
                    # 最多保存30帧
                    if frame_name_count > 30:
                        break

                frame_count += 1
            cap.release()
            # 删除视频文件
            os.remove(video_path)

        # 创建一个新的 DataFrame行
        new_row = pd.DataFrame([{
            'filename': filename,
            'frames_dir': video_frames_dir,
            'video_url': video_url,
            'name': name,
            'cost': cost,
            # 'cost_rate': cost_rate
        }])

        # 使用 pd.concat() 将新行添加到现有 DataFrame
        videoInfoDf = pd.concat([videoInfoDf, new_row], ignore_index=True)


    return videoInfoDf

    


    
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

# 将videoInfoDf中，每个视频使用findColorTag3s函数
# 最终输出一个dataframe，列名为：videoInfoDf 元有列 + '赤ratio','赤2ratio','橙ratio','黄ratio','绿ratio','蓝ratio','紫ratio','黑ratio','白ratio','灰ratio','otherratio'
def videoInfoAddColorTag(videoInfoDf):

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
from sklearn.metrics import mean_absolute_percentage_error

def fit_predict_cost_with_decision_tree():
    
    data = pd.read_csv('videoWithColorTag.csv')
    # 提取颜色比例特征
    color_features = data.columns[4:-1]  # 颜色比例特征列名
    X = data[color_features]  # 输入特征
    
    # 提取目标变量
    y = data['cost']  # 输出变量
    
    # 初始化决策树回归模型
    model = DecisionTreeRegressor(
        # max_depth=5,              # 限制树的最大深度
        # min_samples_split=10,     # 内部节点再分裂所需的最小样本数
        # min_samples_leaf=5,       # 叶子节点所需的最小样本数
        # max_features='sqrt',      # 每次分裂时考虑的最大特征数
        max_leaf_nodes=20         # 限制叶子节点的最大数量
    )
    
    # 拟合模型
    model.fit(X, y)
    
    # 预测 cost
    data['predicted_cost'] = model.predict(X)
    
    # 计算每行的 MAPE
    data['mape'] = np.abs(data['cost'] - data['predicted_cost']) / data['cost']
    
    # 计算最终的 MAPE 的 mean
    mean_mape = data['mape'].mean()
    
    return model, data, mean_mape

import matplotlib.pyplot as plt
def visualize_tree(model, feature_names):
    plt.figure(figsize=(40, 40))
    plt.rcParams['font.sans-serif'] = ['Hiragino Sans GB'] 
    plot_tree(model, feature_names=feature_names, filled=True, rounded=True)
    plt.savefig("decision_tree.png")  # 保存为 decision_tree.png
    # plt.show()

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

    # 按照cost进行分类，超过1000000的标记为0，超过100000的标记为1，超过10000的标记为2，其他的标记为3
    y = np.zeros(data.shape[0])
    # y[data['cost'] <= 10000] = 3
    # y[data['cost'] > 10000] = 2
    # y[data['cost'] > 100000] = 1
    # y[data['cost'] > 1000000] = 0


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

def main():
    # jsonFilename = '20250424.json'

    # # 读取json文件
    # with open(jsonFilename, 'r', encoding='utf-8') as f:
    #     jsonStr = f.read()
    
    # videoInfoDf = getVideoInfoDfFromJson(jsonStr)
    # videoInfoDf.to_csv('video.csv', index=False)

    # # checkColor(color_ranges)

    # videoInfoWithColorTagDf = videoInfoAddColorTag(videoInfoDf)
    # videoInfoWithColorTagDf.to_csv('videoWithColorTag.csv', index=False)

    model, data, precision, recall, r2 = fit_predict_cost_with_decision_tree2()
    data.to_csv('fit_predict_cost_with_decision_tree2.csv', index=False)
    print(f"Precision: {precision}")
    print(f"Recall: {recall}")
    print(f"R2: {r2}")


if __name__ == "__main__":
    main()

    
    
