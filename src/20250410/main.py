# 视频获取
# Google 
# US
# 视频
# 上传时间：2025-01-01~2025-02-28
# 消耗时间：2025-03-01~2025-03-31
# 消耗金额前10名
import pandas as pd
import numpy as np
import os
import cv2
import json

videoInfoDf = pd.read_csv('video2.csv')

def getVideoInfoDfFromJson(jsonStr):
    # 解析JSON字符串
    jsonObj = json.loads(jsonStr)
    dataList = jsonObj['data']['first_level']['list']

    videoInfoDf = pd.DataFrame()

    for i in range(len(dataList)):
        index = i + 1
        filename = f'{index}.mp4'
        name = dataList[i]['material_name']
        cost = dataList[i]['cost_value_usd']
        cost_rate = dataList[i]['cost_rate']

        video_url = dataList[i]['video_url']
        # 下载视频到本地 videos目录下
        video_dir = 'videos'
        if not os.path.exists(video_dir):
            os.makedirs(video_dir)

        video_path = os.path.join(video_dir, filename)
        if not os.path.exists(video_path):
            # 下载视频
            os.system(f'curl -o {video_path} {video_url}')

        # 创建一个新的 DataFrame行
        new_row = pd.DataFrame([{
            'filename': filename,
            'name': name,
            'cost': cost,
            'cost_rate': cost_rate
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

# 将videoInfoDf中，每个视频使用findColorTag3s函数
# 最终输出一个dataframe，列名为：videoInfoDf 元有列 + '赤ratio','赤2ratio','橙ratio','黄ratio','绿ratio','蓝ratio','紫ratio','黑ratio','白ratio','灰ratio','otherratio'
def videoInfoAddColorTag(videoInfoDf):

    colorTagDf = pd.DataFrame()
    for filename in videoInfoDf['filename']:
        colorTagDf0 = findColorTag3s(filename)
        # 将colorTagDf0 pivot成列,filename,'赤ratio','赤2ratio' 等
        colorTagDf0 = colorTagDf0.pivot(index='filename', columns='color', values='ratio').reset_index()
        colorTagDf = pd.concat([colorTagDf, colorTagDf0], ignore_index=True)

    videoInfoDf = pd.merge(videoInfoDf, colorTagDf, on='filename', how='left')

    return videoInfoDf

from econml.dml import LinearDML
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor

def econml_example():
    # 读取数据
    data = pd.read_csv('videoWithColorTag.csv')
    
    # 提取颜色比例特征
    color_features = ['橙', '灰', '白', '紫', '绿', '蓝', '赤', '赤2', '黄', '黑']
    T = data[color_features]  # 处理变量
    


    # 提取目标变量
    y_cost = data['cost']
    
    # 使用双重机器学习模型进行因果推断
    model_y = RandomForestRegressor()  # 用于预测目标变量
    model_t = LinearRegression()       # 用于预测处理变量
    
    # 初始化 DML 模型
    dml_cost = LinearDML(model_y=model_y, model_t=model_t)
    
    # 拟合模型
    dml_cost.fit(Y=y_cost, T=T, X=None)
    
    # 计算 ATE
    ate_cost = dml_cost.ate(X=None)

    print("ATE for cost:")
    print(ate_cost)
    
    # # 输出结果
    # print("ATE for cost:")
    # for feature, ate in zip(color_features, ate_cost):
    #     print(f"{feature}: {ate}")


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

from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.optimizers import Adam
from sklearn.preprocessing import StandardScaler

def fit_predict_cost_with_dnn():
    # 读取数据
    data = pd.read_csv('videoWithColorTag.csv')
    
    # 提取颜色比例特征
    color_features = ['橙', '灰', '白', '紫', '绿', '蓝', '赤', '赤2', '黄', '黑']
    X = data[color_features].values  # 输入特征
    
    # 提取目标变量
    y = data['cost'].values  # 输出变量
    # 标准化输入特征
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 初始化 DNN 模型
    model = Sequential([
        Dense(64, activation='relu', input_shape=(X.shape[1],)),
        Dense(32, activation='relu'),
        Dense(32, activation='relu'),
        Dense(1)
    ])
    
    # 编译模型
    model.compile(optimizer='RMSprop', loss='mean_squared_error', metrics=['mean_absolute_percentage_error'])
    # model.compile(optimizer='RMSprop', loss='mean_absolute_percentage_error', metrics=['mean_absolute_percentage_error'])
    
    # 拟合模型
    model.fit(
        X_scaled, y, epochs=10000, batch_size=8,
        validation_split=0.2,  # 20% 的数据用于验证
        verbose=1
    )
    
    # 预测 cost
    data['predicted_cost'] = model.predict(X_scaled).flatten()
    
    # 计算每行的 MAPE
    # data['mape'] = mean_absolute_percentage_error(data['cost'], data['predicted_cost'])
    data['mape'] = np.abs(data['cost'] - data['predicted_cost']) / data['cost']
    
    # 计算最终的 MAPE 的 mean
    mean_mape = data['mape'].mean()
    
    
    return model, data, mean_mape

from sklearn.metrics import precision_score, recall_score, r2_score
def fit_predict_cost_with_dnn2():
    # 读取数据
    data = pd.read_csv('videoWithColorTag.csv')
    
    # 提取颜色比例特征
    color_features = ['橙', '灰', '白', '紫', '绿', '蓝', '赤', '赤2', '黄', '黑']
    X = data[color_features].values  # 输入特征
    
    # 提取目标变量
    # 将 cost 前10名标记为畅销素材
    top_10_indices = data['cost'].nlargest(10).index
    y = np.zeros(data.shape[0])
    y[top_10_indices] = 1
    
    # 标准化输入特征
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 初始化 DNN 模型
    model = Sequential([
        Dense(64, activation='relu', input_shape=(X_scaled.shape[1],)),
        Dense(32, activation='relu'),
        Dense(32, activation='relu'),
        Dense(1, activation='sigmoid')  # 使用 sigmoid 激活函数进行二分类
    ])
    
    # 编译模型
    model.compile(optimizer='RMSprop', loss='binary_crossentropy', metrics=['accuracy'])
    
    # 拟合模型
    model.fit(
        X_scaled, y, epochs=10000, batch_size=8,
        validation_split=0.2,  # 20% 的数据用于验证
        verbose=1
    )
    
    # 预测畅销素材
    data['predicted_value'] = model.predict(X_scaled).flatten()
    data['predicted_class'] = (model.predict(X_scaled).flatten() > 0.5).astype(int)
    
    # 计算查准率和查全率
    precision = precision_score(y, data['predicted_class'])
    recall = recall_score(y, data['predicted_class'])
    
    # 计算 R2
    r2 = r2_score(y, data['predicted_class'])
    
    # print(f"Precision: {precision}")
    # print(f"Recall: {recall}")
    # print(f"R2: {r2}")
    
    return model, data, precision, recall, r2

from sklearn.tree import DecisionTreeClassifier
def fit_predict_cost_with_decision_tree2():
    # 读取数据
    data = pd.read_csv('videoWithColorTag.csv')
    
    # 提取颜色比例特征
    color_features = data.columns[4:-1]  # 颜色比例特征列名
    print('color_features:')
    print(color_features)

    X = data[color_features]  # 输入特征
    
    # 提取目标变量
    # 将 cost 前10名标记为畅销素材
    top_10_indices = data['cost'].nlargest(10).index
    y = np.zeros(data.shape[0])
    y[top_10_indices] = 1
    
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
    # jsonFilename = '20250411.json'

    # # 读取json文件
    # with open(jsonFilename, 'r', encoding='utf-8') as f:
    #     jsonStr = f.read()
    
    # videoInfoDf = getVideoInfoDfFromJson(jsonStr)
    # videoInfoDf.to_csv('video2.csv', index=False)

    # checkColor(color_ranges)

    # videoInfoWithColorTagDf = videoInfoAddColorTag(videoInfoDf)
    # videoInfoWithColorTagDf.to_csv('videoWithColorTag.csv', index=False)

    # 使用决策树进行拟合和预测
    model, data_with_predictions, mean_mape = fit_predict_cost_with_decision_tree()
    
    # 输出结果
    print(data_with_predictions[['filename', 'cost', 'predicted_cost', 'mape']])
    print(f"Mean MAPE: {mean_mape}")
    
    # 可视化决策树
    visualize_tree(model, ['橙', '灰', '白', '紫', '绿', '蓝', '赤', '赤2', '黄', '黑'])

    # model, data, mean_mape = fit_predict_cost_with_dnn()
    # data.to_csv('videoWithColorTag_dnn.csv', index=False)
    # print(f"Mean MAPE: {mean_mape}")


if __name__ == "__main__":
    # econml_example()

    # main()

    # model, data, mean_mape = fit_predict_cost_with_dnn()
    # data.to_csv('fit_predict_cost_with_dnn.csv', index=False)
    # print(f"Mean MAPE: {mean_mape}")

    # model, data, precision, recall, r2 = fit_predict_cost_with_dnn2()
    # data.to_csv('fit_predict_cost_with_dnn2.csv', index=False)
    # print(f"Precision: {precision}")
    # print(f"Recall: {recall}")
    # print(f"R2: {r2}")

    model, data, precision, recall, r2 = fit_predict_cost_with_decision_tree2()
    data.to_csv('fit_predict_cost_with_decision_tree2.csv', index=False)
    print(f"Precision: {precision}")
    print(f"Recall: {recall}")
    print(f"R2: {r2}")
