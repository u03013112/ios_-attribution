# 用神经网络进行多分类
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

import sys
sys.path.append('/src')
from src.tools import getFilename

# 步骤大致如下
# 1. 读取数据，安装日期，uid，r1usd,r7usd
# 2. 添加label，cv1，cv7
def step1And2():
    installDateDf = pd.read_csv(getFilename('demoSsLoginByInstallDate'))
    df = pd.read_csv(getFilename('demoSsAllMakeLabel'))

    df = df.merge(installDateDf, on='uid', how='left')
    return df
   
# 3. 添加特征，区分每个cv1进行特征分组，并标注到用户
def setp3(df):
    # cv1中的最佳特征，每个暂时只有3个
    features = [
        ['ENERGY','countHeroStarUp','countMergeBuilding'],# cv0
        ['countPayCount','countUserLevelMax','ENERGY'],# cv1
        ['ENERGY','countUserLevelMax','countMergeBuilding'],# cv2
        ['countUserLevelMax','ENERGY','countHeroStarUp'],# cv3
        ['countUserLevelMax','ENERGY','countMergeBuilding'],# cv4
        ['countUserLevelMax','ENERGY','countMergeBuilding'],# cv5
        ['countUserLevelMax','ENERGY','countMergeBuilding'],# cv6
        ['countPayCount','countUserLevelMax','OILA']# cv7
    ]
    featureDf = pd.DataFrame(
        columns=['uid','f1g','f2g','f3g']
    )
    for cv1 in range(8):
        # 将用户特征分组标注给用户
        # 从features[cv1]中找到特征的列名，将该组的指定特征进行聚类分组，分为4组，并将分组结果标注到'f1g','f2g','f3g'列中
        cv1Df = df.loc[df['cv1'] == cv1]
        cv1Df = cv1Df[['uid']+features[cv1]].copy(deep = True)
        for f in range(3):
            feature = features[cv1][f]
            # kmeans = KMeans(n_clusters=4, random_state=0).fit(cv1Df[[feature]])
            bins = pd.cut(cv1Df[feature], 4)
            bin_codes = bins.codes
            cv1Df['f%dg'%(f+1)] = bin_codes
        featureTmpDf = cv1Df[['uid','f1g','f2g','f3g']]
        featureDf = featureDf.append(featureTmpDf)
    df = df.merge(featureDf, on='uid', how='left')
    return df

def setp3Gpt(df):
    features = [
      ['ENERGY', 'countHeroStarUp', 'countMergeBuilding'],
      ['countPayCount', 'countUserLevelMax', 'ENERGY'],
      ['ENERGY', 'countUserLevelMax', 'countMergeBuilding'],
      ['countUserLevelMax', 'ENERGY', 'countHeroStarUp'],
      ['countUserLevelMax', 'ENERGY', 'countMergeBuilding'],
      ['countUserLevelMax', 'ENERGY', 'countMergeBuilding'],
      ['countUserLevelMax', 'ENERGY', 'countMergeBuilding'],
      ['countPayCount', 'countUserLevelMax', 'OILA']
    ]
    featureDf = pd.DataFrame(columns=['uid', 'f1g', 'f2g', 'f3g'])  # 创建一个空的 DataFrame，包含所需的列
    for cv1 in range(8):
      cv1Df = df.loc[df['cv1'] == cv1]
      cv1Df = cv1Df[['uid'] + features[cv1]].copy(deep=True)
      for f in range(3):
        feature = features[cv1][f]
        bins = pd.cut(cv1Df[feature], 4, include_lowest=True, right=False)
        bin_codes = bins.cat.codes
        cv1Df['f%dg' % (f + 1)] = bin_codes.astype(int)
      featureTmpDf = cv1Df[['uid', 'f1g', 'f2g', 'f3g']]
      featureDf = pd.concat([featureDf, featureTmpDf], ignore_index=True)
    df = df.merge(featureDf, on='uid', how='left')
    return df

# 与setp3Gpt的区别是不再对特征进行分档，而是将特征原样放到f1g,f2g,f3g中
# 只做特征预处理
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
def step3Fix(df):
    # 预处理方法
    preprocessing_method = RobustScaler()
    features = [
      ['ENERGY', 'countHeroStarUp', 'countMergeBuilding'],
      ['countPayCount', 'countUserLevelMax', 'ENERGY'],
      ['ENERGY', 'countUserLevelMax', 'countMergeBuilding'],
      ['countUserLevelMax', 'ENERGY', 'countHeroStarUp'],
      ['countUserLevelMax', 'ENERGY', 'countMergeBuilding'],
      ['countUserLevelMax', 'ENERGY', 'countMergeBuilding'],
      ['countUserLevelMax', 'ENERGY', 'countMergeBuilding'],
      ['countPayCount', 'countUserLevelMax', 'OILA']
    ]
    featureDf = pd.DataFrame(columns=['uid', 'f1g', 'f2g', 'f3g'])  # 创建一个空的 DataFrame，包含所需的列
    for cv1 in range(8):
      cv1Df = df.loc[df['cv1'] == cv1]
      cv1Df = cv1Df[['uid'] + features[cv1]].copy(deep=True)
      for f in range(3):
        feature = features[cv1][f]
        X = cv1Df[[feature]]
        preprocessing_method.fit(X)
        cv1Df['f%dg' % (f + 1)] = preprocessing_method.transform(X)
      featureTmpDf = cv1Df[['uid', 'f1g', 'f2g', 'f3g']]
      featureDf = pd.concat([featureDf, featureTmpDf], ignore_index=True)
    df = df.merge(featureDf, on='uid', how='left')
    return df
import tensorflow as tf
from tensorflow.keras.utils import to_categorical

from keras.models import Sequential
from keras.layers import Dense
from keras.callbacks import EarlyStopping
from sklearn.metrics import mean_absolute_percentage_error, r2_score

# 4. 按照cv1分组，分别进行训练，预测 将 cv7p，r7usdp 标注到用户
def dnnTrain(df):
    trainRetDf = pd.DataFrame()
    testRetDf = pd.DataFrame()
    # 显示所有不同的cv7
    # print(df['cv7'].unique())
    # 按照cv1分组
    for cv1 in range(1,8):
        cv1Df = df.loc[df['cv1'] == cv1].copy(deep=True)
        
        # 每组分别进行训练，预测。将数据分为70%训练集，30%测试集。
        cv1Df = cv1Df.sample(frac=1).reset_index(drop=True)
        trainDf = cv1Df.iloc[:int(len(cv1Df)*0.7)].copy(deep=True)
        testDf = cv1Df.iloc[int(len(cv1Df)*0.7):].copy(deep=True)

        # 其中x是列 f1g,f2g,f3g。y是列 cv7。
        x_train = trainDf[['f1g','f2g','f3g']]
        y_train = to_categorical(trainDf['cv7'].to_numpy(), num_classes=9)
        # 打印训练集的数据前20行
        print(x_train.head(20))
        print(y_train[:20])

        x_test = testDf[['f1g','f2g','f3g']]
        y_test = to_categorical(testDf['cv7'].to_numpy(), num_classes=9)

        # 创建简单多分类dnn模型，输入是3，输出9个分类的几率
        def create_model():
            model = Sequential()
            model.add(Dense(64, input_dim=3, activation='relu'))
            
            model.add(Dense(64, activation='relu'))
            model.add(Dense(64, activation='relu'))
            model.add(Dense(64, activation='relu'))
            
            model.add(Dense(9, activation='softmax'))
            model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
            return model

        # 定义EarlyStopping回调
        early_stopping = EarlyStopping(monitor='val_accuracy', patience=5, restore_best_weights=True)
        

        # 训练模型，要求反复训练几次，每次训练到测试集的准确率稳定后停止。
        # 初始化最佳准确率和最佳权重
        best_accuracy = 0
        best_weights = None

        # 反复训练5次
        num_iterations = 2
        for i in range(num_iterations):
            print(f'Iteration {i + 1}')
            
            # 创建并训练模型
            model = create_model()
            model.fit(x_train, y_train, epochs=5, batch_size=10, validation_data=(x_test, y_test)
                # , callbacks=[early_stopping]
            )
            
            # 获取测试集上的准确率
            test_accuracy = model.evaluate(x_test, y_test, verbose=0)[1]
            
            # 如果当前模型的测试集准确率更好，则更新最佳准确率和最佳权重
            if test_accuracy > best_accuracy:
                best_accuracy = test_accuracy
                best_weights = model.get_weights()

        # 将训练集和测试集预测结果都标注到用户cv7p。
        # 使用最佳权重创建最终模型
        final_model = create_model()
        final_model.set_weights(best_weights)
        # 保存模型
        final_model.save('/src/data/dnn_%d.h5'%cv1)

        # 训练集预测
        predictions = final_model.predict(x_train)
        predicted_classes = np.argmax(predictions, axis=1)
        trainDf['cv7p'] = predicted_classes

        # 测试集预测
        predictions = final_model.predict(x_test)
        predicted_classes = np.argmax(predictions, axis=1)
        testDf['cv7p'] = predicted_classes

        # 拼接
        # cv1Df = pd.concat([trainDf, testDf], ignore_index=True)

        # 计算cv7p对应的r7usdp，标注到用户。
        cvMapDf7 = pd.read_csv(getFilename('cvMapDf7_%d'%cv1))
        for cv7 in range(9):
            if len(cvMapDf7.loc[cvMapDf7.cv == cv7]) <= 0:
                continue
            min_event_revenue = cvMapDf7.loc[cvMapDf7.cv == cv7]['min_event_revenue'].values[0]
            max_event_revenue = cvMapDf7.loc[cvMapDf7.cv == cv7]['max_event_revenue'].values[0]
            avg = (min_event_revenue + max_event_revenue)/2
            if avg < 0:
                avg = 0
            trainDf.loc[trainDf['cv7p']==cv7,'r7usdp'] = avg
            testDf.loc[testDf['cv7p']==cv7,'r7usdp'] = avg
            
        # 拼接
        trainRetDf = pd.concat([trainRetDf, trainDf], ignore_index=True)
        testRetDf = pd.concat([testRetDf, testDf], ignore_index=True)

    trainRetDf.to_csv(getFilename('dnn1Step4Train'), index=False)
    testRetDf.to_csv(getFilename('dnn1Step4Test'), index=False)

    # 按照installDate汇总，排序，r7usd和r7usdp取sum
    trainRetSumDf = trainRetDf.groupby(['installDate']).agg({'r7usd': 'sum', 'r7usdp': 'sum'}).reset_index()
    testRetSumDf = testRetDf.groupby(['installDate']).agg({'r7usd': 'sum', 'r7usdp': 'sum'}).reset_index()

    # 计算r7usdp与r7usd的Mape和R2，记录Log
    trainMape = mean_absolute_percentage_error(trainRetSumDf['r7usd'], trainRetSumDf['r7usdp'])
    trainR2 = r2_score(trainRetSumDf['r7usd'], trainRetSumDf['r7usdp'])
    print('train mape: %f, r2: %f'%(trainMape, trainR2))
    testMape = mean_absolute_percentage_error(testRetSumDf['r7usd'], testRetSumDf['r7usdp'])
    testR2 = r2_score(testRetSumDf['r7usd'], testRetSumDf['r7usdp'])
    print('test mape: %f, r2: %f'%(testMape, testR2))


# 5. 预测结果计算r7usd 与 r7usdp 的Mape和R2，记录Log


if __name__ == '__main__':
    df = step1And2()
    df.to_csv(getFilename('dnn1Step2'), index=False)

    # # df = setp3Gpt(df)
    # df = step3Fix(df)
    # print(df.head())
    # df.to_csv(getFilename('dnn1Step3Fix'), index=False)

    # df = pd.read_csv(getFilename('dnn1Step3Fix'))
    # dnnTrain(df)

    # df = pd.read_csv(getFilename('dnn1Step4'))
    # retSumDf = df.groupby(['uid', 'installDate']).agg({'r7usd': 'sum', 'r7usdp': 'sum'}).reset_index()
    # mape = mean_absolute_percentage_error(retSumDf['r7usd'], retSumDf['r7usdp'])
    # r2 = r2_score(retSumDf['r7usd'], retSumDf['r7usdp'])
    # print('mape: %f, r2: %f'%(mape, r2))
    # retSumDf.to_csv(getFilename('dnn1Step5'), index=False)