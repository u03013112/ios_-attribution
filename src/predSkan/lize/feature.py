# 特征筛选
# 目前拥有df，每个用户一行，uid是唯一键，每个用户拥有唯一的安装日期，类似2023-03-01
# 拥有特征列 f1,f2,f3等，拥有标签列 r7usd 是用户7日收入的美元金额
# 希望将用户分成8组，每组的7日收入美元金额的平均值作为yp 与 用户数据每安装日汇总后的的该组7日收入美元金额的平均值作为y ，按照安装日计算的MAPE尽量小
# 1、对目前的特征进行预处理，预处理进行交叉验证，选出最好的预处理方式
# 2、特征筛选
# 3、对筛选过的特征进行kmean分类
# 4、对上述分类进行MAPE计算

import pandas as pd
import numpy as np
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
from sklearn.feature_selection import RFE


import sys
sys.path.append('/src')
from src.tools import getFilename

# 特征预处理方案选择，得到结论，RobustScaler看起来最好
def featurePreprocessing():
    installDateDf = pd.read_csv(getFilename('demoSsLoginByInstallDate'))
    totalDf = pd.read_csv(getFilename('demoSsAllMakeLabel'))
    totalDf = totalDf.merge(installDateDf, on='uid', how='left')

    for cv1 in range(8):
        df = totalDf.loc[totalDf.cv1 == cv1]

        # 特征列表，是在df中的列名，排除uid,installDate,r1usd,r7usd,cv1,cv7
        feature_list = df.columns.tolist()
        feature_list.remove('uid')
        feature_list.remove('installDate')
        feature_list.remove('r1usd')
        feature_list.remove('r7usd')
        feature_list.remove('cv1')
        feature_list.remove('cv7')
        # print(feature_list)

        # 预处理方法
        preprocessing_methods = [StandardScaler(), MinMaxScaler(), RobustScaler()]

        # 交叉验证
        tscv = TimeSeriesSplit(n_splits=5)

        # 存储结果
        results = []

        # 遍历预处理方法
        for method in preprocessing_methods:
            mape_scores = []
            
            # 交叉验证
            for train_index, test_index in tscv.split(df):
                train, test = df.iloc[train_index], df.iloc[test_index]
                
                
                # 预处理
                X_train = train[feature_list]
                X_test = test[feature_list]
                method.fit(X_train)
                X_train = method.transform(X_train)
                X_test = method.transform(X_test)
                
                # 计算y
                y_train = train['r7usd']
                y_test = test['r7usd']
                
                # 使用线性回归模型进行预测
                model = LinearRegression()
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)
                
                # 计算MAPE
                mape = mean_absolute_percentage_error(y_test, y_pred)
                mape_scores.append(mape)
            
            # 计算平均MAPE
            avg_mape = np.mean(mape_scores)
            results.append((method.__class__.__name__, avg_mape))

        # 输出结果
        best_method, best_mape = min(results, key=lambda x: x[1])
        print(f"Best preprocessing method: {best_method}, with MAPE: {best_mape}")

# 目前拥有df，每个用户一行，uid是唯一键，每个用户拥有唯一的安装日期，类似2023-03-01
# 拥有特征列 f1,f2,f3等，拥有标签列 cv7，cv7是0~8的整数，多分类
# 特征用RobustScaler预处理
# 对所有特征进行筛选，找到最好的特征组合
def featureSelection():
    installDateDf = pd.read_csv(getFilename('demoSsLoginByInstallDate'))
    totalDf = pd.read_csv(getFilename('demoSsAllMakeLabel'))
    totalDf = totalDf.merge(installDateDf, on='uid', how='left')

    # 特征列表，是在df中的列名，排除uid,installDate,r1usd,r7usd,cv1,cv7
    feature_list = totalDf.columns.tolist()
    feature_list.remove('uid')
    feature_list.remove('installDate')
    feature_list.remove('r1usd')
    feature_list.remove('r7usd')
    feature_list.remove('cv1')
    feature_list.remove('cv7')
    # print(feature_list)

    # 预处理方法
    preprocessing_method = RobustScaler()

    for cv1 in range(8):
        print(f"cv1: {cv1}")
        df = totalDf.loc[totalDf.cv1 == cv1]

        # 分类器
        classifier = LogisticRegression(multi_class='multinomial', solver='lbfgs')

        # 特征选择
        feature_columns = feature_list
        X = df[feature_columns]
        y = df['cv7']

        # 预处理
        preprocessing_method.fit(X)
        X = preprocessing_method.transform(X)

        # RFE
        rfe = RFE(estimator=classifier, n_features_to_select=3)  # 更改n_features_to_select以选择所需的特征数量
        rfe.fit(X, y)

        # 打印选定的特征
        selected_features = np.array(feature_columns)[rfe.support_]
        print(f"Selected features: {selected_features}")

# 分类
# 目前拥有df，每个用户一行，uid是唯一键，每个用户拥有唯一的安装日期，类似2023-03-01
# 拥有特征列很多，拥有标签列cv1,cv7
# 按照cv1进行分类后，不同的cv1采用不同的特征组合，具体参照features
# 用以上特征 kmean的方式，对每个cv1进行聚类，得到每个cv1的用户群体，记录在列 cv7p
from sklearn.cluster import KMeans
def kmean():
    features = [
        ['countUserLevelMax','count','countMergeArmy'],# cv0
        ['countPayCount','ENERGY','countUserLevelMax'],# cv1
        ['countPayCount','ENERGY','countUserLevelMax'],# cv2
        ['countPayCount','ENERGY','countUserLevelMax'],# cv3
        ['count','countUserLevelMax','ENERGY'],# cv4
        ['countUserLevelMax','ENERGY','countMergeBuilding'],# cv5
        ['countUserLevelMax','countHeroLevelUp','countHeroStarUp'],# cv6
        ['countPayCount','SOIL','OILA']# cv7
    ]
    installDateDf = pd.read_csv(getFilename('demoSsLoginByInstallDate'))
    totalDf = pd.read_csv(getFilename('demoSsAllMakeLabel'))
    totalDf = totalDf.merge(installDateDf, on='uid', how='left')

    # 预处理方法
    preprocessing_method = RobustScaler()

    result_df = pd.DataFrame()

    for cv1 in range(8):
        print(f"cv1: {cv1}")
        df = totalDf.loc[totalDf.cv1 == cv1].copy(deep=True)

        # 选择特征列
        X = df[features[cv1]]

        # 预处理
        preprocessing_method.fit(X)
        X = preprocessing_method.transform(X)


        # 使用K-Means聚类
        kmeans = KMeans(n_clusters=8) # 假设您希望将每个cv1类别的用户分成3个群体
        df['cv7p'] = kmeans.fit_predict(X)
        result_df = result_df.append(df)
        print(df.head(n=100))

    return result_df



if __name__ == "__main__":
    # featurePreprocessing()
    # featureSelection()
    df = kmean()
    df.to_csv(getFilename('kmean'), index=False)