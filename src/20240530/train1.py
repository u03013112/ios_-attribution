import numpy as np
import pandas as pd  
from itertools import combinations
from sklearn.model_selection import train_test_split  
from sklearn.ensemble import RandomForestClassifier  
from sklearn.metrics import classification_report, accuracy_score, recall_score, make_scorer,precision_score
from sklearn.model_selection import GridSearchCV    
from scipy.stats import pointbiserialr
from sklearn.cluster import KMeans

def classify_and_evaluate():
    # 加载数据并筛选特征和目标列
    df = pd.read_csv('/src/data/lwData20240530.csv')
    
    # 只分析24小时内不付费用户
    freeDf = df.loc[(df['payNew24'] == 0)]
    
    # 选取目标列并转换为二分类问题
    freeDf['target'] = freeDf['payNew168'].apply(lambda x: 1 if x > 0 else 0)
    
    # 分离出目标为1的样本以及目标为0的样本
    positive_samples = freeDf[freeDf['target'] == 1]
    negative_samples = freeDf[freeDf['target'] == 0].sample(frac=0.1, random_state=0)
    
    # 合并进行采样后的数据
    sampled_df = pd.concat([positive_samples, negative_samples])

    # 选取特征列
    X = sampled_df[['heroLevelUp', 'appLaunch', 'login', 'payAction', 'plunder', 'goldCost', 'onlineTime', 'mainLevel', 'radar']]
    
    # 重新选取目标列
    y = sampled_df['target']
    
    # 将数据集分为训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
    
    # 初始化随机森林分类器
    n_estimators = 100  # 设定树的数量
    rf_clf = RandomForestClassifier(n_estimators=0, random_state=0, warm_start=True, n_jobs=-1)
    
    # 增量增加树并逐步训练，每训练10次进行一次评估
    for i in range(n_estimators):
        rf_clf.n_estimators = i + 1
        rf_clf.fit(X_train, y_train)
        
        if (i + 1) % 10 == 0:
            print(f'Trained {i + 1}/{n_estimators} trees')
            # 在训练集上预测
            y_train_pred = rf_clf.predict(X_train)
            # 在测试集上预测
            y_test_pred = rf_clf.predict(X_test)

            # 输出训练集表现
            print("Training Performance:")
            print(classification_report(y_train, y_train_pred))
            print('Training Accuracy:', accuracy_score(y_train, y_train_pred))

            # 输出测试集表现
            print("Test Performance:")
            print(classification_report(y_test, y_test_pred))
            print('Test Accuracy:', accuracy_score(y_test, y_test_pred))

# 添加早停
def classify_and_evaluate_et():
    # 加载数据并筛选特征和目标列
    df = pd.read_csv('/src/data/lwData20240530.csv')
    
    # 只分析24小时内不付费用户
    freeDf = df.loc[(df['payNew24'] == 0)]
    
    # 选取目标列并转换为二分类问题
    freeDf['target'] = freeDf['payNew168'].apply(lambda x: 1 if x > 0 else 0)
    
    # 分离出目标为1的样本以及目标为0的样本
    positive_samples = freeDf[freeDf['target'] == 1]
    negative_samples = freeDf[freeDf['target'] == 0].sample(frac=0.1, random_state=0)
    
    # 合并进行采样后的数据
    sampled_df = pd.concat([positive_samples, negative_samples])

    # 选取特征列
    X = sampled_df[['heroLevelUp', 'appLaunch', 'login', 'payAction', 'plunder', 'goldCost', 'onlineTime', 'mainLevel', 'radar']]
    
    # 重新选取目标列
    y = sampled_df['target']
    
    # 将数据集分为训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
    
    # 初始化随机森林分类器
    n_estimators = 100  # 设定树的数量
    rf_clf = RandomForestClassifier(n_estimators=1, random_state=0, warm_start=True, n_jobs=-1)
    
    # 早停参数
    best_val_score = 0
    best_n_estimators = 0
    patience = 10  # 早停的耐心值
    no_improvement_count = 0

    # 增量增加树并逐步训练，每训练10次进行一次评估
    for i in range(1, n_estimators + 1):
        rf_clf.n_estimators = i
        rf_clf.fit(X_train, y_train)
        
        # 在验证集上评估
        y_val_pred = rf_clf.predict(X_test)
        val_score = accuracy_score(y_test, y_val_pred)
        
        if val_score > best_val_score:
            best_val_score = val_score
            best_n_estimators = i
            no_improvement_count = 0
        else:
            no_improvement_count += 1
        
        if no_improvement_count >= patience:
            print(f'Early stopping at {i} trees')
            break
        
        if i % 10 == 0:
            print(f'Trained {i}/{n_estimators} trees')
            # 在训练集上预测
            y_train_pred = rf_clf.predict(X_train)
            # 在测试集上预测
            y_test_pred = rf_clf.predict(X_test)

            # 输出训练集表现
            print("Training Performance:")
            print(classification_report(y_train, y_train_pred))
            print('Training Accuracy:', accuracy_score(y_train, y_train_pred))

            # 输出测试集表现
            print("Test Performance:")
            print(classification_report(y_test, y_test_pred))
            print('Test Accuracy:', accuracy_score(y_test, y_test_pred))
    
    # 最终输出最佳树的数量和对应的表现
    print(f'Best number of trees: {best_n_estimators}, Best Validation Accuracy: {best_val_score:.4f}')
    
    # 使用最佳树的数量重新训练模型
    rf_clf.n_estimators = best_n_estimators
    rf_clf.fit(X_train, y_train)
    
    # 在训练集上预测
    y_train_pred = rf_clf.predict(X_train)
    # 在测试集上预测
    y_test_pred = rf_clf.predict(X_test)

    # 输出最终训练集表现
    print("Final Training Performance:")
    print(classification_report(y_train, y_train_pred))
    print('Training Accuracy:', accuracy_score(y_train, y_train_pred))

    # 输出最终测试集表现
    print("Final Test Performance:")
    print(classification_report(y_test, y_test_pred))
    print('Test Accuracy:', accuracy_score(y_test, y_test_pred))

# 调用函数
# classify_and_evaluate_et()


# 调整权重
def classify_and_evaluate2():
    # 加载数据并筛选特征和目标列
    df = pd.read_csv('/src/data/lwData20240530.csv')
    
    # 只分析24小时内不付费用户
    freeDf = df.loc[(df['payNew24'] == 0)]
    # 选取目标列并转换为二分类问题
    freeDf['target'] = freeDf['payNew168'].apply(lambda x: 1 if x > 0 else 0)
    
    # 分离出目标为1的样本以及目标为0的样本
    positive_samples = freeDf[freeDf['target'] == 1]
    negative_samples = freeDf[freeDf['target'] == 0].sample(frac=0.1, random_state=0)
    
    # 合并进行采样后的数据
    sampled_df = pd.concat([positive_samples, negative_samples])
    
    # 选取特征列
    X = sampled_df[['heroLevelUp', 'appLaunch', 'login', 'payAction', 'plunder', 'goldCost', 'onlineTime', 'mainLevel', 'radar']]
    
    # 重新选取目标列
    y = sampled_df['target']

    # 将数据集分为训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
    
    # 初始化随机森林分类器，设置 class_weight='balanced' 或者定制权重
    n_estimators = 100  # 设定树的数量
    rf_clf = RandomForestClassifier(n_estimators=0, random_state=0, warm_start=True, n_jobs=-1, class_weight={0: 1, 1: 10})  # 使付费用户权重更高
    
    # 增量增加树并逐步训练，每训练10次进行一次评估
    for i in range(n_estimators):
        rf_clf.n_estimators = i + 1
        rf_clf.fit(X_train, y_train)
        
        if (i + 1) % 10 == 0:
            print(f'Trained {i + 1}/{n_estimators} trees')

            # 在训练集上预测
            y_train_pred = rf_clf.predict(X_train)
            
            # 在测试集上预测
            y_test_pred = rf_clf.predict(X_test)
            
            # 输出训练集表现
            print("Training Performance:")
            print(classification_report(y_train, y_train_pred))
            print('Training Accuracy:', accuracy_score(y_train, y_train_pred))

            # 输出测试集表现
            print("Test Performance:")
            print(classification_report(y_test, y_test_pred))
            print('Test Accuracy:', accuracy_score(y_test, y_test_pred))
  
def classify_and_evaluate3():    
    # 加载数据并筛选特征和目标列    
    df = pd.read_csv('/src/data/lwData20240530.csv')    
        
    # 只分析24小时内不付费用户  
    freeDf = df.loc[df['payNew24'] == 0]  
        
    # 选取目标列并转换为二分类问题  
    freeDf['target'] = freeDf['payNew168'].apply(lambda x: 1 if x > 0 else 0)  
        
    # 分离出目标为1的样本以及目标为0的样本  
    positive_samples = freeDf[freeDf['target'] == 1]  
    negative_samples = freeDf[freeDf['target'] == 0].sample(frac=0.1, random_state=0)  
        
    # 合并进行采样后的数据  
    sampled_df = pd.concat([positive_samples, negative_samples])  
    
    # 选取特征列  
    X = sampled_df[['heroLevelUp', 'appLaunch', 'login', 'payAction', 'plunder', 'goldCost', 'onlineTime', 'mainLevel', 'radar']]  
        
    # 重新选取目标列  
    y = sampled_df['target']  
        
    # 将数据集分为训练集和测试集  
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)  
  
    # 定义随机森林分类器，使用class_weight来增强偏向查全率  
    rf_clf = RandomForestClassifier(n_estimators=100, random_state=0, class_weight={0: 1, 1: 10}, n_jobs=-1)  
  
    # 定义网格搜索以优化模型参数  
    param_grid = {  
        'n_estimators': [100, 200],  
        'max_depth': [10, 20, None],  
        'min_samples_split': [2, 10],  
        'min_samples_leaf': [1, 4],  
    }  
  
    # 使用查全率作为主要评分标准  
    scorer = make_scorer(recall_score)  
  
    grid_search = GridSearchCV(rf_clf, param_grid, scoring=scorer, cv=5)  
    grid_search.fit(X_train, y_train)  
  
    rf_clf_best = grid_search.best_estimator_  
  
    # 在训练集和测试集上进行预测  
    y_train_pred_proba = rf_clf_best.predict_proba(X_train)[:, 1]  
    y_train_pred = (y_train_pred_proba > 0.3).astype(int)  
  
    y_test_pred_proba = rf_clf_best.predict_proba(X_test)[:, 1]  
    y_test_pred = (y_test_pred_proba > 0.3).astype(int)  
  
    # 输出训练集表现  
    print("Training Performance:")  
    print(classification_report(y_train, y_train_pred))  
    print('Training Recall:', recall_score(y_train, y_train_pred))  
  
    # 输出测试集表现  
    print("Test Performance:")  
    print(classification_report(y_test, y_test_pred))  
    print('Test Recall:', recall_score(y_test, y_test_pred))  
      
# 运行函数
# classify_and_evaluate()


def load_and_prepare_data(file_path):
    # 加载数据并筛选特征和目标列
    df = pd.read_csv(file_path)
    
    # 只分析24小时内不付费用户
    freeDf = df.loc[(df['payNew24'] == 0)]
    
    # 选取目标列并转换为二分类问题
    freeDf['target'] = freeDf['payNew168'].apply(lambda x: 1 if x > 0 else 0)
    
    # 分离出目标为1的样本以及目标为0的样本
    positive_samples = freeDf[freeDf['target'] == 1]
    negative_samples = freeDf[freeDf['target'] == 0].sample(frac=1, random_state=0)
    
    # 合并进行采样后的数据
    sampled_df = pd.concat([positive_samples, negative_samples])

    # 选取特征列
    X = sampled_df[['heroLevelUp', 'appLaunch', 'login', 'payAction', 'plunder', 'goldCost', 'onlineTime', 'mainLevel', 'radar']]
    
    # 重新选取目标列
    y = sampled_df['target']
    
    return X, y


def calculate_point_biserial_correlation(X, y):
    correlations = {}
    for column in X.columns:
        correlation, _ = pointbiserialr(y, X[column])
        correlations[column] = correlation
        print(f'Correlation between {column} and target: {correlation:.2f}')
    return correlations

def calculate_combination_correlations(X, y):
    combinations_correlations = {}
    for (col1, col2) in combinations(X.columns, 2):
        new_feature = X[col1] * X[col2]
        correlation, _ = pointbiserialr(y, new_feature)
        combinations_correlations[f'{col1}*{col2}'] = correlation
        print(f'Correlation between {col1}*{col2} and target: {correlation:.2f}')
    return combinations_correlations

def evaluate_kmeans(X, y):
    kmeans = KMeans(n_clusters=2, random_state=0)
    kmeans.fit(X)
    y_pred = kmeans.predict(X)
    print(f'K-Means Accuracy: {accuracy_score(y, y_pred):.2f}')
    print(classification_report(y, y_pred))
    return kmeans

def find_thresholds(X, y):
    thresholds = {}
    for column in X.columns:
        best_threshold = None
        best_accuracy = 0
        for threshold in X[column].unique():
            y_pred = (X[column] >= threshold).astype(int)
            accuracy = accuracy_score(y, y_pred)
            if accuracy > best_accuracy:
                best_accuracy = accuracy
                best_threshold = threshold
        thresholds[column] = best_threshold
        print(f'Best threshold for {column}: {best_threshold} with accuracy {best_accuracy:.2f}')
    return thresholds

def evaluate_feature_threshold(X, y):
    results = []
    for feature in X.columns:
        # 使用KMeans进行聚类
        kmeans = KMeans(n_clusters=2, random_state=0)
        kmeans.fit(X[[feature]])
        y_pred = kmeans.predict(X[[feature]])
        
        # 找到最佳阈值
        cluster_centers = kmeans.cluster_centers_.flatten()
        threshold = (cluster_centers[0] + cluster_centers[1]) / 2
        
        # 根据阈值进行分类
        y_pred_threshold = (X[feature] >= threshold).astype(int)
        
        # 计算查准率和查全率
        precision = precision_score(y, y_pred_threshold)
        recall = recall_score(y, y_pred_threshold)
        accuracy = accuracy_score(y, y_pred_threshold)
        
        results.append((feature, threshold, precision, recall, accuracy))
    
    return results

def evaluate_feature_threshold2(X, y):
    results = []
    for feature in X.columns:
        best_threshold = None
        best_precision = 0
        best_recall = 0
        best_accuracy = 0
        best_score = 0
        
        # 计算0.1分位数到0.9分位数的值
        quantiles = np.linspace(0.9, 0.999, 9)
        thresholds = X[feature].quantile(quantiles)
        
        for threshold in thresholds:
            # 根据阈值进行分类
            y_pred_threshold = (X[feature] >= threshold).astype(int)
            
            # 计算查准率和查全率
            precision = precision_score(y, y_pred_threshold)
            recall = recall_score(y, y_pred_threshold)
            accuracy = accuracy_score(y, y_pred_threshold)
            
            # 计算查准率和查全率的乘积
            score = precision * recall
            # score = precision

            print(f"Feature: {feature}, Threshold: {threshold:.2f}, Precision: {precision:.2f}, Recall: {recall:.2f}, Accuracy: {accuracy:.2f}")
            
            # 更新最佳阈值
            if score > best_score:
                best_threshold = threshold
                best_precision = precision
                best_recall = recall
                best_accuracy = accuracy
                best_score = score
        
        results.append((feature, best_threshold, best_precision, best_recall, best_accuracy))
    
    return results
# 主程序
if __name__ == "__main__":
    file_path = '/src/data/lwData20240530.csv'
    
    # 加载和准备数据
    print('Loading and preparing data...')
    X, y = load_and_prepare_data(file_path)
    print('Data loaded and prepared.')
    # # 计算并打印点双列相关系数
    # print('Calculating point-biserial correlation...')
    # calculate_point_biserial_correlation(X, y)

    # print("\nCombination Feature Correlations:")
    # calculate_combination_correlations(X, y)

    # print("\nEvaluating K-Means Clustering:")
    # kmeans_model = evaluate_kmeans(X, y)
    
    # print("\nFinding Best Thresholds for Each Feature:")
    # thresholds = find_thresholds(X, y)

    results = evaluate_feature_threshold2(X, y)
    
    for feature, threshold, precision, recall, accuracy in results:
        print(f"Feature: {feature}, Threshold: {threshold:.2f}, Precision: {precision:.2f}, Recall: {recall:.2f}, Accuracy: {accuracy:.2f}")