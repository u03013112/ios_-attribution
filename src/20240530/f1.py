import pandas as pd
import numpy as np
from sklearn.metrics import precision_score, recall_score, f1_score

def getData():
    df = pd.read_csv('/src/data/lwData20240530.csv')    
        
    # 只分析24小时内不付费用户  
    freeDf = df.loc[df['payNew24'] == 0]  
        
    # 选取目标列并转换为二分类问题  
    freeDf['target'] = freeDf['payNew168'].apply(lambda x: 1 if x > 0 else 0)  
        
    # X = freeDf[['heroLevelUp', 'appLaunch', 'login', 'payAction', 'plunder', 'goldCost', 'onlineTime', 'mainLevel', 'radar']]  
    X = freeDf[['heroLevelUp', 'appLaunch', 'login', 'payAction', 'onlineTime', 'mainLevel', 'radar']]
    y = freeDf['target']

    return X, y

def func1():
    X, y = getData()
    
    # 定义阈值分位数
    quantiles = [0.8, 0.9, 0.99]
    
    # 存储每个特征的最佳阈值和对应的查准率、查全率、F1分数
    best_thresholds = {}
    
    for feature in X.columns:
        best_f1 = 0
        best_threshold = None
        best_precision = 0
        best_recall = 0
        
        for q in quantiles:
            threshold = X[feature].quantile(q)
            y_pred = (X[feature] >= threshold).astype(int)
            
            precision = precision_score(y, y_pred)
            recall = recall_score(y, y_pred)
            f1 = f1_score(y, y_pred)
            
            print(f"Feature: {feature}, Quantiles:{q}, Threshold: {threshold:.2f}, Precision: {precision:.2f}, Recall: {recall:.2f}, F1: {f1:.2f}")
            if f1 > best_f1:
                best_f1 = f1
                best_threshold = threshold
                best_precision = precision
                best_recall = recall
        
        best_thresholds[feature] = {
            'threshold': best_threshold,
            'precision': best_precision,
            'recall': best_recall,
            'f1': best_f1
        }
    
    # 设定F1分数的阈值
    f1_threshold = 0.1
    
    # 选择效果较好的特征
    selected_features = [feature for feature, metrics in best_thresholds.items() if metrics['f1'] > f1_threshold]
    
    # 组合特征进行分类
    combined_pred = np.ones(len(y), dtype=int)
    for feature in selected_features:
        threshold = best_thresholds[feature]['threshold']
        combined_pred &= (X[feature] >= threshold).astype(int)
    
    # 计算组合分类的查准率、查全率和F1分数
    combined_precision = precision_score(y, combined_pred)
    combined_recall = recall_score(y, combined_pred)
    combined_f1 = f1_score(y, combined_pred)
    
    print("Selected Features and their thresholds:")
    for feature in selected_features:
        print(f"{feature}: {best_thresholds[feature]['threshold']}")
    
    print(f"Combined Precision: {combined_precision:.2f}")
    print(f"Combined Recall: {combined_recall:.2f}")
    print(f"Combined F1: {combined_f1:.2f}")

def func1quick():
    X, y = getData()
    
    # 预设的特征和阈值
    selected_features = {
        'heroLevelUp': 170.0,
        'login': 6.0,
        'onlineTime': 5721.927600000001,
        'mainLevel': 9.0,
        'radar': 32.0
    }
    
    # 计算每个样本满足条件的特征数
    condition_count = np.zeros(len(y), dtype=int)
    for feature, threshold in selected_features.items():
        condition_count += (X[feature] >= threshold).astype(int)
    
    # 当满足条件的特征数大于等于2时，预测为付费用户
    combined_pred = (condition_count >= 3).astype(int)
    
    # 计算组合分类的查准率、查全率和F1分数
    combined_precision = precision_score(y, combined_pred)
    combined_recall = recall_score(y, combined_pred)
    combined_f1 = f1_score(y, combined_pred)
    
    print("Selected Features and their thresholds:")
    for feature, threshold in selected_features.items():
        print(f"{feature}: {threshold}")
    
    print(f"Combined Precision: {combined_precision:.2f}")
    print(f"Combined Recall: {combined_recall:.2f}")
    print(f"Combined F1: {combined_f1:.2f}")

if __name__ == "__main__":
    func1quick()
