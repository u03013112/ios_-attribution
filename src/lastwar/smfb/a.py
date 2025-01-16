import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import precision_score, recall_score, f1_score

def getScoreData():
    filename = 'score.csv'
    df = pd.read_csv(filename)

    # wk 是类似 ‘2024-12-30 00:00:00.000’ 的字符串，改为日期格式
    df['wk'] = pd.to_datetime(df['wk'])

    df.rename(columns={'groupa':'group'},inplace=True)

    df['group'] = df['group'].astype(int)
    df['enemygroup'] = df['enemygroup'].astype(int)

    df['score'] = df['score'].astype(int)
    df['enemy_score'] = df['enemy_score'].astype(int)

    return df

def getStrengthAndScore():
    filename = '沙漠风暴 匹配分数 与 得分_20250110.csv'
    df = pd.read_csv(filename)

    df['wk'] = pd.to_datetime(df['wk'])

    return df

def checkStrengthAndScore():
    df = getStrengthAndScore()
    
    df = df.loc[df['wk'] == '2024-11-25']
    # df = df.loc[df['wk'] == '2024-12-02']
    df = df.loc[df['is_quality'] == 1]
    print(df.head())
    print('len(df):',len(df))


def getCorrBetweenStrengthAndScore():
    df = getStrengthAndScore()
    df = df[['strength_a','score_a','strength_b','score_b']]
    df0 = df[['strength_a','score_a']]
    df0.rename(columns={'strength_a':'strength','score_a':'score'},inplace=True)
    df1 = df[['strength_b','score_b']]
    df1.rename(columns={'strength_b':'strength','score_b':'score'},inplace=True)
    df = pd.concat([df0,df1],ignore_index=True)
    corr = df.corr()
    print(corr)
# getCorrBetweenStrengthAndScore 结果：
#           strength     score
# strength  1.000000  0.088845
# score     0.088845  1.000000
# 可以看出 strength 和 score 的相关性很低，符合预期

# 匹配分数与得分的差异比例的相关系数
def getCorrBetweenStrengthAndScore2():
    df = getStrengthAndScore()
    df['strength_diff_ratio'] = np.abs(df['strength_a'] - df['strength_b']) / df['strength_a']
    df['score_diff_ratio'] = np.abs(df['score_a'] - df['score_b']) / df['score_a']
    corr = df[['strength_diff_ratio','score_diff_ratio']].corr()
    print(corr)

def drawStrengthAndScore():
    df = getStrengthAndScore()
    df['strength_diff'] = np.abs(df['strength_a'] - df['strength_b'])
    df['score_diff'] = np.abs(df['score_a'] - df['score_b'])
    df['strength_min'] = df[['strength_a','strength_b']].min(axis=1)
    df['score_min'] = df[['score_a','score_b']].min(axis=1)

    # 将一些极端值抹平，将strength_diff和score_diff 的 99%分位数以上的值设置为99%分位数
    strength_diff_quantile = np.percentile(df['strength_diff'], 99)
    score_diff_quantile = np.percentile(df['score_diff'], 99)
    print('strength_diff_quantile:',strength_diff_quantile)
    print('score_diff_quantile:',score_diff_quantile)
    
    df.loc[df['strength_diff'] > strength_diff_quantile,'strength_diff'] = strength_diff_quantile
    df.loc[df['score_diff'] > score_diff_quantile,'score_diff'] = score_diff_quantile

    df['strength_diff_ratio'] = df['strength_diff'] / df['strength_min']
    df['score_diff_ratio'] = df['score_diff'] / df['score_min']

    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=['strength_diff_ratio', 'score_diff_ratio'])

    # 将一些极端值抹平
    strength_diff_ratio_quantile = np.percentile(df['strength_diff_ratio'], 99)
    score_diff_ratio_quantile = np.percentile(df['score_diff_ratio'], 99)
    print('strength_diff_ratio_quantile:',strength_diff_ratio_quantile)
    print('score_diff_ratio_quantile:',score_diff_ratio_quantile)

    df.loc[df['strength_diff_ratio'] > strength_diff_ratio_quantile,'strength_diff_ratio'] = strength_diff_ratio_quantile
    df.loc[df['score_diff_ratio'] > score_diff_ratio_quantile,'score_diff_ratio'] = score_diff_ratio_quantile
    
    # 画一张图，上下两个小图
    # 上面一张，先对strength_diff_ratio按照10%取整，score_diff_ratio按照分组取平均值，is_quality按照分组取平均值
    #  x 是 strength_diff_ratio，
    # 双y轴， 1个是 score_diff_ratio，1个是 is_quality
    # 按照strength_diff_ratio 升序排列

    # 下面一张 ，先对 strength_diff 按10%分位数取整，score_diff按照分组取平均值，is_quality按照分组取平均值
    # x 是 strength_diff，
    # 双y轴，1个是 score_diff，1个是 is_quality
    # 按照strength_diff 升序排列
    # 保存到文件 '/src/data/smfb_drawStrengthAndScore.png'

    # 上图数据处理
    df['strength_diff_ratio_bin'] = (df['strength_diff_ratio'] * 40).astype(int) / 40.0
    df_upper = df.groupby('strength_diff_ratio_bin').agg({
        'score_diff_ratio': 'mean',
        'is_quality': 'mean'
    }).reset_index()

    # 下图数据处理
    quantiles = np.percentile(df['strength_diff'], np.arange(0, 110, 10))
    df['strength_diff_bin'] = pd.cut(df['strength_diff'], bins=quantiles, include_lowest=True)
    df.head(100).to_csv('/src/data/smfb_drawStrengthAndScore_df.csv',index=False)
    df_lower = df.groupby('strength_diff_bin').agg({
        'score_diff': 'mean',
        'is_quality': 'mean'
    }).reset_index()

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

    # 上图数据处理
    df['strength_diff_ratio_bin'] = (df['strength_diff_ratio'] * 20).astype(int) / 20.0  # 每5%分一组
    df_upper = df.groupby('strength_diff_ratio_bin').agg({
        'score_diff_ratio': 'mean',
        'is_quality': 'mean'
    }).reset_index()

    # 中图数据处理
    quantiles = np.percentile(df['strength_diff'], np.arange(0, 110, 10))  # 包括100%的分位数
    print('quantiles:', quantiles)
    df['strength_diff_bin'] = pd.cut(df['strength_diff'], bins=quantiles, include_lowest=True, duplicates='drop')
    df.head(100).to_csv('/src/data/smfb_drawStrengthAndScore_df.csv', index=False)
    df_middle = df.groupby('strength_diff_bin').agg({
        'score_diff': 'mean',
        'is_quality': 'mean'
    }).reset_index()

    # 下图数据处理
    df_lower = df.groupby('strength_diff_bin').agg({
        'strength_diff_ratio': 'mean'
    }).reset_index()

    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 15))

    # 上图
    ax1_1 = ax1
    ax1_2 = ax1.twinx()
    ax1_1.plot(df_upper['strength_diff_ratio_bin'], df_upper['score_diff_ratio'], 'g-', label='Score Diff Ratio')
    ax1_2.plot(df_upper['strength_diff_ratio_bin'], df_upper['is_quality'], 'b-', label='Is Quality')

    ax1_1.set_xlabel('Strength Diff Ratio')
    ax1_1.set_ylabel('Score Diff Ratio', color='g')
    ax1_2.set_ylabel('Is Quality', color='b')

    ax1_1.legend(loc='upper left')
    ax1_2.legend(loc='upper right')

    # 中图
    ax2_1 = ax2
    ax2_2 = ax2.twinx()
    ax2_1.plot(df_middle['strength_diff_bin'].astype(str), df_middle['score_diff'], 'g-', label='Score Diff')
    ax2_2.plot(df_middle['strength_diff_bin'].astype(str), df_middle['is_quality'], 'b-', label='Is Quality')

    ax2_1.set_xlabel('Strength Diff')
    ax2_1.set_ylabel('Score Diff', color='g')
    ax2_2.set_ylabel('Is Quality', color='b')

    ax2_1.legend(loc='upper left')
    ax2_2.legend(loc='upper right')

    # 下图
    ax3.plot(df_lower['strength_diff_bin'].astype(str), df_lower['strength_diff_ratio'], 'r-', label='Strength Diff Ratio Mean')
    ax3.set_xlabel('Strength Diff')
    ax3.set_ylabel('Strength Diff Ratio Mean', color='r')
    ax3.legend(loc='upper left')

    plt.tight_layout()
    plt.savefig('/src/data/smfb_drawStrengthAndScore.png')

def drawStrengthAndScore2():
    df = getStrengthAndScore()
    print('raw len(df):', len(df))
    df['strength_diff'] = np.abs(df['strength_a'] - df['strength_b'])
    df['score_diff'] = np.abs(df['score_a'] - df['score_b'])

    # 将一些极端值抹平，将strength_diff和score_diff 的 99%分位数以上的值设置为99%分位数
    strength_diff_quantile = np.percentile(df['strength_diff'], 99)
    score_diff_quantile = np.percentile(df['score_diff'], 99)
    print('strength_diff_quantile:', strength_diff_quantile)
    print('score_diff_quantile:', score_diff_quantile)
    
    df.loc[df['strength_diff'] > strength_diff_quantile, 'strength_diff'] = strength_diff_quantile
    df.loc[df['score_diff'] > score_diff_quantile, 'score_diff'] = score_diff_quantile

    df['strength_diff_ratio'] = df['strength_diff'] / df['strength_a']
    df['score_diff_ratio'] = df['score_diff'] / df['score_a']

    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=['strength_diff_ratio', 'score_diff_ratio'])

    print('dropna len(df):', len(df))

    # 将一些极端值抹平
    strength_diff_ratio_quantile = np.percentile(df['strength_diff_ratio'], 99)
    score_diff_ratio_quantile = np.percentile(df['score_diff_ratio'], 99)
    print('strength_diff_ratio_quantile:', strength_diff_ratio_quantile)
    print('score_diff_ratio_quantile:', score_diff_ratio_quantile)

    df.loc[df['strength_diff_ratio'] > strength_diff_ratio_quantile, 'strength_diff_ratio'] = strength_diff_ratio_quantile
    df.loc[df['score_diff_ratio'] > score_diff_ratio_quantile, 'score_diff_ratio'] = score_diff_ratio_quantile
    
    # 上图数据处理
    df['strength_diff_ratio_bin'] = (df['strength_diff_ratio'] * 50).astype(int) / 50.0
    df_upper = df.groupby('strength_diff_ratio_bin').agg({
        'score_diff_ratio': 'mean',
        'is_quality': 'mean'
    }).reset_index()

    fig, ax1 = plt.subplots(figsize=(12, 5))

    # 上图
    ax1_1 = ax1
    ax1_2 = ax1.twinx()
    ax1_1.plot(df_upper['strength_diff_ratio_bin'], df_upper['score_diff_ratio'], 'g-', label='Score Diff Ratio')
    ax1_2.plot(df_upper['strength_diff_ratio_bin'], df_upper['is_quality'], 'b-', label='Is Quality')

    ax1_1.set_xlabel('Strength Diff Ratio')
    ax1_1.set_ylabel('Score Diff Ratio', color='g')
    ax1_2.set_ylabel('Is Quality', color='b')

    ax1_1.legend(loc='upper left')
    ax1_2.legend(loc='upper right')

    plt.tight_layout()
    plt.savefig('/src/data/smfb_drawStrengthAndScore2.png')
    plt.close()

def getCorrBetweenStrengthAndScore3():
    df = getStrengthAndScore()
    print('raw len(df):', len(df))
    df['strength_diff'] = np.abs(df['strength_a'] - df['strength_b'])
    df['score_diff'] = np.abs(df['score_a'] - df['score_b'])

    # 将一些极端值抹平，将strength_diff和score_diff 的 99%分位数以上的值设置为99%分位数
    strength_diff_quantile = np.percentile(df['strength_diff'], 99)
    score_diff_quantile = np.percentile(df['score_diff'], 99)
    print('strength_diff_quantile:', strength_diff_quantile)
    print('score_diff_quantile:', score_diff_quantile)
    
    df.loc[df['strength_diff'] > strength_diff_quantile, 'strength_diff'] = strength_diff_quantile
    df.loc[df['score_diff'] > score_diff_quantile, 'score_diff'] = score_diff_quantile

    df['strength_diff_ratio'] = df['strength_diff'] / df['strength_a']
    df['score_diff_ratio'] = df['score_diff'] / df['score_a']

    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=['strength_diff_ratio', 'score_diff_ratio'])

    print('dropna len(df):', len(df))

    # 将一些极端值抹平
    strength_diff_ratio_quantile = np.percentile(df['strength_diff_ratio'], 99)
    score_diff_ratio_quantile = np.percentile(df['score_diff_ratio'], 99)
    print('strength_diff_ratio_quantile:', strength_diff_ratio_quantile)
    print('score_diff_ratio_quantile:', score_diff_ratio_quantile)

    df.loc[df['strength_diff_ratio'] > strength_diff_ratio_quantile, 'strength_diff_ratio'] = strength_diff_ratio_quantile
    df.loc[df['score_diff_ratio'] > score_diff_ratio_quantile, 'score_diff_ratio'] = score_diff_ratio_quantile
    
    corr = df[['strength_diff_ratio', 'score_diff_ratio','is_quality']].corr()
    print('逐条数据的相关系数：')
    print(corr)

    df['strength_diff_ratio_bin'] = (df['strength_diff_ratio'] * 50).astype(int) / 50.0
    df_upper = df.groupby('strength_diff_ratio_bin').agg({
        'score_diff_ratio': 'mean',
        'is_quality': 'mean'
    }).reset_index()

    df_upper.to_csv('/src/data/smfb_strength_diff_ratio_score_diff_ratio.csv',index=False)
    corr = df_upper[['strength_diff_ratio_bin', 'score_diff_ratio', 'is_quality']].corr()
    print('分组数据的相关系数：')
    print(corr)


def rocAndAUC():
    from sklearn.metrics import roc_curve, roc_auc_score

    df = getStrengthAndScore()
    
    # 计算 strength_diff_ratio
    df = df[['strength_a', 'strength_b', 'is_quality']]
    df['strength_diff_ratio'] = np.abs(df['strength_a'] - df['strength_b']) / df['strength_a']
    
    print(df.head())
    print('len(df):',len(df))
    # 删除包含 NaN 或无穷大值的行
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=['strength_diff_ratio'])
    print('dropna len(df):',len(df))
    # 计算 ROC 曲线
    fpr, tpr, thresholds = roc_curve(df['is_quality'], df['strength_diff_ratio'])
    
    # 计算 Youden's J 统计量
    youden_j = tpr - fpr
    best_threshold_index = np.argmax(youden_j)
    best_threshold = thresholds[best_threshold_index]
    
    # 使用最佳阈值计算 AUC
    y_pred = (df['strength_diff_ratio'] < best_threshold).astype(int)
    auc = roc_auc_score(df['is_quality'], y_pred)
    
    print(f"最佳阈值: {best_threshold}")
    print(f"AUC: {auc}")

def evaluateStrengthWithMetrics():
    df = getStrengthAndScore()
    df['strength_diff'] = np.abs(df['strength_a'] - df['strength_b'])
    df['score_diff'] = np.abs(df['score_a'] - df['score_b'])

    # 将一些极端值抹平，将strength_diff和score_diff 的 99%分位数以上的值设置为99%分位数
    strength_diff_quantile = np.percentile(df['strength_diff'], 99)
    score_diff_quantile = np.percentile(df['score_diff'], 99)
    
    df.loc[df['strength_diff'] > strength_diff_quantile, 'strength_diff'] = strength_diff_quantile
    df.loc[df['score_diff'] > score_diff_quantile, 'score_diff'] = score_diff_quantile

    df['strength_diff_ratio'] = df['strength_diff'] / df['strength_a']
    df['score_diff_ratio'] = df['score_diff'] / df['score_a']

    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=['strength_diff_ratio', 'score_diff_ratio'])

    # 将一些极端值抹平
    strength_diff_ratio_quantile = np.percentile(df['strength_diff_ratio'], 99)
    score_diff_ratio_quantile = np.percentile(df['score_diff_ratio'], 99)

    df.loc[df['strength_diff_ratio'] > strength_diff_ratio_quantile, 'strength_diff_ratio'] = strength_diff_ratio_quantile
    df.loc[df['score_diff_ratio'] > score_diff_ratio_quantile, 'score_diff_ratio'] = score_diff_ratio_quantile
    
    # 定义阈值范围
    thresholds = np.linspace(0, 0.25, 26)
    results = []

    for threshold in thresholds:
        df['predicted_quality'] = df['strength_diff_ratio'] < threshold
        precision = precision_score(df['is_quality'], df['predicted_quality'])
        recall = recall_score(df['is_quality'], df['predicted_quality'])
        f1 = f1_score(df['is_quality'], df['predicted_quality'])
        results.append({
            'threshold': threshold,
            'precision': precision,
            'recall': recall,
            'f1': f1
        })

    results_df = pd.DataFrame(results)
    results_df.to_csv('/src/data/strength_evaluation_metrics.csv', index=False)
    print('结果已保存到 strength_evaluation_metrics.csv')

    return results_df

def main():
    # checkStrengthAndScore()
    # getCorrBetweenStrengthAndScore()
    # getCorrBetweenStrengthAndScore2()
    # getCorrBetweenStrengthAndScore3()
    # drawStrengthAndScore()
    # drawStrengthAndScore2()    
    # rocAndAUC()

    evaluateStrengthWithMetrics()

if __name__ == '__main__':
    main()