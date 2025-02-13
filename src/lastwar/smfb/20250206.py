# 新的预测是否出战规则
# 历史3场得分均值 大于等于3，即预测出战，否则不出战。

# 并尝试启用新的匹配分计算规则
# 个人匹配分=最强的3个编队战力 对应评分求和
# 单编队战力（M）,对应评分(60,467)，(58,467),(56,363),(54,311),(52,207),(50,173),(48,144),(46,115),(44,86),(42,72),(40,60),(38,50),(36,40),(34,30),(32,20),(30,10),(28,5),(26,2),(24,1),(22,1),(20,1);
import pandas as pd
import numpy as np
from sklearn.metrics import precision_score, recall_score, f1_score
import matplotlib.pyplot as plt

# 简单版本
# 不做极值处理
# 将不能处理的数据直接丢弃，这会导致数据量减少
def evaluateStrengthWithMetrics():
    df = pd.read_csv('/src/data/20250206smfb_data_20241125_20250205.csv')
    # df = pd.read_csv('/src/data/20250206smfb_data_s_20241125_20250205.csv')
    # df = pd.read_csv('/src/data/20250206smfb_data_s2_20241125_20250205.csv')

    df['wk'] = pd.to_datetime(df['wk'])
    df = df[(df['wk'] >= '2024-12-31') & (df['wk'] <= '2025-01-27')]

    # df.rename(columns={
    #     'strength_old_a': 'strength_a',
    #     'strength_old_b': 'strength_b'
    # }, inplace=True)

    df.rename(columns={
        'strength_new_a': 'strength_a',
        'strength_new_b': 'strength_b'
    }, inplace=True)

    df['strength_diff'] = np.abs(df['strength_a'] - df['strength_b'])
    df['score_diff'] = np.abs(df['score_a'] - df['score_b'])

    min_strength = np.minimum(df['strength_a'], df['strength_b'])
    df['strength_diff_ratio'] = df['strength_diff'] / min_strength

    min_score = np.minimum(df['score_a'], df['score_b'])
    df['score_diff_ratio'] = df['score_diff'] / min_score

    print('共计', len(df), '条数据')
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=['strength_diff_ratio', 'score_diff_ratio'])
    print('去除inf和nan后，剩余', len(df), '条数据')

    # 定义阈值范围
    thresholds = np.linspace(0, 1.5, 16)
    results = []

    for threshold in thresholds:
        df['predicted_quality'] = df['strength_diff_ratio'] < threshold
        precision = precision_score(df['is_quality'], df['predicted_quality'])
        recall = recall_score(df['is_quality'], df['predicted_quality'])
        f1 = f1_score(df['is_quality'], df['predicted_quality'])
        
        # 计算预测的正样本和负样本数量
        positive_predictions = df['predicted_quality'].sum()
        negative_predictions = len(df) - positive_predictions
        positive_ratio = positive_predictions / len(df)
        
        results.append({
            'threshold': threshold,
            'precision': precision,
            'recall': recall,
            'f1': f1,
            'positive_predictions': positive_predictions,
            'negative_predictions': negative_predictions,
            'positive_ratio': positive_ratio
        })

    results_df = pd.DataFrame(results)
    results_df.to_csv('/src/data/20250206strength_evaluation_metrics.csv', index=False)
    print('结果已保存到 20250206strength_evaluation_metrics.csv')

    print(results_df)

    return results_df

# 复杂版本
# 对不能处理的数据进行处理，将0匹配分改为0.1，将0分改为0.1
# 这使得匹配分和评分的差异比例不会无穷大，但是应该是个非常大的数，大概率会被预测为质量差的比赛
# 极值处理在这里有，但是可能被注释掉
def evaluateStrengthWithMetrics2():
    # df = pd.read_csv('/src/data/20250206smfb_data_20241125_20250205.csv')
    # df = pd.read_csv('/src/data/20250206smfb_data_server_id_20241125_20250205.csv')
    df = pd.read_csv('/src/data/20250206smfb_data_s_20241125_20250205.csv')
    # df = pd.read_csv('/src/data/20250206smfb_data_s2_20241125_20250205.csv')
    # df = pd.read_csv('/src/data/20250206smfb_data_s2t_20241125_20250205.csv')

    df['wk'] = pd.to_datetime(df['wk'])
    df = df[(df['wk'] >= '2024-12-31') & (df['wk'] <= '2025-01-27')]

    df.rename(columns={
        'strength_old_a': 'strength_a',
        'strength_old_b': 'strength_b'
    }, inplace=True)

    # df.rename(columns={
    #     'strength_new_a': 'strength_a',
    #     'strength_new_b': 'strength_b'
    # }, inplace=True)

    df['strength_diff'] = np.abs(df['strength_a'] - df['strength_b'])
    df['score_diff'] = np.abs(df['score_a'] - df['score_b'])

    # 将一些极端值抹平，将strength_diff和score_diff 的 99%分位数以上的值设置为99%分位数
    strength_diff_quantile = np.percentile(df['strength_diff'], 99)
    score_diff_quantile = np.percentile(df['score_diff'], 99)

    # df.loc[df['strength_diff'] > strength_diff_quantile, 'strength_diff'] = strength_diff_quantile
    # df.loc[df['score_diff'] > score_diff_quantile, 'score_diff'] = score_diff_quantile

    print('strength_diff 99%分位数:', strength_diff_quantile)
    print('score_diff 99%分位数:', score_diff_quantile)
    print('strength_diff 99%分位数以上的数据已被抹平')
    print('score_diff 99%分位数以上的数据已被抹平')

    min_strength = np.minimum(df['strength_a'], df['strength_b'])
    min_strength[min_strength == 0] = 0.1  # 将除数为0的情况改为0.1
    df['strength_diff_ratio'] = df['strength_diff'] / min_strength

    min_score = np.minimum(df['score_a'], df['score_b'])
    min_score[min_score == 0] = 0.1  # 将除数为0的情况改为0.1
    df['score_diff_ratio'] = df['score_diff'] / min_score

    # df['predicted_quality'] = True
    # df.loc[np.isinf(df['strength_diff_ratio']) | df['strength_diff_ratio'].isna(), 'predicted_quality'] = False
    df = df.replace([np.inf, -np.inf], np.nan)
    
    dropedDf = df[np.isinf(df['strength_diff_ratio']) | df['strength_diff_ratio'].isna()]
    dropedDf.to_csv('/src/data/20250206strength_evaluation_metrics_droped.csv', index=False)
    print('inf和nan的数据已保存到 20250206strength_evaluation_metrics_droped.csv')

    print('共计', len(df), '条数据')
    df = df.dropna(subset=['strength_diff_ratio', 'score_diff_ratio'])
    print('去除inf和nan后，剩余', len(df), '条数据')

    # 将一些极端值抹平
    strength_diff_ratio_quantile = np.percentile(df['strength_diff_ratio'], 99)
    score_diff_ratio_quantile = np.percentile(df['score_diff_ratio'], 99)

    df.loc[df['strength_diff_ratio'] > strength_diff_ratio_quantile, 'strength_diff_ratio'] = strength_diff_ratio_quantile
    df.loc[df['score_diff_ratio'] > score_diff_ratio_quantile, 'score_diff_ratio'] = score_diff_ratio_quantile
    
    # 定义阈值范围
    # thresholds = np.linspace(0, 2.5, 26)
    thresholds = np.linspace(0, 0.25, 26)
    results = []

    for threshold in thresholds:
        df['predicted_quality'] = df['strength_diff_ratio'] < threshold
        precision = precision_score(df['is_quality'], df['predicted_quality'])
        recall = recall_score(df['is_quality'], df['predicted_quality'])
        f1 = f1_score(df['is_quality'], df['predicted_quality'])
        
        # 计算预测的正样本和负样本数量
        positive_predictions = df['predicted_quality'].sum()
        negative_predictions = len(df) - positive_predictions
        positive_ratio = positive_predictions / len(df)
        
        results.append({
            'threshold': threshold,
            'precision': precision,
            'recall': recall,
            'f1': f1,
            'positive_predictions': positive_predictions,
            'negative_predictions': negative_predictions,
            'positive_ratio': positive_ratio
        })

    results_df = pd.DataFrame(results)
    results_df.to_csv('/src/data/20250206strength_evaluation_metrics.csv', index=False)
    print('结果已保存到 20250206strength_evaluation_metrics.csv')

    print(results_df)

    return results_df


# 保持统一的方向，
# 确保 strength_diff_ratio 和 score_diff_ratio 的计算方向一致，
# 并将 score_diff_ratio 出现负值的场次标记为非质量局。然后，计算不同阈值下的精度、召回率和 F1 分数
def evaluateStrengthWithMetrics3():
    # 读取数据
    # df = pd.read_csv('/src/data/20250206smfb_data_20241125_20250205.csv')
    # df = pd.read_csv('/src/data/20250206smfb_data_server_id_20241125_20250205.csv')
    df = pd.read_csv('/src/data/20250206smfb_data_s_20241125_20250205.csv')
    # df = pd.read_csv('/src/data/20250206smfb_data_s2_20241125_20250205.csv')
    # df = pd.read_csv('/src/data/20250206smfb_data_s2t_20241125_20250205.csv')

    df['wk'] = pd.to_datetime(df['wk'])
    df = df[(df['wk'] >= '2024-12-31') & (df['wk'] <= '2025-01-27')]

    # df.rename(columns={
    #     'strength_new_a': 'strength_a',
    #     'strength_new_b': 'strength_b'
    # }, inplace=True)

    df.rename(columns={
        'strength_old_a': 'strength_a',
        'strength_old_b': 'strength_b'
    }, inplace=True)

    # 分成两部分
    aDf = df[df['strength_a'] > df['strength_b']].copy()
    bDf = df[df['strength_a'] <= df['strength_b']].copy()

    
    # 调整bDf的列名
    bDf = bDf.rename(columns={
        'strength_a': 'strength_b',
        'strength_b': 'strength_a',
        'score_a': 'score_b',
        'score_b': 'score_a'
    })

    # 合并数据
    df = pd.concat([aDf, bDf], ignore_index=True)

    # 计算差值
    df['strength_diff'] = df['strength_a'] - df['strength_b']
    df['score_diff'] = df['score_a'] - df['score_b']

    # # 将一些极端值抹平，将strength_diff和score_diff 的 99%分位数以上的值设置为99%分位数
    # strength_diff_quantile = np.percentile(df['strength_diff'], 99)
    # score_diff_quantile = np.percentile(df['score_diff'], 99)

    # print('strength_diff 99%分位数:', strength_diff_quantile)
    # print('score_diff 99%分位数:', score_diff_quantile)
    # print('strength_diff 99%分位数以上的数据已被抹平')
    # print('score_diff 99%分位数以上的数据已被抹平')

    # 计算差值率
    df.loc[df['strength_b'] == 0, 'strength_b'] = 0.1  # 将除数为0的情况改为0.1
    df['strength_diff_ratio'] = df['strength_diff'] / df['strength_b']
    
    df.loc[df['score_b'] == 0, 'score_b'] = 0.1  # 将除数为0的情况改为0.1
    df['score_diff_ratio'] = df['score_diff'] / df['score_b']

    # 将score_diff_ratio出现负值的场次标记为非质量局
    df.loc[df['score_diff_ratio'] < 0, 'is_quality'] = 0

    # 处理inf和nan值
    df = df.replace([np.inf, -np.inf], np.nan)
    
    # dropedDf = df[np.isinf(df['strength_diff_ratio']) | df['strength_diff_ratio'].isna()]
    # dropedDf.to_csv('/src/data/20250206strength_evaluation_metrics_droped.csv', index=False)
    # print('inf和nan的数据已保存到 20250206strength_evaluation_metrics_droped.csv')

    print('共计', len(df), '条数据')
    df = df.dropna(subset=['strength_diff_ratio', 'score_diff_ratio'])
    print('去除inf和nan后，剩余', len(df), '条数据')

    # 将一些极端值抹平
    strength_diff_ratio_quantile = np.percentile(df['strength_diff_ratio'], 99)
    score_diff_ratio_quantile = np.percentile(df['score_diff_ratio'], 99)

    df.loc[df['strength_diff_ratio'] > strength_diff_ratio_quantile, 'strength_diff_ratio'] = strength_diff_ratio_quantile
    df.loc[df['score_diff_ratio'] > score_diff_ratio_quantile, 'score_diff_ratio'] = score_diff_ratio_quantile
    
    # 定义阈值范围
    # thresholds = np.linspace(0, 2.5, 26)
    thresholds = np.linspace(0, 0.25, 26)

    results = []

    for threshold in thresholds:
        df['predicted_quality'] = df['strength_diff_ratio'] < threshold
        precision = precision_score(df['is_quality'], df['predicted_quality'])
        recall = recall_score(df['is_quality'], df['predicted_quality'])
        f1 = f1_score(df['is_quality'], df['predicted_quality'])
        
        # 计算预测的正样本和负样本数量
        positive_predictions = df['predicted_quality'].sum()
        negative_predictions = len(df) - positive_predictions
        positive_ratio = positive_predictions / len(df)
        
        results.append({
            'threshold': threshold,
            'precision': precision,
            'recall': recall,
            'f1': f1,
            'positive_predictions': positive_predictions,
            'negative_predictions': negative_predictions,
            'positive_ratio': positive_ratio
        })

    results_df = pd.DataFrame(results)
    results_df.to_csv('/src/data/20250206strength_evaluation_metrics.csv', index=False)
    print('结果已保存到 20250206strength_evaluation_metrics.csv')

    print(results_df)

    return results_df


def corrBetweenstrengthDiffRatioAndscoreDiffRatio():
    df = pd.read_csv('/src/data/20250206smfb_data_20241125_20250205.csv')

    df['wk'] = pd.to_datetime(df['wk'])
    df = df[(df['wk'] >= '2024-12-31') & (df['wk'] <= '2025-01-27')]
    
    df = df[df['is_quality'] == 1]

    df.rename(columns={
        'strength_new_a': 'strength_a',
        'strength_new_b': 'strength_b'
    }, inplace=True)

    df['strength_diff'] = np.abs(df['strength_a'] - df['strength_b'])
    df['score_diff'] = np.abs(df['score_a'] - df['score_b'])

    # 将一些极端值抹平，将strength_diff和score_diff 的 99%分位数以上的值设置为99%分位数
    strength_diff_quantile = np.percentile(df['strength_diff'], 99)
    score_diff_quantile = np.percentile(df['score_diff'], 99)

    df.loc[df['strength_diff'] > strength_diff_quantile, 'strength_diff'] = strength_diff_quantile
    df.loc[df['score_diff'] > score_diff_quantile, 'score_diff'] = score_diff_quantile

    min_strength = np.minimum(df['strength_a'], df['strength_b'])
    df['strength_diff_ratio'] = df['strength_diff'] / min_strength

    min_score = np.minimum(df['score_a'], df['score_b'])
    df['score_diff_ratio'] = df['score_diff'] / min_score

    df = df.replace([np.inf, -np.inf], np.nan)    
    df = df.dropna(subset=['strength_diff_ratio', 'score_diff_ratio'])

    # 计算并打印相关系数
    corr = df['strength_diff_ratio'].corr(df['score_diff_ratio'])
    print('strength_diff_ratio 和 score_diff_ratio 的相关系数:', corr)


def test():
    df = pd.read_csv('/src/data/20250206smfb_data_s_20241125_20250205.csv')
    df['wk'] = pd.to_datetime(df['wk'])
    
    df = df[df['is_quality'] == 1]

    df = df.sort_values(by=['score_a'])

    df = df[['strength_old_a', 'strength_old_b', 'strength_new_a', 'strength_new_b', 'score_a', 'score_b']]

    print(df.head(10))

    print(df.tail(10))

def getCorrBetweenStrengthAndScore5():
    # df = pd.read_csv('/src/data/20250206smfb_data_20241125_20250205.csv')
    df = pd.read_csv('/src/data/20250206smfb_data_s_20241125_20250205.csv')
    df.rename(columns={
        'strength_new_a': 'strength_a',
        'strength_new_b': 'strength_b'
    }, inplace=True)

    df = df[df['is_quality'] == 1]

    # 将strength_a == 0 和 strength_b == 0 的数据去掉
    df = df[(df['strength_a'] != 0) & (df['strength_b'] != 0)]

    # 根据score_a和score_b确定胜负组
    win_group = df[df['score_a'] > df['score_b']][['strength_a', 'score_a']].rename(columns={'strength_a': 'strength', 'score_a': 'score'})
    win_group = win_group.append(df[df['score_b'] > df['score_a']][['strength_b', 'score_b']].rename(columns={'strength_b': 'strength', 'score_b': 'score'}))
    print('win_group len:', len(win_group))

    lose_group = df[df['score_a'] < df['score_b']][['strength_a', 'score_a']].rename(columns={'strength_a': 'strength', 'score_a': 'score'})
    lose_group = lose_group.append(df[df['score_b'] < df['score_a']][['strength_b', 'score_b']].rename(columns={'strength_b': 'strength', 'score_b': 'score'}))
    print('lose_group len:', len(lose_group))

    # 合并胜者组和败者组
    all_group = win_group.append(lose_group)
    print('all_group len:', len(all_group))

    def process_group(group, group_name):
        # 去除异常值
        strength_99 = group['strength'].quantile(0.99)
        score_99 = group['score'].quantile(0.99)
        
        group['strength'] = group['strength'].clip(upper=strength_99)
        group['score'] = group['score'].clip(upper=score_99)
        
        # 计算0%到100%的分位数
        N = 10
        quantiles = group['strength'].quantile([i/N for i in range(N+1)])
        
        # 按照strength进行分组
        group['strength_group'] = pd.cut(group['strength'], bins=quantiles, include_lowest=True, labels=False)
    
        # 计算每组中score的平均值
        grouped_df = group.groupby('strength_group')['score'].mean().reset_index()
        
        # 计算strength的分组分位数与分组内score均值的相关系数
        grouped_df['strength_quantile'] = quantiles.values[1:]  # 去掉第一个0%分位数
        final_corr = grouped_df[['strength_quantile', 'score']].corr()
        
        print(f'{group_name}组的strength分组分位数与分组内score均值的相关系数：')
        print(final_corr)
        
        # 绘制图表
        plt.figure(figsize=(10, 6))
        plt.plot(grouped_df['strength_quantile'], grouped_df['score'], marker='o')
        plt.xlabel('Strength Quantile')
        plt.ylabel('Average Score')
        plt.title(f'Average Score by Strength Quantile ({group_name} Group)')
        plt.grid(True)
        plt.savefig(f'/src/data/pic_{group_name}.png')
    
    # 处理战胜组
    process_group(win_group, 'win')
    
    # 处理战败组
    process_group(lose_group, 'lose')
    
    # 处理全部组
    process_group(all_group, 'all')

# 用于计算strength_diff_rate分组分位数与分组内score_diff_rate均值的相关系数
def getCorrBetweenStrengthAndScore6():
    # df = pd.read_csv('/src/data/20250206smfb_data_20241125_20250205.csv')
    df = pd.read_csv('/src/data/20250206smfb_data_s_20241125_20250205.csv')

    df.rename(columns={
        'strength_new_a': 'strength_a',
        'strength_new_b': 'strength_b'
    }, inplace=True)

    df = df[df['is_quality'] == 1]

    # 将strength_a == 0 和 strength_b == 0 的数据去掉
    df = df[(df['strength_a'] != 0) & (df['strength_b'] != 0)]

    # 计算strength和score的差值率
    df['strength_diff_rate'] = (df[['strength_a', 'strength_b']].max(axis=1) - df[['strength_a', 'strength_b']].min(axis=1)) / df[['strength_a', 'strength_b']].min(axis=1)
    df['score_diff_rate'] = (df[['score_a', 'score_b']].max(axis=1) - df[['score_a', 'score_b']].min(axis=1)) / df[['score_a', 'score_b']].min(axis=1)

    # 去除异常值
    strength_diff_rate_99 = df['strength_diff_rate'].quantile(0.99)
    score_diff_rate_99 = df['score_diff_rate'].quantile(0.99)
    
    df['strength_diff_rate'] = df['strength_diff_rate'].clip(upper=strength_diff_rate_99)
    df['score_diff_rate'] = df['score_diff_rate'].clip(upper=score_diff_rate_99)

    # 计算0%到100%的分位数
    N = 10
    quantiles = df['strength_diff_rate'].quantile([i/N for i in range(N+1)])
    
    # 按照strength_diff_rate进行分组
    df['strength_diff_rate_group'] = pd.cut(df['strength_diff_rate'], bins=quantiles, include_lowest=True, labels=False)

    # 计算每组中score_diff_rate的平均值
    grouped_df = df.groupby('strength_diff_rate_group')['score_diff_rate'].mean().reset_index()
    
    # 计算strength_diff_rate的分组分位数与分组内score_diff_rate均值的相关系数
    grouped_df['strength_diff_rate_quantile'] = quantiles.values[1:]  # 去掉第一个0%分位数
    grouped_df = grouped_df[['strength_diff_rate_quantile', 'score_diff_rate']]
    # 打印用于计算相关系数和绘图的数据
    print('用于计算相关系数和绘图的数据：')
    print(grouped_df)

    final_corr = grouped_df.corr()
    
    print('strength_diff_rate分组分位数与分组内score_diff_rate均值的相关系数：')
    print(final_corr)
    
    # 绘制图表
    plt.figure(figsize=(10, 6))
    plt.plot(grouped_df['strength_diff_rate_quantile'], grouped_df['score_diff_rate'], marker='o')
    plt.xlabel('Strength Diff Rate Quantile')
    plt.ylabel('Average Score Diff Rate')
    plt.title('Average Score Diff Rate by Strength Diff Rate Quantile')
    plt.grid(True)
    plt.savefig('/src/data/pic_diff_rate.png')

def getWinRate():
    # df = pd.read_csv('/src/data/20250206smfb_data_20241125_20250205.csv')
    df = pd.read_csv('/src/data/20250206smfb_data_s_20241125_20250205.csv')

    df.rename(columns={
        'strength_new_a': 'strength_a',
        'strength_new_b': 'strength_b'
    }, inplace=True)

    df = df[df['is_quality'] == 1]

    # 将strength_a == 0 和 strength_b == 0 的数据去掉
    df = df[(df['strength_a'] != 0) & (df['strength_b'] != 0)]

    # 将两组对手分开，并叠加
    win_group_a = df[df['score_a'] > df['score_b']][['strength_a']].rename(columns={'strength_a': 'strength'})
    win_group_b = df[df['score_b'] > df['score_a']][['strength_b']].rename(columns={'strength_b': 'strength'})
    win_group = pd.concat([win_group_a, win_group_b], ignore_index=True)
    win_group['win'] = 1

    lose_group_a = df[df['score_a'] < df['score_b']][['strength_a']].rename(columns={'strength_a': 'strength'})
    lose_group_b = df[df['score_b'] < df['score_a']][['strength_b']].rename(columns={'strength_b': 'strength'})
    lose_group = pd.concat([lose_group_a, lose_group_b], ignore_index=True)
    lose_group['win'] = 0

    all_group = pd.concat([win_group, lose_group], ignore_index=True)

    # 计算不同分位数的胜率分布
    N = 10
    quantiles = all_group['strength'].quantile([i/N for i in range(N+1)])
    all_group['strength_group'] = pd.cut(all_group['strength'], bins=quantiles, include_lowest=True, labels=False)

    win_rate_df = all_group.groupby('strength_group')['win'].mean().reset_index()
    win_rate_df['strength_quantile'] = quantiles.values[1:]  # 去掉第一个0%分位数

    # 打印胜率分布
    print('不同分位数的胜率分布：')
    print(win_rate_df)

    # 绘制胜率分布图
    plt.figure(figsize=(10, 6))
    plt.plot(win_rate_df['strength_quantile'], win_rate_df['win'], marker='o')
    plt.xlabel('Strength Quantile')
    plt.ylabel('Win Rate')
    plt.title('Win Rate by Strength Quantile')
    plt.grid(True)
    plt.savefig('/src/data/pic_win_rate.png')

# 用差值率计算
def getWinRate2():
    # 读取数据
    df = pd.read_csv('/src/data/20250206smfb_data_server_id_20241125_20250205.csv')
    # df = pd.read_csv('/src/data/20250206smfb_data_s_20241125_20250205.csv')
    df['wk'] = pd.to_datetime(df['wk'])
    df = df[(df['wk'] >= '2024-12-31') & (df['wk'] <= '2025-01-27')]

    # 重命名列
    df.rename(columns={
        'strength_new_a': 'strength_a',
        'strength_new_b': 'strength_b'
    }, inplace=True)
    
    # df.rename(columns={
    #     'strength_old_a': 'strength_a',
    #     'strength_old_b': 'strength_b'
    # }, inplace=True)

    # 过滤数据
    df = df[df['is_quality'] == 1]
    df = df[(df['strength_a'] != 0) & (df['strength_b'] != 0)]

    # 计算差值率
    df['strength_diff_rate_a'] = (df['strength_a'] - df['strength_b']) / df['strength_b']
    df['strength_diff_rate_b'] = (df['strength_b'] - df['strength_a']) / df['strength_a']

    # 创建胜负列
    df['win_a'] = (df['score_a'] > df['score_b']).astype(int)
    df['win_b'] = (df['score_b'] > df['score_a']).astype(int)

    # 将两组对手分开，并叠加
    win_group_a = df[['strength_diff_rate_a', 'win_a']].rename(columns={'strength_diff_rate_a': 'strength_diff_rate', 'win_a': 'win'})
    win_group_b = df[['strength_diff_rate_b', 'win_b']].rename(columns={'strength_diff_rate_b': 'strength_diff_rate', 'win_b': 'win'})
    all_group = pd.concat([win_group_a, win_group_b], ignore_index=True)
    

    # 计算不同分位数的胜率分布
    N = 10
    quantiles = all_group['strength_diff_rate'].quantile([i/N for i in range(N+1)])
    all_group['strength_diff_rate_group'] = pd.cut(all_group['strength_diff_rate'], bins=quantiles, include_lowest=True, labels=False)

    win_rate_df = all_group.groupby('strength_diff_rate_group')['win'].mean().reset_index()
    win_rate_df['strength_diff_rate_quantile'] = quantiles.values[1:]  # 去掉第一个0%分位数

    # 设置显示选项，避免科学计数法
    pd.set_option('display.float_format', lambda x: '%.4f' % x)

    # 打印胜率分布
    print('不同分位数的胜率分布：')
    print(win_rate_df)

    # 绘制胜率分布图
    plt.figure(figsize=(10, 6))
    plt.plot(win_rate_df['strength_diff_rate_quantile'], win_rate_df['win'], marker='o')
    plt.xlabel('Strength Diff Rate Quantile')
    plt.ylabel('Win Rate')
    plt.title('Win Rate by Strength Diff Rate Quantile')
    plt.grid(True)
    plt.savefig('/src/data/pic_win_rate2.png')

def getWinRate3():
    # 读取数据
    # df = pd.read_csv('/src/data/20250206smfb_data_server_id_20241125_20250205.csv')
    df = pd.read_csv('/src/data/20250206smfb_data_s_20241125_20250205.csv')
    df['wk'] = pd.to_datetime(df['wk'])
    df = df[(df['wk'] >= '2024-12-31') & (df['wk'] <= '2025-01-27')]

    # 重命名列
    # df.rename(columns={
    #     'strength_new_a': 'strength_a',
    #     'strength_new_b': 'strength_b'
    # }, inplace=True)
    
    df.rename(columns={
        'strength_old_a': 'strength_a',
        'strength_old_b': 'strength_b'
    }, inplace=True)

    # 过滤数据
    df = df[df['is_quality'] == 1]
    df = df[(df['strength_a'] != 0) & (df['strength_b'] != 0)]

    # 计算差值率
    df['strength_diff_rate_a'] = (df['strength_a'] - df['strength_b']) / df['strength_b']
    df['strength_diff_rate_b'] = (df['strength_b'] - df['strength_a']) / df['strength_a']

    # 创建胜负列
    df['win_a'] = (df['score_a'] > df['score_b']).astype(int)
    df['win_b'] = (df['score_b'] > df['score_a']).astype(int)

    # 将两组对手分开，并叠加
    win_group_a = df[['strength_diff_rate_a', 'win_a']].rename(columns={'strength_diff_rate_a': 'strength_diff_rate', 'win_a': 'win'})
    win_group_b = df[['strength_diff_rate_b', 'win_b']].rename(columns={'strength_diff_rate_b': 'strength_diff_rate', 'win_b': 'win'})
    all_group = pd.concat([win_group_a, win_group_b], ignore_index=True)
    
    # 按照 -250% 到 250%，每隔 10% 分组
    bins = [-2.5 + i*0.1 for i in range(51)]
    labels = range(len(bins) - 1)
    all_group['strength_diff_rate_group'] = pd.cut(all_group['strength_diff_rate'], bins=bins, include_lowest=True, labels=labels)

    # 计算每组的平均胜率和比赛数量
    win_rate_df = all_group.groupby('strength_diff_rate_group').agg(
        win_rate=('win', 'mean'),
        match_count=('win', 'size')
    ).reset_index()
    win_rate_df['strength_diff_rate_group'] = win_rate_df['strength_diff_rate_group'].apply(lambda x: bins[int(x)] if pd.notnull(x) else None)

    # 设置显示选项，避免科学计数法
    pd.set_option('display.float_format', lambda x: '%.4f' % x)

    # 打印胜率分布
    print('不同分组的胜率分布：')
    print(win_rate_df)

    # 绘制胜率分布图
    plt.figure(figsize=(10, 6))
    plt.plot(win_rate_df['strength_diff_rate_group'], win_rate_df['win_rate'], marker='o')
    plt.xlabel('Strength Diff Rate Group')
    plt.ylabel('Win Rate')
    plt.title('Win Rate by Strength Diff Rate Group')
    plt.grid(True)
    plt.savefig('/src/data/pic_win_rate3.png')

def getCorrBetweenStrengthAndScoreByServerId():
    # 读取数据
    df = pd.read_csv('/src/data/20250206smfb_data_server_id_20241125_20250205.csv')
    # df = pd.read_csv('/src/data/20250206smfb_data_s_server_id_20241125_20250205.csv')
    
    df['wk'] = pd.to_datetime(df['wk'])
    df = df[(df['wk'] >= '2024-12-31') & (df['wk'] <= '2025-01-27')]

    # 重命名列
    # df.rename(columns={
    #     'strength_new_a': 'strength_a',
    #     'strength_new_b': 'strength_b'
    # }, inplace=True)

    df.rename(columns={
        'strength_old_a': 'strength_a',
        'strength_old_b': 'strength_b'
    }, inplace=True)

    # 过滤数据
    df = df[df['is_quality'] == 1]

    df = df[(df['strength_a'] != 0) & (df['strength_b'] != 0)]

    print('after filter,server_id bigger than 1200:')
    print(len(df[df['server_id_a'] > 1200]) + len(df[df['server_id_b'] > 1200]))

    # 合并两组对手
    all_group_a = df[['server_id_a', 'strength_a', 'score_a']].rename(columns={'server_id_a': 'server_id', 'strength_a': 'strength', 'score_a': 'score'})
    all_group_b = df[['server_id_b', 'strength_b', 'score_b']].rename(columns={'server_id_b': 'server_id', 'strength_b': 'strength', 'score_b': 'score'})
    all_group = pd.concat([all_group_a, all_group_b], ignore_index=True)

    # 按照server_id分组，每300个分一组
    all_group['server_group'] = (all_group['server_id'] // 300) * 300

    # 获取所有服务器分组并按升序排序
    server_groups = sorted(all_group['server_group'].unique())
    
    # 遍历所有组计算相关性，并画图保存
    for server_group in server_groups:
        group = all_group[all_group['server_group'] == server_group].copy()
        
        # 去除异常值
        strength_99 = group['strength'].quantile(0.99)
        score_99 = group['score'].quantile(0.99)
        
        group.loc[:, 'strength'] = group['strength'].clip(upper=strength_99)
        group.loc[:, 'score'] = group['score'].clip(upper=score_99)
        
        # 计算0%到100%的分位数
        N = 10
        quantiles = group['strength'].quantile([i/N for i in range(N+1)]).drop_duplicates()
        
        # 按照strength进行分组
        group['strength_group'] = pd.cut(group['strength'], bins=quantiles, include_lowest=True, labels=False)
    
        # 计算每组中score的平均值
        grouped_df = group.groupby('strength_group')['score'].mean().reset_index()
        
        # 计算strength的分组分位数与分组内score均值的相关系数
        grouped_df['strength_quantile'] = quantiles.values[1:]  # 去掉第一个0%分位数
        
        # 检查数据情况
        if grouped_df['strength_quantile'].nunique() <= 1 or grouped_df['score'].nunique() <= 1:
            final_corr = float('nan')
        else:
            final_corr = grouped_df[['strength_quantile', 'score']].corr().iloc[0, 1]
        
        print(f'server_group {server_group}~{server_group + 299} 的strength分组分位数与分组内score均值的相关系数：{final_corr:.6f}')
        
        # 绘制图表
        plt.figure(figsize=(10, 6))
        plt.plot(grouped_df['strength_quantile'], grouped_df['score'], marker='o')
        plt.xlabel('Strength Quantile')
        plt.ylabel('Average Score')
        plt.title(f'Average Score by Strength Quantile (Server Group {server_group})')
        plt.grid(True)
        plt.savefig(f'/src/data/pic_server_group_{server_group}.png')

# 用差值率计算
def getCorrBetweenStrengthAndScoreByServerId2():
    # 读取数据
    # df = pd.read_csv('/src/data/20250206smfb_data_server_id_20241125_20250205.csv')
    df = pd.read_csv('/src/data/20250206smfb_data_s_server_id_20241125_20250205.csv')
    
    df['wk'] = pd.to_datetime(df['wk'])
    df = df[(df['wk'] >= '2024-12-31') & (df['wk'] <= '2025-01-27')]

    # 重命名列
    # df.rename(columns={
    #     'strength_new_a': 'strength_a',
    #     'strength_new_b': 'strength_b'
    # }, inplace=True)

    df.rename(columns={
        'strength_old_a': 'strength_a',
        'strength_old_b': 'strength_b'
    }, inplace=True)

    # 过滤数据
    df = df[df['is_quality'] == 1]
    df = df[(df['strength_a'] != 0) & (df['strength_b'] != 0)]

    # 计算差值率
    df['strength_diff_rate_a'] = (df['strength_a'] - df['strength_b']) / df['strength_a']
    df['strength_diff_rate_b'] = (df['strength_b'] - df['strength_a']) / df['strength_b']
    df['score_diff_rate_a'] = (df['score_a'] - df['score_b']) / df['score_a']
    df['score_diff_rate_b'] = (df['score_b'] - df['score_a']) / df['score_b']

    # 合并两组对手
    all_group_a = df[['server_id_a', 'strength_diff_rate_a', 'score_diff_rate_a']].rename(columns={'server_id_a': 'server_id', 'strength_diff_rate_a': 'strength_diff_rate', 'score_diff_rate_a': 'score_diff_rate'})
    all_group_b = df[['server_id_b', 'strength_diff_rate_b', 'score_diff_rate_b']].rename(columns={'server_id_b': 'server_id', 'strength_diff_rate_b': 'strength_diff_rate', 'score_diff_rate_b': 'score_diff_rate'})
    all_group = pd.concat([all_group_a, all_group_b], ignore_index=True)

    # 按照server_id分组，每300个分一组
    all_group['server_group'] = (all_group['server_id'] // 300) * 300

    # 获取所有服务器分组并按升序排序
    server_groups = sorted(all_group['server_group'].unique())
    
    # 遍历所有组计算相关性，并画图保存
    for server_group in server_groups:
        group = all_group[all_group['server_group'] == server_group].copy()
        
        # 去除异常值
        strength_diff_rate_99 = group['strength_diff_rate'].quantile(0.99)
        score_diff_rate_99 = group['score_diff_rate'].quantile(0.99)
        
        group.loc[:, 'strength_diff_rate'] = group['strength_diff_rate'].clip(upper=strength_diff_rate_99)
        group.loc[:, 'score_diff_rate'] = group['score_diff_rate'].clip(upper=score_diff_rate_99)
        
        # 计算0%到100%的分位数
        N = 10
        quantiles = group['strength_diff_rate'].quantile([i/N for i in range(N+1)]).drop_duplicates()
        
        # 按照strength_diff_rate进行分组
        group['strength_diff_rate_group'] = pd.cut(group['strength_diff_rate'], bins=quantiles, include_lowest=True, labels=False)
    
        # 计算每组中score_diff_rate的平均值
        grouped_df = group.groupby('strength_diff_rate_group')['score_diff_rate'].mean().reset_index()
        
        # 计算strength_diff_rate的分组分位数与分组内score_diff_rate均值的相关系数
        grouped_df['strength_diff_rate_quantile'] = quantiles.values[1:]  # 去掉第一个0%分位数
        
        # 检查数据情况
        if grouped_df['strength_diff_rate_quantile'].nunique() <= 1 or grouped_df['score_diff_rate'].nunique() <= 1:
            final_corr = float('nan')
        else:
            final_corr = grouped_df[['strength_diff_rate_quantile', 'score_diff_rate']].corr().iloc[0, 1]
        
        print(f'server_group {server_group}~{server_group + 299} 的strength_diff_rate分组分位数与分组内score_diff_rate均值的相关系数：{final_corr:.6f}')
        
        # 绘制图表
        plt.figure(figsize=(10, 6))
        plt.plot(grouped_df['strength_diff_rate_quantile'], grouped_df['score_diff_rate'], marker='o')
        plt.xlabel('Strength Diff Rate Quantile')
        plt.ylabel('Average Score Diff Rate')
        plt.title(f'Average Score Diff Rate by Strength Diff Rate Quantile (Server Group {server_group})')
        plt.grid(True)
        plt.savefig(f'/src/data/pic_server_group_{server_group}.png')

if __name__ == '__main__':
    # evaluateStrengthWithMetrics()
    # evaluateStrengthWithMetrics2()
    # evaluateStrengthWithMetrics3()

    # corrBetweenstrengthDiffRatioAndscoreDiffRatio()

    # test()
    # getCorrBetweenStrengthAndScore5()
    # getCorrBetweenStrengthAndScore6()

    # getWinRate()
    # getWinRate2()
    getWinRate3()

    # getCorrBetweenStrengthAndScoreByServerId()
    # getCorrBetweenStrengthAndScoreByServerId2()