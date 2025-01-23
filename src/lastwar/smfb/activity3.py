# 验证目前方案
# 计算目前方案的预测用户是否出战的准确率
# 并同样按照匹配分进行汇总，并与我的方案对比

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

import matplotlib.pyplot as plt

# 查阅文档，获得，目前预测用户是否参战有以下几条 预测玩家不参战，除此之外，均预测玩家参战。
# 1、若玩家存在历史数据：
# 1.1 计算每位玩家的历史3场的s_dragon_battle_user_score.individual_score_total的均值。若玩家individual_score_total的均值低于alliance_match_score字段K11的配置值，则该玩家不计入参与名单
# 1.2 若玩家7天未登录，则该玩家也不计入参与名单
# 2、若玩家不存在历史数据：
# 2.1 玩家3天未登录，该玩家也不计入参与名单


def main():
    df = pd.read_csv('/src/data/20250121smfb_data_20241125_20250120.csv')

    # 数据进行一定的裁剪
    df = df[df['wk'] >= '2024-12-30']

    df['y_pred'] = 1
    # 1.1
    df.loc[(df['individual_score_total_mean'] > 0) & (df['individual_score_total_mean'] < 5000),'y_pred'] = 0
    # 1.2
    df.loc[(df['individual_score_total_mean'] >= 5000) & (df['7day_login_count'] == 0),'y_pred'] = 0
    # 2.1
    df.loc[(df['individual_score_total_mean'] == 0) & (df['3day_login_count'] == 0),'y_pred'] = 0

    # 计算整体准确率
    accuracy = accuracy_score(df['activity'], df['y_pred'])
    precision = precision_score(df['activity'], df['y_pred'])
    recall = recall_score(df['activity'], df['y_pred'])
    f1 = f1_score(df['activity'], df['y_pred'])
    print(f'accuracy: {accuracy}, precision: {precision}, recall: {recall}, f1: {f1}')

    # 按照匹strength_percentile 分组计算每组的准确率、精确率、召回率和 F1 分数
    df['strength_percentile'] = pd.qcut(df['strength'], q=np.arange(0, 1.05, 0.05))
    grouped = df.groupby('strength_percentile')

    accuracy_by_group = grouped.apply(lambda g: accuracy_score(g['activity'], g['y_pred']))
    precision_by_group = grouped.apply(lambda g: precision_score(g['activity'], g['y_pred'], zero_division=0))
    recall_by_group = grouped.apply(lambda g: recall_score(g['activity'], g['y_pred'], zero_division=0))
    f1_by_group = grouped.apply(lambda g: f1_score(g['activity'], g['y_pred'], zero_division=0))

    # 绘制图表
    x0 = [interval.mid for interval in accuracy_by_group.index]
    y_accuracy = accuracy_by_group.values
    y_precision = precision_by_group.values
    y_recall = recall_by_group.values
    y_f1 = f1_by_group.values

    plt.figure(figsize=(15, 6))  # 设置图的尺寸

    plt.plot(x0, y_accuracy, marker='o', linestyle='-', color='b', label='Accuracy')
    plt.plot(x0, y_precision, marker='o', linestyle='-', color='g', label='Precision')
    plt.plot(x0, y_recall, marker='o', linestyle='-', color='r', label='Recall')
    plt.plot(x0, y_f1, marker='o', linestyle='-', color='c', label='F1 Score')

    plt.scatter(x0, y_accuracy, color='b')
    plt.scatter(x0, y_precision, color='g')
    plt.scatter(x0, y_recall, color='r')
    plt.scatter(x0, y_f1, color='c')

    plt.xlabel('Strength Percentile')
    plt.ylabel('Score')
    plt.title('Model Performance by Strength Percentile')
    plt.legend()
    plt.grid(True)  # 网格线
    plt.savefig('/src/data/20250121dt3.png')  # 保存图像

if __name__ == '__main__':
    main()