# 新版本预测是否出战

import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier, plot_tree
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

import sys
sys.path.append('/src')

from src.lastwar.ss.ss import ssSql

# 从s_para1&s_para2获得预测是否出战结果验算结果
# 按照周和服务器分组，计算TP,FP,FN。后续可以汇总后计算precision和recall
def getData1(startDayStr = '2024-11-25', endDayStr = '2025-03-17'):
    sql = f'''
WITH wk_account AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        key AS "#account_id",
        CAST(split_part(value, '|', 1) AS BIGINT) AS p1,
        COALESCE(
            CASE WHEN SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 1) = '' THEN 0
                ELSE CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 1) AS INTEGER)
            END, 0) AS p2_1,
        COALESCE(
            CASE WHEN SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 2) = '' THEN 0
                ELSE CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 2) AS INTEGER)
            END, 0) AS p2_2,
        COALESCE(
            CASE WHEN SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 3) = '' THEN 0
                ELSE CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 3) AS INTEGER)
            END, 0) AS p2_3,
        COALESCE(
            CASE WHEN SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 4) = '' THEN 0
                ELSE CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 4) AS INTEGER)
            END, 0) AS p2_4,CAST(split_part(value, '|', 3) AS DOUBLE) AS p3,
        CAST(split_part(value, '|', 4) AS DOUBLE) AS p4,
        CAST(split_part(value, '|', 5) AS DOUBLE) AS p5,
        alliance_id,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamagroup
            WHEN alliance_id = teamballianceid THEN teambgroup
        END AS alliance_group,
        CASE
            WHEN alliance_id = teamaallianceid THEN servera
            WHEN alliance_id = teamballianceid THEN serverb
        END AS server_id
    FROM ta.v_event_3,
        UNNEST(CAST(json_parse(s_para1) AS MAP<VARCHAR, VARCHAR>)) AS t (key, value)
    WHERE
        "$part_event" = 'alliance_dragon_battle_match'
        AND "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
    UNION ALL
    SELECT
        date_trunc('week', "#event_time") AS wk,
        key AS "#account_id",
        CAST(split_part(value, '|', 1) AS BIGINT) AS p1,
        COALESCE(
            CASE WHEN SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 1) = '' THEN 0
                ELSE CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 1) AS INTEGER)
            END, 0) AS p2_1,
        COALESCE(
            CASE WHEN SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 2) = '' THEN 0
                ELSE CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 2) AS INTEGER)
            END, 0) AS p2_2,
        COALESCE(
            CASE WHEN SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 3) = '' THEN 0
                ELSE CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 3) AS INTEGER)
            END, 0) AS p2_3,
        COALESCE(
            CASE WHEN SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 4) = '' THEN 0
                ELSE CAST(SPLIT_PART(SPLIT_PART(value, '|', 2), ';', 4) AS INTEGER)
            END, 0) AS p2_4,CAST(split_part(value, '|', 3) AS DOUBLE) AS p3,
        CAST(split_part(value, '|', 4) AS DOUBLE) AS p4,
        CAST(split_part(value, '|', 5) AS DOUBLE) AS p5,
        alliance_id,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamagroup
            WHEN alliance_id = teamballianceid THEN teambgroup
        END AS alliance_group,
        CASE
            WHEN alliance_id = teamaallianceid THEN servera
            WHEN alliance_id = teamballianceid THEN serverb
        END AS server_id
    FROM ta.v_event_3,
        UNNEST(CAST(json_parse(s_para2) AS MAP<VARCHAR, VARCHAR>)) AS t (key, value)
    WHERE
        "$part_event" = 'alliance_dragon_battle_match'
        AND "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
),
add_score_data AS (
    SELECT 
        "#account_id",
        date_trunc('week', "#event_time") AS wk,
        SUM(add_score) AS add_score_sum
    FROM ta.v_event_3 
    WHERE 
        "$part_event" = 's_desertStorm_point'
        AND "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
        AND add_score > 0
        AND minute("#event_time") <= 15
    GROUP BY "#account_id", date_trunc('week', "#event_time")
),
ranked_data AS (
    SELECT 
        "#account_id",
        date_trunc('week', "#event_time") AS wk,
        SUM(individual_score_total) AS individual_score_total,
        ROW_NUMBER() OVER (PARTITION BY "#account_id" ORDER BY date_trunc('week', "#event_time")) AS rn
    FROM ta.v_event_3 
    WHERE 
        "$part_event" = 's_dragon_battle_user_score' 
        AND "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
    GROUP BY "#account_id", date_trunc('week', "#event_time")
),
individual_score_mean AS (
    SELECT 
        "#account_id",
        wk,
        individual_score_total,
        COALESCE(
            AVG(individual_score_total) OVER (
                PARTITION BY "#account_id" 
                ORDER BY rn 
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ), 0
        ) AS individual_score_total_mean
    FROM ranked_data
),
prediction AS (
    SELECT
        w.wk,
        w."#account_id",
        w.p1,
        w.p2_1,w.p2_2,w.p2_3,
        w.p3,
        w.p4,
        w.p5,
        w.server_id,
        COALESCE(a.add_score_sum, 0) AS add_score_sum,
        CASE 
            WHEN COALESCE(a.add_score_sum, 0) > 0 THEN 1
            ELSE 0
        END AS actual_activity,
        COALESCE(i.individual_score_total_mean, 0) AS individual_score_total_mean,
        CASE 
            WHEN COALESCE(i.individual_score_total_mean, 0) <= 3 THEN 0
            ELSE 1
        END AS predicted_activity
    FROM wk_account w
    LEFT JOIN add_score_data a
    ON w.wk = a.wk AND w."#account_id" = a."#account_id"
    LEFT JOIN individual_score_mean i
    ON w.wk = i.wk AND w."#account_id" = i."#account_id"
),
login_data AS (
    SELECT 
        "#account_id",
        "#event_time",
        -- 判断事件时间是周几
        CASE 
            -- 如果是周一至周三，归属于本周四
            WHEN EXTRACT(DOW FROM "#event_time") BETWEEN 1 AND 3 THEN
                date_trunc('week', "#event_time") + INTERVAL '3' DAY
            -- 如果是周四至周六，归属于下周四
            WHEN EXTRACT(DOW FROM "#event_time") BETWEEN 4 AND 6 THEN
                date_trunc('week', "#event_time") + INTERVAL '10' DAY
            -- 周日单独处理
            WHEN EXTRACT(DOW FROM "#event_time") = 0 THEN
                date_trunc('week', "#event_time") + INTERVAL '10' DAY
        END AS thursday
    FROM v_event_3 
    WHERE "$part_event" = 's_login' 
        AND "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
),
login_counts AS (
    SELECT 
        "#account_id",
        date_trunc('week', thursday - INTERVAL '3' DAY) AS wk,  -- 将周四转回周一
        COUNT(CASE 
            WHEN "#event_time" >= thursday - INTERVAL '3' DAY 
            AND "#event_time" < thursday
            THEN 1 
            ELSE NULL 
        END) AS "3day_login_count",
        COUNT(CASE 
            WHEN "#event_time" >= thursday - INTERVAL '7' DAY 
            AND "#event_time" < thursday
            THEN 1 
            ELSE NULL 
        END) AS "7day_login_count"
    FROM login_data
    GROUP BY "#account_id", date_trunc('week', thursday - INTERVAL '3' DAY)
),
pay_data AS (
    SELECT 
        "#account_id",
        "#event_time",
        usd,
        -- 判断事件时间是周几
        CASE 
            -- 如果是周一至周三，归属于本周四
            WHEN EXTRACT(DOW FROM "#event_time") BETWEEN 1 AND 3 THEN
                date_trunc('week', "#event_time") + INTERVAL '3' DAY
            -- 如果是周四至周六，归属于下周四
            WHEN EXTRACT(DOW FROM "#event_time") BETWEEN 4 AND 6 THEN
                date_trunc('week', "#event_time") + INTERVAL '10' DAY
            -- 周日单独处理
            WHEN EXTRACT(DOW FROM "#event_time") = 0 THEN
                date_trunc('week', "#event_time") + INTERVAL '10' DAY
        END AS thursday
    FROM v_event_3 
    WHERE "$part_event" = 's_pay_new' 
        AND "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
),
pay_usd AS (
    SELECT 
        "#account_id",
        date_trunc('week', thursday - INTERVAL '3' DAY) AS wk,  -- 将周四转回周一
        SUM(CASE 
            WHEN "#event_time" >= thursday - INTERVAL '3' DAY 
            AND "#event_time" < thursday
            THEN usd
            ELSE 0 
        END) AS "3day_pay_usd",
        SUM(CASE 
            WHEN "#event_time" >= thursday - INTERVAL '7' DAY 
            AND "#event_time" < thursday
            THEN usd
            ELSE 0 
        END) AS "7day_pay_usd"
    FROM pay_data
    GROUP BY "#account_id", date_trunc('week', thursday - INTERVAL '3' DAY)
)
SELECT
    p.wk,
    p."#account_id",
    p.p1,
    p.p2_1,p2_2,p2_3,
    p.p3,
    p.p4,
    p.p5,
    p.server_id,
    p.actual_activity,
    p.predicted_activity,
    l."3day_login_count",
    l."7day_login_count",
    pusd."3day_pay_usd",
    pusd."7day_pay_usd"
FROM prediction p
LEFT JOIN login_counts l
ON p.wk = l.wk AND p."#account_id" = l."#account_id"
LEFT JOIN pay_usd pusd
ON p.wk = pusd.wk AND p."#account_id" = pusd."#account_id"
;
    '''

    print(sql)
    
    lines = ssSql(sql)
    print('lines:',len(lines))
    print(lines[:10])

    data = []
    for line in lines:
        if line == '':
            continue
        j = json.loads(line)
        data.append(j)

    df = pd.DataFrame(data, columns=['wk', '#account_id', 'p1','p2_1','p2_2','p2_3','p3','p4','p5', 'server_id', 'actual_activity', 'predicted_activity', '3day_login_count', '7day_login_count', '3day_pay_usd', '7day_pay_usd'])
    df.to_csv(f'/src/data/smfb_getData1_{startDayStr}_{endDayStr}.csv', index=False)
    return df

def main():
    # df = getData1(startDayStr='2025-02-16', endDayStr='2025-03-17')
    df = pd.read_csv('/src/data/smfb_getData1_2025-02-16_2025-03-17.csv')
    # 只要1200服务器以后的服务器
    df1200 = df[df['server_id'] >= 1300].copy()

    # debug，df1200中的 正样本数量
    print('df1200:',len(df1200))
    print('df1200 actual_activity:',len(df1200[df1200['actual_activity'] == 1]))

    df1200['3day_pay_usd_percentile'] = df1200.groupby('wk')['3day_pay_usd'].rank(pct=True)
    df1200['7day_pay_usd_percentile'] = df1200.groupby('wk')['7day_pay_usd'].rank(pct=True)
    df1200['3day_login_count_percentile'] = df1200.groupby('wk')['3day_login_count'].rank(pct=True)
    df1200['7day_login_count_percentile'] = df1200.groupby('wk')['7day_login_count'].rank(pct=True)

    df1200['p1_percentile'] = df1200.groupby('wk')['p1'].rank(pct=True)


    # 只要预测结果是0的进行重新预测
    retrainDf = df1200[df1200['predicted_activity'] == 0]
    print('retrainDf:',len(retrainDf))
    print('retrainDf actual_activity:',len(retrainDf[retrainDf['actual_activity'] == 1]))

    # 
    x = retrainDf[[
        '3day_pay_usd', '7day_pay_usd',
        # '3day_pay_usd_percentile', '7day_pay_usd_percentile', 
        '3day_login_count', '7day_login_count', 
        # '3day_login_count_percentile', '7day_login_count_percentile',
        'p1_percentile',
        'p1', 'p2_1', 'p2_2', 'p2_3', 'p3', 'p4', 'p5', 
    ]]
    y = retrainDf['actual_activity']

    x = x.fillna(0)
    

    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)

    # 创建决策树分类器
    # clf = DecisionTreeClassifier(random_state=0, max_depth=2, min_samples_split=10, min_samples_leaf=5, criterion='gini')
    clf = DecisionTreeClassifier(random_state=0, max_depth=4, min_samples_split=10, min_samples_leaf=5, criterion='entropy')

    # 训练模型
    clf.fit(x_train, y_train)

    # 预测
    y_pred = clf.predict(x_test)

    # 计算TP,FP,FN
    TP = sum((y_test == 1) & (y_pred == 1))
    FP = sum((y_test == 0) & (y_pred == 1))
    FN = sum((y_test == 1) & (y_pred == 0))
    print(f'TP:{TP}, FP:{FP}, FN:{FN}')

    # 计算准确率、精确率、召回率和 F1 分数
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)

    print(f'Accuracy: {accuracy:.2f}')
    print(f'Precision: {precision:.2f}')
    print(f'Recall: {recall:.2f}')
    print(f'F1 Score: {f1:.2f}')

    # 将测试集结果与原始数据合并
    x_test = x_test.copy()
    x_test['y_true'] = y_test
    x_test['y_pred'] = y_pred

    # 按照 p1_percentile 分组计算每组的准确率、精确率、召回率和 F1 分数
    x_test['p1'] = pd.cut(x_test['p1_percentile'], bins=np.arange(0, 1.05, 0.05), include_lowest=True)
    grouped = x_test.groupby('p1')
    
    accuracy_by_group = grouped.apply(lambda g: accuracy_score(g['y_true'], g['y_pred']))
    precision_by_group = grouped.apply(lambda g: precision_score(g['y_true'], g['y_pred'], zero_division=0))
    recall_by_group = grouped.apply(lambda g: recall_score(g['y_true'], g['y_pred'], zero_division=0))
    f1_by_group = grouped.apply(lambda g: f1_score(g['y_true'], g['y_pred'], zero_division=0))

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
    plt.savefig('/src/data/20250318dt2.png')  # 保存图像

    # 可视化决策树
    plt.figure(figsize=(20, 20))
    plot_tree(clf, filled=True, feature_names=x.columns, class_names=['Class 0', 'Class 1'])
    plt.title('Decision Tree Visualization')
    plt.savefig('/src/data/20250318dt_tree2.png')  # 保存决策树图像



if __name__ == '__main__':
    # getData1(startDayStr='2025-02-16', endDayStr='2025-03-17')
    main()