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
    df = pd.read_csv('/src/data/smfb_getData1_2025-02-16_2025-03-17.csv')
    df1200 = df[df['server_id'] >= 1365].copy()

    df1200['3day_pay_usd_percentile'] = df1200.groupby(['wk','server_id'])['3day_pay_usd'].rank(pct=True)
    df1200['7day_pay_usd_percentile'] = df1200.groupby(['wk','server_id'])['7day_pay_usd'].rank(pct=True)
    df1200['3day_login_count_percentile'] = df1200.groupby(['wk','server_id'])['3day_login_count'].rank(pct=True)
    df1200['7day_login_count_percentile'] = df1200.groupby(['wk','server_id'])['7day_login_count'].rank(pct=True)
    df1200['p1_percentile'] = df1200.groupby(['wk','server_id'])['p1'].rank(pct=True)

    retrainDf = df1200[df1200['predicted_activity'] == 0]
    print('total users:', len(df1200))
    print('retrain users:', len(retrainDf))

    x = retrainDf[[
        '3day_pay_usd', '7day_pay_usd',
        '3day_pay_usd_percentile', '7day_pay_usd_percentile', 
        '3day_login_count', '7day_login_count', 
        '3day_login_count_percentile', '7day_login_count_percentile',
        'p1_percentile',
        'p1', 'p2_1', 'p2_2', 'p2_3', 'p3', 'p4', 'p5', 
    ]]
    y = retrainDf['actual_activity']

    x = x.fillna(0)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)

    # # y_test中的0和1的数量
    # print('y_test:', y_test)
    # print('y_test 0:', len(y_test[y_test == 0]))
    # print('y_test 1:', len(y_test[y_test == 1]))
    # return

    # 参数范围
    max_depth_range = [2, 4, 6, 8, 10]
    min_samples_split_range = [2, 5, 10]
    min_samples_leaf_range = [1, 5, 10]
    criterion_options = ['gini', 'entropy']

    # # 暂时最优参数
    # max_depth_range = [8,]
    # min_samples_split_range = [10]
    # min_samples_leaf_range = [1,]
    # criterion_options = ['entropy']

    results = []

    for max_depth in max_depth_range:
        for min_samples_split in min_samples_split_range:
            for min_samples_leaf in min_samples_leaf_range:
                for criterion in criterion_options:
                    print(f"Training Decision Tree with max_depth={max_depth}, min_samples_split={min_samples_split}, min_samples_leaf={min_samples_leaf}, criterion={criterion}")
                    clf = DecisionTreeClassifier(
                        random_state=0,
                        max_depth=max_depth,
                        min_samples_split=min_samples_split,
                        min_samples_leaf=min_samples_leaf,
                        criterion=criterion
                    )
                    clf.fit(x_train, y_train)
                    y_pred = clf.predict(x_test)

                    accuracy = accuracy_score(y_test, y_pred)
                    precision = precision_score(y_test, y_pred, zero_division=0)
                    recall = recall_score(y_test, y_pred, zero_division=0)
                    f1 = f1_score(y_test, y_pred, zero_division=0)

                    print(f"Accuracy: {accuracy}, Precision: {precision}, Recall: {recall}, F1 Score: {f1}")
                    print('----------------------------------------')
                    results.append({
                        'max_depth': max_depth,
                        'min_samples_split': min_samples_split,
                        'min_samples_leaf': min_samples_leaf,
                        'criterion': criterion,
                        'accuracy': accuracy,
                        'precision': precision,
                        'recall': recall,
                        'f1': f1
                    })

    # 保存结果到CSV
    results_df = pd.DataFrame(results)
    results_df.to_csv('/src/data/decision_tree_results.csv', index=False)

    # 找到F1分数最高的参数组合
    best_result = results_df.loc[results_df['f1'].idxmax()]
    print("Best Parameters:")
    print(best_result)

    # 使用最佳参数重新训练模型并绘图
    best_clf = DecisionTreeClassifier(
        random_state=0,
        max_depth=best_result['max_depth'],
        min_samples_split=best_result['min_samples_split'],
        min_samples_leaf=best_result['min_samples_leaf'],
        criterion=best_result['criterion']
    )
    best_clf.fit(x_train, y_train)
    y_pred = best_clf.predict(x_test)

    x_test = x_test.copy()
    x_test['y_true'] = y_test
    x_test['y_pred'] = y_pred

    x_test['p1'] = pd.cut(x_test['p1_percentile'], bins=np.arange(0, 1.05, 0.05), include_lowest=True)
    grouped = x_test.groupby('p1')

    accuracy_by_group = grouped.apply(lambda g: accuracy_score(g['y_true'], g['y_pred']))
    precision_by_group = grouped.apply(lambda g: precision_score(g['y_true'], g['y_pred'], zero_division=0))
    recall_by_group = grouped.apply(lambda g: recall_score(g['y_true'], g['y_pred'], zero_division=0))
    f1_by_group = grouped.apply(lambda g: f1_score(g['y_true'], g['y_pred'], zero_division=0))

    x0 = [interval.mid for interval in accuracy_by_group.index]
    y_accuracy = accuracy_by_group.values
    y_precision = precision_by_group.values
    y_recall = recall_by_group.values
    y_f1 = f1_by_group.values

    plt.figure(figsize=(15, 6))
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
    plt.grid(True)
    plt.savefig('/src/data/20250318dt_best.png')

    # plt.figure(figsize=(20, 20))
    plt.figure(figsize=(20, 10))
    plot_tree(best_clf, filled=True, feature_names=x.columns, class_names=['Class 0', 'Class 1'], fontsize=10)
    plt.title('Best Decision Tree Visualization')
    plt.savefig('/src/data/20250318dt_tree_best.png')

def prune_tree(clf):
    tree = clf.tree_
    children_left = tree.children_left
    children_right = tree.children_right
    value = tree.value

    def prune_node(node_id):
        # 如果当前节点是叶子节点，返回
        if children_left[node_id] == children_right[node_id]:
            return True, np.argmax(value[node_id])

        # 递归检查子节点
        left_is_leaf, left_class = prune_node(children_left[node_id])
        right_is_leaf, right_class = prune_node(children_right[node_id])

        # 如果两个子节点都是叶子节点且类别相同，则剪枝
        if left_is_leaf and right_is_leaf and left_class == right_class:
            tree.children_left[node_id] = tree.children_right[node_id] = -1
            # value[node_id] = value[children_left[node_id]] + value[children_right[node_id]]
            return True, left_class

        return False, None

    prune_node(0)

import joblib

def main2():
    df = pd.read_csv('/src/data/smfb_getData1_2025-02-16_2025-03-17.csv')
    df1200 = df[df['server_id'] >= 1365].copy()

    df1200['power'] = df1200['p2_1'] + df1200['p2_2']

    df1200['3day_pay_usd_percentile'] = df1200.groupby(['wk', 'server_id'])['3day_pay_usd'].rank(pct=True)
    df1200['7day_pay_usd_percentile'] = df1200.groupby(['wk', 'server_id'])['7day_pay_usd'].rank(pct=True)
    df1200['3day_login_count_percentile'] = df1200.groupby(['wk', 'server_id'])['3day_login_count'].rank(pct=True)
    df1200['7day_login_count_percentile'] = df1200.groupby(['wk', 'server_id'])['7day_login_count'].rank(pct=True)
    df1200['p1_percentile'] = df1200.groupby(['wk', 'server_id'])['p1'].rank(pct=True)
    df1200['p2_1_percentile'] = df1200.groupby(['wk', 'server_id'])['p2_1'].rank(pct=True)
    df1200['p2_2_percentile'] = df1200.groupby(['wk', 'server_id'])['p2_2'].rank(pct=True)
    df1200['power_percentile'] = df1200.groupby(['wk', 'server_id'])['power'].rank(pct=True)

    retrainDf = df1200[df1200['predicted_activity'] == 0]
    print('total users:', len(df1200))
    print('retrain users:', len(retrainDf))

    x = retrainDf[[
        '3day_pay_usd', '7day_pay_usd',
        '3day_pay_usd_percentile', '7day_pay_usd_percentile', 
        '3day_login_count', '7day_login_count', 
        '3day_login_count_percentile', '7day_login_count_percentile',
        'p1_percentile',
        'p2_1_percentile', 'p2_2_percentile', 
        'power_percentile',
        'power',
        'p1', 'p2_1', 'p2_2', 'p2_3', 'p3', 'p4', 'p5', 
    ]]
    y = retrainDf['actual_activity']

    x = x.fillna(0)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)

    # 参数范围
    max_depth_range = [4, 6, 8, 10]
    min_samples_split_range = [2, 5, 10]
    min_samples_leaf_range = [1, 5, 10]
    criterion_options = ['gini', 'entropy']

    results = []

    for max_depth in max_depth_range:
        for min_samples_split in min_samples_split_range:
            for min_samples_leaf in min_samples_leaf_range:
                for criterion in criterion_options:
                    clf = DecisionTreeClassifier(
                        random_state=0,
                        max_depth=max_depth,
                        min_samples_split=min_samples_split,
                        min_samples_leaf=min_samples_leaf,
                        criterion=criterion
                    )
                    clf.fit(x_train, y_train)
                    y_pred = clf.predict(x_test)

                    accuracy = accuracy_score(y_test, y_pred)
                    precision = precision_score(y_test, y_pred, zero_division=0)
                    recall = recall_score(y_test, y_pred, zero_division=0)
                    f1 = f1_score(y_test, y_pred, zero_division=0)

                    results.append({
                        'max_depth': max_depth,
                        'min_samples_split': min_samples_split,
                        'min_samples_leaf': min_samples_leaf,
                        'criterion': criterion,
                        'accuracy': accuracy,
                        'precision': precision,
                        'recall': recall,
                        'f1': f1
                    })

    # 保存结果到CSV
    results_df = pd.DataFrame(results)
    results_df.to_csv('/src/data/decision_tree_results.csv', index=False)

    # 找到F1分数最高的参数组合
    best_result = results_df.loc[results_df['f1'].idxmax()]
    print("Best Parameters:")
    print(best_result)

    # 使用最佳参数重新训练模型
    best_clf = DecisionTreeClassifier(
        random_state=0,
        max_depth=best_result['max_depth'],
        min_samples_split=best_result['min_samples_split'],
        min_samples_leaf=best_result['min_samples_leaf'],
        criterion=best_result['criterion']
    )
    best_clf.fit(x_train, y_train)

    # 获取有效的 ccp_alpha 值
    path = best_clf.cost_complexity_pruning_path(x_train, y_train)
    ccp_alphas = path.ccp_alphas

    print("CCP Alphas:(len)", len(ccp_alphas))
    print(ccp_alphas)


    ccp_alpha = ccp_alphas[len(ccp_alphas) // 2]
    # ccp_alpha = ccp_alphas[35]


    # 使用 ccp_alpha 进行裁剪
    pruned_clf = DecisionTreeClassifier(
        random_state=0,
        max_depth=best_result['max_depth'],
        min_samples_split=best_result['min_samples_split'],
        min_samples_leaf=best_result['min_samples_leaf'],
        criterion=best_result['criterion'],
        ccp_alpha=ccp_alpha
    )
    pruned_clf.fit(x_train, y_train)

    # 保存模型到文件
    joblib.dump(pruned_clf, '/src/data/decision_tree_model.pkl')

    # # 自定义后剪枝
    # prune_tree(pruned_clf)

    y_pred_pruned = pruned_clf.predict(x_test)

    # 重新计算测试集的结果
    accuracy_pruned = accuracy_score(y_test, y_pred_pruned)
    precision_pruned = precision_score(y_test, y_pred_pruned, zero_division=0)
    recall_pruned = recall_score(y_test, y_pred_pruned, zero_division=0)
    f1_pruned = f1_score(y_test, y_pred_pruned, zero_division=0)

    print("Pruned Model Performance:")
    print(f"Accuracy: {accuracy_pruned}, Precision: {precision_pruned}, Recall: {recall_pruned}, F1 Score: {f1_pruned}")

    # prune_tree(pruned_clf)

    # 绘制裁剪后的决策树
    plt.figure(figsize=(60, 30))
    plot_tree(
        pruned_clf, 
        filled=True, 
        feature_names=x.columns, 
        class_names=['Class 0', 'Class 1'],
        fontsize=12
    )
    plt.title('Pruned Decision Tree Visualization')
    plt.savefig('/src/data/20250318dt_tree_pruned.png')

from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping

def main3():
    df = pd.read_csv('/src/data/smfb_getData1_2025-02-16_2025-03-17.csv')
    df1200 = df[df['server_id'] >= 1365].copy()

    df1200['power'] = df1200['p2_1'] + df1200['p2_2']

    df1200['3day_pay_usd_percentile'] = df1200.groupby(['wk', 'server_id'])['3day_pay_usd'].rank(pct=True)
    df1200['7day_pay_usd_percentile'] = df1200.groupby(['wk', 'server_id'])['7day_pay_usd'].rank(pct=True)
    df1200['3day_login_count_percentile'] = df1200.groupby(['wk', 'server_id'])['3day_login_count'].rank(pct=True)
    df1200['7day_login_count_percentile'] = df1200.groupby(['wk', 'server_id'])['7day_login_count'].rank(pct=True)
    df1200['p1_percentile'] = df1200.groupby(['wk', 'server_id'])['p1'].rank(pct=True)
    df1200['p2_1_percentile'] = df1200.groupby(['wk', 'server_id'])['p2_1'].rank(pct=True)
    df1200['p2_2_percentile'] = df1200.groupby(['wk', 'server_id'])['p2_2'].rank(pct=True)
    df1200['power_percentile'] = df1200.groupby(['wk', 'server_id'])['power'].rank(pct=True)

    retrainDf = df1200[df1200['predicted_activity'] == 0]
    print('total users:', len(df1200))
    print('retrain users:', len(retrainDf))

    x = retrainDf[[
        '3day_pay_usd', '7day_pay_usd',
        '3day_pay_usd_percentile', '7day_pay_usd_percentile', 
        '3day_login_count', '7day_login_count', 
        '3day_login_count_percentile', '7day_login_count_percentile',
        'p1_percentile',
        'p2_1_percentile', 'p2_2_percentile', 
        'power_percentile',
        'power',
        'p1', 'p2_1', 'p2_2', 'p2_3', 'p3', 'p4', 'p5', 
    ]]
    y = retrainDf['actual_activity']

    x = x.fillna(0)

    # 标准化特征
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x)

    x_train, x_test, y_train, y_test = train_test_split(x_scaled, y, test_size=0.2, random_state=42)

    # print(y_test)
    # return

    # 构建DNN模型
    model = Sequential([
        Dense(64, activation='relu', input_shape=(x_train.shape[1],)),
        Dense(32, activation='relu'),
        Dense(32, activation='relu'),
        Dense(1, activation='sigmoid')
    ])

    model.compile(optimizer='RMSprop', loss='binary_crossentropy', metrics=['accuracy'])

    # 早停法
    early_stopping = EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True)

    # 训练模型
    model.fit(x_train, y_train, validation_split=0.2, epochs=100, callbacks=[early_stopping], batch_size=32)

    # 预测
    y_pred = (model.predict(x_test) > 0.5).astype("int32")

    # 评估模型
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)

    print("DNN Model Performance:")
    print(f"Accuracy: {accuracy}, Precision: {precision}, Recall: {recall}, F1 Score: {f1}")

    # # 保存模型
    # model.save('/src/data/dnn_model.h5')
    # joblib.dump(scaler, '/src/data/scaler.pkl')

    # x_test = x_test.copy()
    x_test = pd.DataFrame(x_test, columns=x.columns)
    x_test['y_true'] = y_test.values
    x_test['y_pred'] = y_pred

    x_test['p1'] = pd.cut(x_test['p1_percentile'], bins=np.arange(0, 1.05, 0.05), include_lowest=True)
    grouped = x_test.groupby('p1')

    accuracy_by_group = grouped.apply(lambda g: accuracy_score(g['y_true'], g['y_pred']))
    precision_by_group = grouped.apply(lambda g: precision_score(g['y_true'], g['y_pred'], zero_division=0))
    recall_by_group = grouped.apply(lambda g: recall_score(g['y_true'], g['y_pred'], zero_division=0))
    f1_by_group = grouped.apply(lambda g: f1_score(g['y_true'], g['y_pred'], zero_division=0))

    x0 = [interval.mid for interval in accuracy_by_group.index]
    y_accuracy = accuracy_by_group.values
    y_precision = precision_by_group.values
    y_recall = recall_by_group.values
    y_f1 = f1_by_group.values

    plt.figure(figsize=(15, 6))
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
    plt.grid(True)
    plt.savefig('/src/data/20250318dt_best.png')


from sklearn.tree import export_text
def debug():
    # 从文件读取模型
    loaded_clf = joblib.load('/src/data/decision_tree_model.pkl')

    prune_tree(loaded_clf)

    feature_names = [
        '3day_pay_usd', '7day_pay_usd',
        '3day_pay_usd_percentile', '7day_pay_usd_percentile', 
        '3day_login_count', '7day_login_count', 
        '3day_login_count_percentile', '7day_login_count_percentile',
        'p1_percentile',
        'p2_1_percentile', 'p2_2_percentile', 
        'power_percentile',
        'power',
        'p1', 'p2_1', 'p2_2', 'p2_3', 'p3', 'p4', 'p5', 
    ]

    r = export_text(loaded_clf, feature_names=feature_names)
    print(r)

    tree = loaded_clf.tree_

    n_nodes = tree.node_count
    children_left = tree.children_left
    children_right = tree.children_right
    feature = tree.feature
    threshold = tree.threshold
    value = tree.value

    def print_node_info(node_id):
        if children_left[node_id] == children_right[node_id]:  # 叶子节点
            print(f"Node {node_id} is a leaf node.")
            # print(f"Class distribution: {value[node_id]}")
            print(f"Predicted class: {np.argmax(value[node_id])}")
        else:  # 非叶子节点
            print(f"Node {node_id} splits on feature '{feature_names[feature[node_id]]}' with threshold {threshold[node_id]}.")
            print(f"Left child: {children_left[node_id]}, Right child: {children_right[node_id]}")

    for node_id in range(n_nodes):
        print_node_info(node_id)

        
if __name__ == '__main__':
    # getData1(startDayStr='2025-02-16', endDayStr='2025-03-17')
    main()

    # main2()

    # debug()

    # main3()