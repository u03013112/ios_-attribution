# 战区对决 续航能力评估
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')

from src.lastwar.ss.ss import ssSql


def getData(startDayStr = '2024-11-25', endDayStr = '2025-03-17'):
    sql = f'''
WITH battle_data AS (
	SELECT
		uid,
		"$part_date",
		MIN("#event_time") battle_time_min,
		MAX("#event_time") battle_time_max,
		COUNT(*) battle_count
	FROM
		(
			SELECT
				"def_uid" uid,
				"$part_date",
				"#event_time"
			FROM
				v_event_15
			WHERE
				(
					("$part_event" = 's_battle')
					AND (
						"$part_date" BETWEEN '{startDayStr}'
						AND '{endDayStr}'
					)
					AND (battle_type IN ('CROSS_THRONE'))
					AND (
						CAST(substr("def_uid", -4) AS INTEGER) BETWEEN 3
						AND 196
					)
				)
			UNION
			ALL
			SELECT
				"atk_uid" uid,
				"$part_date",
				"#event_time"
			FROM
				v_event_15
			WHERE
				(
					("$part_event" = 's_battle')
					AND (
						"$part_date" BETWEEN '{startDayStr}'
						AND '{endDayStr}'
					)
					AND (battle_type IN ('CROSS_THRONE'))
					AND (
						CAST(substr("def_uid", -4) AS INTEGER) BETWEEN 3
						AND 196
					)
				)
		) combined
	GROUP BY
		uid,
		"$part_date"
),
resource_data AS (
	SELECT
		"#account_id" uid,
		"$part_date",
		"#event_time",
		original_num,
		cost
	FROM
		(
			(
				v_event_15
				LEFT JOIN ta_dim."dim_15_0_78242" ON (
					"resource_item_id" = "dim_15_0_78242"."resource_item_id@id"
				)
			)
			LEFT JOIN ta_dim."dim_15_0_78243" ON ("op_type" = "dim_15_0_78243"."op_type@id")
		)
	WHERE
		(
			("$part_event" = 's_resource_item')
			AND (
				"$part_date" BETWEEN '{startDayStr}'
				AND '{endDayStr}'
			)
			AND (cost > 0)
			AND ("resource_item_id@type" = '士兵')
			AND (
				"op_type@desc" IN ('治疗', '秒治疗')
			)
		)
),
all_soldier_data AS (
	SELECT
		"#account_id" uid,
		"$part_date",
		"#event_time",
		"resource_item_id",
		remain_num
	FROM
		(
			(
				v_event_15
				LEFT JOIN ta_dim."dim_15_0_78242" ON (
					"resource_item_id" = "dim_15_0_78242"."resource_item_id@id"
				)
			)
			LEFT JOIN ta_dim."dim_15_0_78243" ON ("op_type" = "dim_15_0_78243"."op_type@id")
		)
	WHERE
		(
			("$part_event" = 's_resource_item')
			AND (
				"$part_date" BETWEEN '{startDayStr}'
				AND '{endDayStr}'
			)
			AND ("resource_item_id@type" = '士兵')
		)
),
joined_data AS (
	SELECT
		b.uid,
		b."$part_date",
		MIN(r."#event_time") heal_time_min,
		MAX(r."#event_time") heal_time_max,
		b.battle_time_min,
		b.battle_time_max,
		b.battle_count,
		COUNT(r.cost) heal_count,
		SUM(r.cost) healed_soldiers
	FROM
		(
			battle_data b
			LEFT JOIN resource_data r ON (
				(b.uid = r.uid)
				AND (
					r."#event_time" BETWEEN b.battle_time_min
					AND b.battle_time_max
				)
			)
		)
	GROUP BY
		b.uid,
		b."$part_date",
		b.battle_time_min,
		b.battle_time_max,
		b.battle_count
),
soldier_data2 AS (
	SELECT
		a.uid,
		b."$part_date",
        "resource_item_id",
        remain_num,
        ROW_NUMBER() OVER (
            PARTITION BY a."uid", b."$part_date", "resource_item_id"
            ORDER BY "#event_time" DESC
        ) AS rn
	FROM
		(
			all_soldier_data a
			INNER JOIN battle_data b ON (
				(a.uid = b.uid)
				AND (a."#event_time" < b.battle_time_min)
			)
		)
),
initial_soldiers2_data AS (
	SELECT
		uid,
		"$part_date",
		SUM(remain_num) initial_soldiers2
	FROM
		soldier_data2
    where 
        rn = 1
	GROUP BY
		uid,
		"$part_date"
)
SELECT
	j.uid,
	j."$part_date",
	date_format(j.battle_time_min, '%H:%i:%s') battle_time_min,
	date_format(j.battle_time_max, '%H:%i:%s') battle_time_max,
	j.battle_count,
	date_format(j.heal_time_min, '%H:%i:%s') heal_time_min,
	date_format(j.heal_time_max, '%H:%i:%s') heal_time_max,
	j.heal_count,
	j.healed_soldiers,
	s.initial_soldiers2
FROM
	joined_data j
	LEFT JOIN initial_soldiers2_data s ON (
		(j.uid = s.uid)
		AND (j."$part_date" = s."$part_date")
	)

    '''

    print('old sql:')
    print(sql)

    # 将sql中的 
    # v_event_15-> v_event_3
    # ta_dim.dim_15_0_78242 -> ta_dim.dim_3_0_66670
    # ta_dim.dim_15_0_78243 -> ta_dim.dim_3_0_66671

    sql = sql.replace('v_event_15', 'v_event_3')
    sql = sql.replace('dim_15_0_78242', 'dim_3_0_66670')
    sql = sql.replace('dim_15_0_78243', 'dim_3_0_66671')

    print('new sql:')
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

    df = pd.DataFrame(data, columns=['uid', 'part_date', 'battle_time_min', 'battle_time_max', 'battle_count', 'heal_time_min', 'heal_time_max', 'heal_count', 'healed_soldiers', 'initial_soldiers2'])
    df.to_csv(f'/src/data/zqdj_xunhang_data_{startDayStr}_{endDayStr}.csv', index=False)
    return df


def debug():
    # df = pd.read_csv('/src/data/zqdj_xunhang_data_2025-01-01_2025-03-23.csv')
    # print(df[df['uid'] == 1287950332000084])

    df = pd.read_csv('/src/data/zqdj_xuhang_3.csv')
    print(df[df['mape']>100])

def data():
    
    df2024Q4 = pd.read_csv('/src/data/zqdj_xunhang_data_2024-10-01_2024-12-31.csv')
    df2025Q1 = pd.read_csv('/src/data/zqdj_xunhang_data_2025-01-01_2025-03-31.csv')

    df = pd.concat([df2024Q4, df2025Q1])
    df.to_csv('/src/data/zqdj_xuhang_data.csv', index=False)

    df = df.sort_values(by=['uid', 'part_date'])

    # columns=['uid', 'part_date', 'battle_time_min', 'battle_time_max', 'battle_count', 'heal_time_min', 'heal_time_max', 'heal_count', 'healed_soldiers', 'initial_soldiers2']
    
    df = df.fillna(0)
    df['soldiers'] = df['initial_soldiers2'] + df['healed_soldiers']
    
    # 将soldiers == 0 的数据删除
    df = df[df['soldiers'] > 0]

    for i in range(1,4):
        dfN = df.groupby('uid').filter(lambda x: len(x) >= i)
        print(f'大于等于{i}条战绩的用户数:',dfN['uid'].nunique())
    

# 变异系数 直方图
def data2():
    df = pd.read_csv('/src/data/zqdj_xuhang_data.csv')
    df = df.fillna(0)
    # 选取战斗次数大于等于3的用户
    df3 = df.groupby('uid').filter(lambda x: len(x) >= 3)
    df3 = df3[['uid', 'part_date', 'battle_count', 'heal_count', 'healed_soldiers', 'initial_soldiers2']]
    df3['soldiers'] = df3['initial_soldiers2'] + df3['healed_soldiers']

    # 计算变异系数的函数
    def calculate_cv(group, column):
        mean_value = group[column].mean()
        std_value = group[column].std()
        cv = std_value / mean_value if mean_value != 0 else 0
        return cv

    # 计算每个用户的变异系数
    cv_soldiers = df3.groupby('uid').apply(lambda x: calculate_cv(x, 'soldiers')).reset_index(name='cv_soldiers')
    cv_initial = df3.groupby('uid').apply(lambda x: calculate_cv(x, 'initial_soldiers2')).reset_index(name='cv_initial')
    cv_healed = df3.groupby('uid').apply(lambda x: calculate_cv(x, 'healed_soldiers')).reset_index(name='cv_healed')

    # 绘制直方图
    fig, axs = plt.subplots(3, 1, figsize=(10, 18))

    axs[0].hist(cv_initial['cv_initial'], bins=30, color='lightcoral', edgecolor='black')
    axs[0].set_title('Distribution of Coefficient of Variation for Initial Soldiers')
    axs[0].set_xlabel('Coefficient of Variation')
    axs[0].set_ylabel('Frequency')
    axs[0].grid(axis='y', alpha=0.75)

    axs[1].hist(cv_healed['cv_healed'], bins=30, color='lightgreen', edgecolor='black')
    axs[1].set_title('Distribution of Coefficient of Variation for Healed Soldiers')
    axs[1].set_xlabel('Coefficient of Variation')
    axs[1].set_ylabel('Frequency')
    axs[1].grid(axis='y', alpha=0.75)

    axs[2].hist(cv_soldiers['cv_soldiers'], bins=30, color='skyblue', edgecolor='black')
    axs[2].set_title('Distribution of Coefficient of Variation for Total Soldiers')
    axs[2].set_xlabel('Coefficient of Variation')
    axs[2].set_ylabel('Frequency')
    axs[2].grid(axis='y', alpha=0.75)

    # 调整布局和保存图像
    plt.tight_layout()
    plt.savefig('/src/data/zqdj_xuhang_2.png')
    print('save /src/data/zqdj_xuhang_2.png')
    plt.close()

# 趋势占比
def data3():
    # 读取数据
    df = pd.read_csv('/src/data/zqdj_xuhang_data.csv')
    df = df.fillna(0)
    df['soldiers'] = df['initial_soldiers2'] + df['healed_soldiers']

    # 转换 part_date 为 datetime 类型
    df['part_date'] = pd.to_datetime(df['part_date'], errors='coerce')

    # 选取战斗次数大于等于3的用户
    df3 = df.groupby('uid').filter(lambda x: len(x) >= 3)

    # 获取最近三场战斗信息
    def get_last_three_battles(group):
        last_battles = group.nlargest(3, 'part_date')
        soldiers = last_battles['soldiers'].values
        if len(soldiers) == 3:
            if soldiers[2] > soldiers[1] > soldiers[0]:
                return '上涨趋势'
            elif soldiers[2] < soldiers[1] < soldiers[0]:
                return '下滑趋势'
            else:
                return '无明显趋势'
        return '无明显趋势'

    # 计算每个用户的趋势
    df3['trend'] = df3.groupby('uid').apply(get_last_three_battles).reset_index(drop=True)

    # 计算趋势占比
    trend_counts = df3['trend'].value_counts(normalize=True) * 100
    print('趋势占比:')
    print(trend_counts)

# 斜率分布直方图
from sklearn.linear_model import LinearRegression
def data4():
    # 读取数据
    df = pd.read_csv('/src/data/zqdj_xuhang_data.csv')
    df = df.fillna(0)
    df['soldiers'] = df['initial_soldiers2'] + df['healed_soldiers']

    # 转换 part_date 为 datetime 类型
    df['part_date'] = pd.to_datetime(df['part_date'], errors='coerce')

    # 选取战斗次数大于等于3的用户
    df3 = df.groupby('uid').filter(lambda x: len(x) >= 3)

    # 获取最近三场战斗信息并计算斜率
    def calculate_slope(group):
        # 按时间排序并选择最近三场
        last_battles = group.sort_values(by='part_date', ascending=False).head(3)
        soldiers = last_battles['soldiers'].values[::-1]  # 反转顺序以确保时间顺序
        if len(soldiers) == 3:
            # 线性回归计算斜率
            X = np.array([0, 1, 2]).reshape(-1, 1)
            y = soldiers
            model = LinearRegression().fit(X, y)
            return model.coef_[0]
        return np.nan

    # 计算每个用户的斜率
    df3['slope'] = df3.groupby('uid').apply(calculate_slope).reset_index(drop=True)

    # 绘制斜率分布的直方图
    slopes = df3['slope'].dropna()
    plt.figure(figsize=(10, 6))
    plt.hist(slopes, bins=30, color='skyblue', edgecolor='black')
    plt.title('Slope Distribution')
    plt.xlabel('Slope')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.savefig('/src/data/zqdj_xuhang_4.png')
    plt.close()

# 尝试输出
def calculate_soldiers_metrics():
    df = pd.read_csv('/src/data/zqdj_xuhang_data.csv')
    df = df.fillna(0)
    df['soldiers'] = df['initial_soldiers2'] + df['healed_soldiers']
    df = df[df['soldiers'] > 0]

    # 转换 part_date 为 datetime 类型
    df['part_date'] = pd.to_datetime(df['part_date'], errors='coerce')

    # 选取战斗次数大于等于3的用户
    df3 = df.groupby('uid').filter(lambda x: len(x) >= 3)

    # 计算df中用户数
    print('用户数:', len(df['uid'].unique()))
    # 计算df3中用户数
    print('大于等于3条战绩的用户数:', len(df3['uid'].unique()))


    # 计算每个用户的平均兵力
    average_soldiers_df = df3.groupby('uid')['soldiers'].mean().reset_index(name='output_soldiers')

    # 获取最近三场战斗信息
    def get_last_battles_info(group):
        last_battles = group.nlargest(3, 'part_date')
        info = {}
        for i in range(3):
            if i < len(last_battles):
                battle = last_battles.iloc[i]
                info[f'last{i+1}_battle_day'] = battle['part_date'].strftime('%Y-%m-%d')
                info[f'last{i+1}_battle_soldiers'] = battle['soldiers']
                info[f'last{i+1}_battle_init'] = battle['initial_soldiers2']
                info[f'last{i+1}_battle_healed'] = battle['healed_soldiers']
            else:
                info[f'last{i+1}_battle_day'] = np.nan
                info[f'last{i+1}_battle_soldiers'] = np.nan
                info[f'last{i+1}_battle_init'] = np.nan
                info[f'last{i+1}_battle_healed'] = np.nan
        return pd.Series(info)

    last_battles_df = df3.groupby('uid').apply(get_last_battles_info).reset_index()

    # 合并数据
    result_df = average_soldiers_df.merge(last_battles_df, on='uid')

    # 计算 MAPE
    def calculate_mape(group):
        uid = group.name
        output_soldiers = average_soldiers_df.loc[average_soldiers_df['uid'] == uid, 'output_soldiers'].values[0]
        group['absolute_percentage_error'] = np.abs((group['soldiers'] - output_soldiers) / group['soldiers'])
        return group['absolute_percentage_error'].mean()

    mape_df = df3.groupby('uid').apply(calculate_mape).reset_index(name='mape')

    # 合并 MAPE
    result_df = result_df.merge(mape_df, on='uid')

    # 输出结果
    print(result_df)
    print('MAPE:', result_df['mape'].mean())

    result_df.to_csv('/src/data/zqdj_xuhang_3.csv', index=False)

def data5():
    sql = f'''
WITH battle_data AS (
    SELECT
        uid,
        "$part_date",
        MIN("#event_time") battle_time_min,
        MAX("#event_time") battle_time_max,
        COUNT(*) battle_count
    FROM
        (
            SELECT
                "def_uid" uid,
                "$part_date",
                "#event_time"
            FROM
                v_event_15
            WHERE
                (
                    ("$part_event" = 's_battle')
                    AND (
                        "$part_date" BETWEEN '2024-10-01'
                        AND '2025-03-31'
                    )
                    AND (battle_type IN ('CROSS_THRONE'))
                    AND (
                        CAST(substr("def_uid", -4) AS INTEGER) BETWEEN 3
                        AND 196
                    )
                )
            UNION ALL
            SELECT
                "atk_uid" uid,
                "$part_date",
                "#event_time"
            FROM
                v_event_15
            WHERE
                (
                    ("$part_event" = 's_battle')
                    AND (
                        "$part_date" BETWEEN '2024-10-01'
                        AND '2025-03-31'
                    )
                    AND (battle_type IN ('CROSS_THRONE'))
                    AND (
                        CAST(substr("def_uid", -4) AS INTEGER) BETWEEN 3
                        AND 196
                    )
                )
        ) combined
    GROUP BY
        uid,
        "$part_date"
),
resource_data AS (
    SELECT
        "#account_id" uid,
        "$part_date",
        "#event_time",
        original_num,
        COALESCE(cost, 0) AS cost
    FROM
        (
            (
                v_event_15
                LEFT JOIN ta_dim."dim_15_0_78242" ON (
                    "resource_item_id" = "dim_15_0_78242"."resource_item_id@id"
                )
            )
            LEFT JOIN ta_dim."dim_15_0_78243" ON ("op_type" = "dim_15_0_78243"."op_type@id")
        )
    WHERE
        (
            ("$part_event" = 's_resource_item')
            AND (
                "$part_date" BETWEEN '2024-10-01'
                AND '2025-03-31'
            )
            AND (cost > 0)
            AND ("resource_item_id@type" = '士兵')
            AND (
                "op_type@desc" IN ('治疗', '秒治疗')
            )
        )
),
all_soldier_data AS (
    SELECT
        "#account_id" uid,
        "$part_date",
        "#event_time",
        "resource_item_id",
        remain_num
    FROM
        (
            (
                v_event_15
                LEFT JOIN ta_dim."dim_15_0_78242" ON (
                    "resource_item_id" = "dim_15_0_78242"."resource_item_id@id"
                )
            )
            LEFT JOIN ta_dim."dim_15_0_78243" ON ("op_type" = "dim_15_0_78243"."op_type@id")
        )
    WHERE
        (
            ("$part_event" = 's_resource_item')
            AND (
                "$part_date" BETWEEN '2024-10-01'
                AND '2025-03-31'
            )
            AND ("resource_item_id@type" = '士兵')
        )
),
joined_data AS (
    SELECT
        b.uid,
        b."$part_date",
        MIN(r."#event_time") heal_time_min,
        MAX(r."#event_time") heal_time_max,
        b.battle_time_min,
        b.battle_time_max,
        b.battle_count,
        COUNT(r.cost) heal_count,
        SUM(r.cost) healed_soldiers
    FROM
        (
            battle_data b
            LEFT JOIN resource_data r ON (
                (b.uid = r.uid)
                AND (
                    r."#event_time" BETWEEN b.battle_time_min
                    AND b.battle_time_max
                )
            )
        )
    GROUP BY
        b.uid,
        b."$part_date",
        b.battle_time_min,
        b.battle_time_max,
        b.battle_count
),
soldier_data2 AS (
    SELECT
        a.uid,
        b."$part_date",
        "resource_item_id",
        remain_num,
        ROW_NUMBER() OVER (
            PARTITION BY a."uid", b."$part_date", "resource_item_id"
            ORDER BY "#event_time" DESC
        ) AS rn
    FROM
        (
            all_soldier_data a
            INNER JOIN battle_data b ON (
                (a.uid = b.uid)
                AND (a."#event_time" < b.battle_time_min)
            )
        )
),
initial_soldiers2_data AS (
    SELECT
        uid,
        "$part_date",
        SUM(remain_num) initial_soldiers2
    FROM
        soldier_data2
    WHERE 
        rn = 1
    GROUP BY
        uid,
        "$part_date"
),
last_battles AS (
    SELECT
        j.uid,
        j."$part_date",
        ROW_NUMBER() OVER (
            PARTITION BY j.uid
            ORDER BY j."$part_date" DESC
        ) AS rn,
        j.battle_time_min,
        j.battle_time_max,
        j.battle_count,
        j.heal_time_min,
        j.heal_time_max,
        j.heal_count,
        COALESCE(j.healed_soldiers, 0) AS healed_soldiers,
        s.initial_soldiers2,
        s.initial_soldiers2 + COALESCE(j.healed_soldiers, 0) AS total_soldiers
    FROM
        joined_data j
        LEFT JOIN initial_soldiers2_data s ON (
            (j.uid = s.uid)
            AND (j."$part_date" = s."$part_date")
        )
    WHERE
        s.initial_soldiers2 IS NOT NULL
),
output_data AS (
    SELECT
        uid,
        AVG(total_soldiers) AS output_soldiers
    FROM
        last_battles
    WHERE
        rn <= 3
    GROUP BY
        uid
    HAVING
        COUNT(total_soldiers) = 3 AND SUM(total_soldiers) > 0
)
SELECT
    o.uid,
    o.output_soldiers,
    MAX(CASE WHEN lb.rn = 1 THEN lb."$part_date" END) AS last1_battle_day,
    MAX(CASE WHEN lb.rn = 1 THEN lb.total_soldiers END) AS last1_battle_soldiers,
    MAX(CASE WHEN lb.rn = 1 THEN lb.initial_soldiers2 END) AS last1_battle_init,
    MAX(CASE WHEN lb.rn = 1 THEN lb.healed_soldiers END) AS last1_battle_healed,
    MAX(CASE WHEN lb.rn = 2 THEN lb."$part_date" END) AS last2_battle_day,
    MAX(CASE WHEN lb.rn = 2 THEN lb.total_soldiers END) AS last2_battle_soldiers,
    MAX(CASE WHEN lb.rn = 2 THEN lb.initial_soldiers2 END) AS last2_battle_init,
    MAX(CASE WHEN lb.rn = 2 THEN lb.healed_soldiers END) AS last2_battle_healed,
    MAX(CASE WHEN lb.rn = 3 THEN lb."$part_date" END) AS last3_battle_day,
    MAX(CASE WHEN lb.rn = 3 THEN lb.total_soldiers END) AS last3_battle_soldiers,
    MAX(CASE WHEN lb.rn = 3 THEN lb.initial_soldiers2 END) AS last3_battle_init,
    MAX(CASE WHEN lb.rn = 3 THEN lb.healed_soldiers END) AS last3_battle_healed
FROM
    output_data o
    JOIN last_battles lb ON o.uid = lb.uid
WHERE
    lb.rn <= 3
GROUP BY
    o.uid, o.output_soldiers

    '''
    print('old sql:')
    print(sql)

    sql = sql.replace('v_event_15', 'v_event_3')
    sql = sql.replace('dim_15_0_78242', 'dim_3_0_66670')
    sql = sql.replace('dim_15_0_78243', 'dim_3_0_66671')

    print('new sql:')
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

    df = pd.DataFrame(data, columns=['uid', 'output_soldiers', 'last1_battle_day', 'last1_battle_soldiers', 'last1_battle_init', 'last1_battle_healed', 'last2_battle_day', 'last2_battle_soldiers', 'last2_battle_init', 'last2_battle_healed', 'last3_battle_day', 'last3_battle_soldiers', 'last3_battle_init', 'last3_battle_healed'])
    df.to_csv(f'/src/data/zqdj_xuhang_5.csv', index=False)
    return df

if __name__ == '__main__':
    # getData(startDayStr='2024-10-01', endDayStr='2024-12-31')
    # getData(startDayStr='2025-01-01', endDayStr='2025-03-31')
    # debug()

    # data()

    # df = data()
    # data2()
    # data3()
    # data4()

    # calculate_soldiers_metrics()

    data5()