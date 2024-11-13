# 计算各种方案对应的相关性分数

import os
import numpy as np
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql as execSql_local

execSql = execSql_local


def generate_case_statements(group_list, value_field, aggregate='SUM', is_count=False):
    """
    生成SQL的CASE语句部分，用于按组聚合字段。
    """
    statements = []
    for group in group_list:
        # 处理Infinity的情况
        max_value = '999999999' if group['max'] == np.inf else group['max']
        if is_count:
            statement = f"""
        SUM(
            CASE
                WHEN {value_field} > {group['min']}
                AND {value_field} <= {max_value} THEN 1
                ELSE 0
            END
        ) AS pu_1d_{group['name']},
            """
        else:
            statement = f"""
        SUM(
            CASE
                WHEN {value_field} > {group['min']}
                AND {value_field} <= {max_value} THEN {value_field}
                ELSE 0
            END
        ) AS revenue_1d_{group['name']},
            """
        statements.append(statement)
    return "\n".join(statements)

def getHistoricalData(startDayStr = '20240901',endDayStr = '20241031', platform='android', payUserGroupList=None):
    # 根据平台选择不同的表名和应用包名
    table_name = 'tmp_lw_cost_and_roi_by_day' if platform == 'android' else 'tmp_lw_cost_and_roi_by_day_ios'
    app_package = 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147'

    # 生成动态的CASE语句
    if payUserGroupList:
        revenue_case = generate_case_statements(payUserGroupList, 'revenue_1d', aggregate='SUM', is_count=False)
        count_case = generate_case_statements(payUserGroupList, 'revenue_1d', aggregate='SUM', is_count=True)
    else:
        revenue_case = ""
        count_case = ""

    sql = f'''
    @cost_data :=
    SELECT
        install_day,
        CASE 
            WHEN mediasource = 'bytedanceglobal_int' THEN 'tiktokglobal_int' 
            ELSE mediasource 
        END AS mediasource,
        country,
        SUM(usd) AS usd
    FROM
        {table_name}
    WHERE
        install_day BETWEEN '{startDayStr}' AND '{endDayStr}'
    GROUP BY
        install_day,
        CASE 
            WHEN mediasource = 'bytedanceglobal_int' THEN 'tiktokglobal_int' 
            ELSE mediasource 
        END,
        country
    ;

    @d1_purchase_data :=
    SELECT
        install_day,
        game_uid,
        country,
        mediasource,
        SUM(revenue_value_usd) AS revenue_1d
    FROM
        dwd_overseas_revenue_allproject
    WHERE
        app = 502
        AND app_package = '{app_package}'
        AND zone = 0
        AND day >= '{startDayStr}'
        AND install_day BETWEEN '{startDayStr}' AND '{endDayStr}'
        AND DATEDIFF(
            FROM_UNIXTIME(event_time),
            FROM_UNIXTIME(CAST(install_timestamp AS BIGINT)),
            'dd'
        ) = 0
    GROUP BY
        install_day,
        game_uid,
        country,
        mediasource
    ;

    @country_map :=
    SELECT
        d1.game_uid,
        d1.install_day,
        d1.country,
        d1.mediasource,
        d1.revenue_1d,
        map.countrygroup AS countrygroup
    FROM
        @d1_purchase_data AS d1
        LEFT JOIN cdm_laswwar_country_map AS map 
            ON d1.country = map.country
    ;

    @result :=
    SELECT
        install_day,
        countrygroup AS country,
        mediasource,
        {revenue_case}
        {count_case}
        SUM(revenue_1d) AS revenue_1d,
        COUNT(DISTINCT game_uid) AS pu_1d
    FROM
        @country_map
    GROUP BY
        install_day,
        countrygroup,
        mediasource
    ;

    SELECT
        COALESCE(r.install_day, c.install_day) AS install_day,
        COALESCE(r.country, c.country) AS country,
        COALESCE(r.mediasource, c.mediasource) AS mediasource,
        {', '.join([f"r.revenue_1d_{group['name']}" for group in payUserGroupList]) if payUserGroupList else ''},
        {', '.join([f"r.pu_1d_{group['name']}" for group in payUserGroupList]) if payUserGroupList else ''},
        r.revenue_1d,
        r.pu_1d,
        c.usd AS cost
    FROM
        @result AS r
        FULL OUTER JOIN @cost_data AS c 
            ON r.install_day = c.install_day
            AND r.country = c.country
            AND r.mediasource = c.mediasource
    ;
    '''
    print("执行的SQL语句如下：\n")
    print(sql)
    data = execSql(sql)
    
    return data

def processHistoricalData(data, platform, group_name, payUserGroupList):
    processed_data = pd.DataFrame()
    for group in payUserGroupList:
        temp_data = data[['install_day', 'country', 'mediasource', f'revenue_1d_{group["name"]}', f'pu_1d_{group["name"]}', 'cost']].copy()
        temp_data.rename(columns={f'revenue_1d_{group["name"]}': 'revenue_1d', f'pu_1d_{group["name"]}': 'pu_1d'}, inplace=True)
        temp_data['platform'] = platform
        temp_data['group_name'] = group_name
        temp_data['pay_user_group'] = group['name']
        processed_data = pd.concat([processed_data, temp_data], ignore_index=True)
    
    return processed_data


def getData(configurations_android):
    data = pd.DataFrame()
    for config in configurations_android:
        name = config['group_name']
        filename = f"/src/data/20241113corrScore_{name}.csv"
        processed_data = None
        if os.path.exists(filename):
            print(f"文件 {filename} 已存在，读取本地文件。")
            processed_data = pd.read_csv(filename)
        else:
            historical_data = getHistoricalData(platform='android', payUserGroupList=config['payUserGroupList'])
            processed_data = processHistoricalData(historical_data, 'android', name, config['payUserGroupList'])
            processed_data.to_csv(filename, index=False)
            print(f"结果已保存到 {filename}")

        print(f'处理前缀: {name}')
        print(f"处理结果：\n{processed_data.head()}")

        data = pd.concat([data, processed_data], ignore_index=True)

    return data


# 大盘数据
def main1():
    configurations_android = [
        {
            'group_name':'g1__all',
            'payUserGroupList':[
                {'name': 'all', 'min': 0, 'max': 1e10}  # 将 np.inf 替换为 1e10
            ],
        },{
            'group_name':'g2__2',
            'payUserGroupList':[
                {'name': '0_2', 'min': 0, 'max': 2},
                {'name': '2_inf', 'min': 2, 'max': 1e10}  # 将 np.inf 替换为 1e10
            ],
        },
    ]
    df = getData(configurations_android)
    df = df.groupby(['install_day', 'platform', 'group_name', 'pay_user_group']).agg({
        'cost':'sum',
        'pu_1d':'sum',
        'revenue_1d':'sum'
    }).reset_index()
    
    # 创建一个空的DataFrame用于存储各配置的相关性分数
    correlation_scores = pd.DataFrame(columns=['group_name', 'correlation_score'])
    
    for config in configurations_android:
        name = config['group_name']
        print(f"处理前缀: {name}")
        dfConfig = df[df['group_name'] == name]
        
        # 计算该配置下所有组的总收入，用于计算每组的收入权重
        total_revenue = dfConfig['revenue_1d'].sum()
        
        # 初始化用于存储每个组的相关性和权重的列表
        correlation_weight_list = []
        
        for group in config['payUserGroupList']:
            group_name = group['name']
            dfGroup = dfConfig[dfConfig['pay_user_group'] == group_name].sort_values('install_day').reset_index(drop=True)
            
            # 计算 cost 的变化率和 pu_1d 的变化率
            dfGroup['cost_pct'] = dfGroup['cost'].pct_change()
            dfGroup['pu_pct'] = dfGroup['pu_1d'].pct_change()
            
            # 去除缺失值（第一行的变化率为 NaN）
            dfGroup_clean = dfGroup.dropna(subset=['cost_pct', 'pu_pct'])
            
            if not dfGroup_clean.empty:
                # 计算 cost_pct 和 pu_pct 的相关系数
                correlation = dfGroup_clean['cost_pct'].corr(dfGroup_clean['pu_pct'])
            else:
                correlation = np.nan  # 如果数据不足，相关系数设为 NaN
            
            # 计算该组的收入权重
            group_revenue = dfGroup['revenue_1d'].sum()
            revenue_weight = group_revenue / total_revenue if total_revenue > 0 else 0
            
            # 将结果添加到列表中
            correlation_weight_list.append((correlation, revenue_weight))
            
            print(f"  组别: {group_name}")
            print(f"    收入总额: {group_revenue:.2f}, 收入权重: {revenue_weight:.4f}")
            print(f"    Correlation: {correlation:.4f}" if not np.isnan(correlation) else "    Correlation: NaN")
        
        # 计算整体的相关性分数：加权相关系数之和
        # 忽略相关系数为 NaN 的情况
        overall_correlation = sum([corr * weight for corr, weight in correlation_weight_list if not np.isnan(corr)])
        
        print(f"  配置 '{name}' 的整体相关性分数: {overall_correlation:.4f}\n")
        
        # 将结果添加到correlation_scores中
        correlation_scores = correlation_scores.append({
            'group_name': name,
            'correlation_score': overall_correlation
        }, ignore_index=True)
    
    # 保存相关性分数到CSV文件
    correlation_scores.to_csv("/src/data/correlation_scores.csv", index=False)
    print("所有配置的相关性分数已保存到 /src/data/correlation_scores.csv")
    
        

if __name__ == '__main__':
    main1()