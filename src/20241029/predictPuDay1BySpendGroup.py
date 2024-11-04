import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json

def init():
    global execSql
    global dayStr

    if 'o' in globals():
        print('this is online version')

        def execSql_online(sql):
            with o.execute_sql(sql).open_reader(tunnel=True, limit=False) as reader:
                pd_df = reader.to_pandas()
                print('获得%d行数据' % len(pd_df))
                return pd_df

        execSql = execSql_online

        # 线上版本是有args这个全局变量的，无需再判断
        dayStr = args['dayStr']
    else:
        print('this is local version')
        import sys
        sys.path.append('/src')
        from src.maxCompute import execSql as execSql_local

        execSql = execSql_local
        dayStr = '20241021'

    print('dayStr:', dayStr)

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

def getHistoricalData(install_day_start, install_day_end, platform='android', payUserGroupList=None):
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
        install_day BETWEEN {install_day_start} AND {install_day_end}
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
        AND day >= {install_day_start}
        AND install_day BETWEEN {install_day_start} AND {install_day_end}
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

def preprocessData(data, payUserGroupList, media=None, country=None):
    """
    预处理数据，包括日期转换、过滤、聚合、重塑和特征工程。
    """

    # 1. 转换 'install_day' 列为日期格式
    data['install_day'] = pd.to_datetime(data['install_day'], format='%Y%m%d')
    
    # 2. 过滤数据
    if media:
        data = data[data['mediasource'] == media]
    if country:
        data = data[data['country'] == country]
    
    # 3. 按 'install_day' 分组并汇总所需列
    aggregation_dict = {
        'cost': 'sum',
    }
    
    # 动态添加所有 revenue_1d_* 和 pu_1d_* 列的聚合方式
    for group in payUserGroupList:
        revenue_col = f"revenue_1d_{group['name']}"
        pu_col = f"pu_1d_{group['name']}"
        aggregation_dict[revenue_col] = 'sum'
        aggregation_dict[pu_col] = 'sum'
    
    aggregated_data = data.groupby('install_day').agg(aggregation_dict).reset_index()
    
    # 4. 重塑数据，从宽格式转换为长格式
    # 构建所有相关列的列表
    revenue_cols = [f"revenue_1d_{group['name']}" for group in payUserGroupList]
    pu_cols = [f"pu_1d_{group['name']}" for group in payUserGroupList]
    
    # 使用 pd.melt 将 revenue 和 pu 列转为长格式
    melted_revenue = aggregated_data.melt(
        id_vars=['install_day', 'cost'],
        value_vars=revenue_cols,
        var_name='pay_user_group',
        value_name='revenue_1d'
    )
    
    melted_pu = aggregated_data.melt(
        id_vars=['install_day'],
        value_vars=pu_cols,
        var_name='pay_user_group',
        value_name='pu_1d'
    )
    
    # 提取 pay_user_group_name
    melted_revenue['pay_user_group_name'] = melted_revenue['pay_user_group'].str.replace('revenue_1d_', '', regex=False)
    melted_pu['pay_user_group_name'] = melted_pu['pay_user_group'].str.replace('pu_1d_', '', regex=False)
    
    # 合并 revenue 和 pu 数据
    merged_long = pd.merge(
        melted_revenue[['install_day', 'cost', 'pay_user_group_name', 'revenue_1d']],
        melted_pu[['install_day', 'pay_user_group_name', 'pu_1d']],
        on=['install_day', 'pay_user_group_name'],
        how='left'
    )
    
    # 5. 按照 pay_user_group_name 和日期排序
    merged_long = merged_long.sort_values(['pay_user_group_name', 'install_day'])
    
    # 6. 计算 cost_change_ratio 和 pu_change_ratio
    merged_long['cost_change_ratio'] = merged_long.groupby('pay_user_group_name')['cost'].pct_change()
    merged_long['pu_change_ratio'] = merged_long.groupby('pay_user_group_name')['pu_1d'].pct_change()
    
    # 移除第一天（无法计算变动比例）
    merged_long = merged_long.dropna(subset=['cost_change_ratio', 'pu_change_ratio'])
    
    # 7. 计算 actual_ARPPU 和 predicted_ARPPU
    # 计算实际 ARPPU
    merged_long['actual_ARPPU'] = merged_long['revenue_1d'] / merged_long['pu_1d']
    merged_long['actual_ARPPU'].replace([np.inf, -np.inf], np.nan, inplace=True)
    
    # 计算预测 ARPPU：先shift一天，再计算过去15天的均值
    merged_long['actual_ARPPU_shifted'] = merged_long.groupby('pay_user_group_name')['actual_ARPPU'].shift(1)
    merged_long['predicted_ARPPU'] = merged_long.groupby('pay_user_group_name')['actual_ARPPU_shifted'].rolling(window=15, min_periods=1).mean().reset_index(level=0, drop=True)
    
    # 8. 重命名和选择最终列
    merged_long = merged_long.rename(columns={'install_day': 'ds', 'pu_change_ratio': 'y'})
    
    # 最终选择列
    df = merged_long[['ds', 'cost', 'cost_change_ratio', 'y', 'pay_user_group_name', 'actual_ARPPU', 'predicted_ARPPU', 'pu_1d', 'revenue_1d']]
    
    # 添加周末特征
    df['is_weekend'] = df['ds'].dt.dayofweek.isin([5, 6]).astype(int)

    return df

def train_model(train_df):
    """
    训练 Prophet 模型。
    """
    # 创建和训练Prophet模型
    model = Prophet()
    model.add_regressor('cost_change_ratio')
    # model.add_regressor('is_weekend')

    train_df2 = train_df[['ds', 'y', 'cost_change_ratio','is_weekend']]
    # 去掉输入列中NaN和inf
    train_df2 = train_df2.replace([np.inf, -np.inf], np.nan).dropna()

    if len(train_df2) < 30:
        print("训练数据不足（少于30条），跳过训练。")
        return None

    model.fit(train_df2)
    
    # 打印模型训练日志
    print("Model Training Completed")

    return model


def createTable():
    if 'o' in globals():
        # 下面部分就只有线上环境可以用了
        from odps.models import Schema, Column, Partition
        columns = [
            Column(name='app', type='string', comment='app identifier'),
            Column(name='media', type='string', comment=''),
            Column(name='country', type='string', comment=''),
            Column(name='model', type='string', comment=''),
            Column(name='group_name', type='string', comment='g3__2_10'),
            Column(name='pay_user_group_name', type='string', comment='like:0~2,2~10 or 10~inf'),
        ]
        
        partitions = [
            Partition(name='day', type='string', comment='postback time,like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('lastwar_predict_day1_pu_pct_by_cost_pct', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def deletePartition(dayStr):
    if 'o' in globals():
        t = o.get_table('lastwar_predict_day1_pu_pct_by_cost_pct')
        t.delete_partition('day=%s'%(dayStr), if_exists=True)
        print(f"Partition day={dayStr} deleted.")
    else:
        print('deletePartition failed, o is not defined')

def writeTable(df, dayStr):
    print('try to write table:')
    print(df.head(5))
    if 'o' in globals():
        t = o.get_table('lastwar_predict_day1_pu_pct_by_cost_pct')
        with t.open_writer(partition='day=%s'%(dayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
        print(f"Data written to table partition day={dayStr}.")
    else:
        print('writeTable failed, o is not defined')
        print(dayStr)
        print(df)

def main(configurations,group_by_media=False, group_by_country=False):
    global dayStr

    groupName = configurations['group_name']
    payUserGroupList = configurations['payUserGroupList']

    # 找到本周的周一
    monday = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=pd.to_datetime(dayStr, format='%Y%m%d').dayofweek)
    mondayStr = monday.strftime('%Y%m%d')

    print(f"本周一： {mondayStr}")

    # 向前推8周
    # start_date = monday - pd.Timedelta(weeks=8)
    start_date = monday - pd.Timedelta(days=60)
    startDateStr = start_date.strftime('%Y%m%d')

    lastSunday = monday - pd.Timedelta(days=1)
    lastSundayStr = lastSunday.strftime('%Y%m%d')

    print(f'向前推60天：{startDateStr}~{lastSundayStr}')

    platformList = ['android', 'ios']
    appDict = {'android': 'com.fun.lastwar.gp', 'ios': 'id6448786147'}

    # mediaList = ['Facebook Ads', 'applovin_int', 'googleadwords_int'] if group_by_media else [None]
    # countryList = ['GCC', 'JP', 'KR', 'T1', 'T2', 'T3', 'TW', 'US'] if group_by_country else [None]
    mediaList = ['Facebook Ads', 'applovin_int', 'googleadwords_int'] if group_by_media else [None]
    countryList = ['JP', 'KR', 'US', 'T1'] if group_by_country else [None]

    modelDf = pd.DataFrame(columns=['app', 'media', 'country', 'model', 'group_name','pay_user_group_name'])

    for platform in platformList:
        app = appDict[platform]
        # 获取当前平台的历史数据
        historical_data = getHistoricalData(startDateStr, lastSundayStr, platform, payUserGroupList)
        print(f"Platform: {platform}, App: {app}, Data Length: {len(historical_data)}")
        print(historical_data.head())
        # 按照分组进行遍历
        for media in mediaList:
            for country in countryList:
                print('\n\n')
                print(f"platform: {platform}, app: {app}, media: {media}, country: {country}")
                # 数据预处理
                df = preprocessData(historical_data, payUserGroupList, media, country)
                print(f"Data Length After Preprocessing: {len(df)}")
                print(df.head())

                # 遍历每个 pay_user_group_name
                for payUserGroup in payUserGroupList:
                    payUserGroupName = payUserGroup['name']
                    print(f"\n正在处理 pay_user_group_name: {payUserGroupName}")

                    # 过滤当前组的数据
                    train_subset = df[df['pay_user_group_name'] == payUserGroupName].copy()
                
                    # train_subset 按照ds升序排序
                    train_subset = train_subset.sort_values('ds')
                    print(f"Data Length After Filtering: {len(train_subset)}")
                    print(train_subset.head())

                    # 训练模型
                    model = train_model(train_subset)

                    # 保存模型
                    if model is not None:
                        model_json = model_to_json(model)
                        print(model_json)
                        media_mapped = media if media else 'ALL'
                        country_mapped = country if country else 'ALL'


                        # 对 media 进行重命名
                        media_mapping = {
                            'Facebook Ads': 'FACEBOOK',
                            'applovin_int': 'APPLOVIN',
                            'googleadwords_int': 'GOOGLE',
                            'ALL': 'ALL'
                        }
                        media_mapped = media_mapping.get(media_mapped, media_mapped)

                        modelDf = modelDf.append({
                            'app': app, 
                            'media': media_mapped, 
                            'country': country_mapped, 
                            'model': model_json,
                            'group_name': groupName,
                            'pay_user_group_name': payUserGroupName,
                        }, ignore_index=True)
                    else:
                        print(f"Skipping model for platform: {platform}, media: {media}, country: {country} due to insufficient data.")

    # 写入表格前打印 modelDf
    print("\nFinal modelDf before writing to table:")
    print(modelDf.head())

    # 写入表格
    writeTable(modelDf, mondayStr)

if __name__ == "__main__":
    init()
    createTable()
    # 删除指定分区
    deletePartition(dayStr)
    
    configurations = [
        {
            'group_name':'g3__2_10',
            'payUserGroupList':[
                {'name': '0_2', 'min': 0, 'max': 2},
                {'name': '2_10', 'min': 2, 'max': 10},
                {'name': '10_inf', 'min': 10, 'max': np.inf}
            ],
        }
    ]

    
    # 依次调用 main 函数

    for configuration in configurations:
        main(configuration, False, False)
        # main(configuration, True, False)
        # main(configuration, False, True)
        # main(configuration, True, True)
