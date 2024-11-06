import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_from_json

def init():
    global execSql
    global dayStr


    if 'o' in globals():
        print('this is online version')

        from odps import options
        # UTC+0
        options.sql.settings = {
            'odps.sql.timezone':'Africa/Accra',
            "odps.sql.submit.mode" : "script"
        }

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
        dayStr = '20241104'  # 本地测试时的日期，可自行修改

def getHistoricalData(installDayStart,installDayEnd,platform='android'):
    app_package = 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147'

    sql = f'''
select
    app,
    install_day,
    media,
    country,
    group_name,
    pay_user_group_name,
    actual_pu,
    predicted_pu,
    actual_arppu,
    predicted_arppu,
    actual_revenue,
    predicted_revenue
from lastwar_predict_day1_pu_pct_by_cost_pct_verification
where day > 0
and install_day between {installDayStart} and {installDayEnd}
and app = '{app_package}'
;
    '''
    # print(sql)
    data = execSql(sql)
    return data

def getMinWeekMape(installDayStart, installDayEnd, platform='android'):
    print(f"获取最小MAPE：installDayStart={installDayStart}, installDayEnd={installDayEnd}, platform={platform}")
    # 获取历史数据
    historical_data = getHistoricalData(installDayStart, installDayEnd, platform)
    historical_data['install_day'] = pd.to_datetime(historical_data['install_day'], format='%Y%m%d')

    # 计算 天MAPE
    dayDf = historical_data.groupby(['install_day', 'media', 'country', 'group_name']).agg({
        'actual_revenue': 'sum',
        'predicted_revenue': 'sum'
    }).reset_index()

    dayDf['mape_revenue'] = np.abs((dayDf['actual_revenue'] - dayDf['predicted_revenue']) / dayDf['actual_revenue']) * 100
    dayDf2 = dayDf.groupby(['media', 'country', 'group_name']).agg({
        'mape_revenue': 'mean'
    }).reset_index()

    # 计算 周MAPE
    historical_data['week'] = historical_data['install_day'].dt.strftime('%Y-%W')
    weekDf = historical_data.groupby(['week', 'media', 'country', 'group_name']).agg({
        'actual_revenue': 'sum',
        'predicted_revenue': 'sum'
    }).reset_index()
    weekDf['mape_revenue'] = np.abs((weekDf['actual_revenue'] - weekDf['predicted_revenue']) / weekDf['actual_revenue'])

    weekDf2 = weekDf.groupby(['media', 'country', 'group_name']).agg({
        'mape_revenue': 'mean'
    }).reset_index()

    # 找到按照 media 和 country 分组的最小 MAPE 对应的 group_name，以及最小的 MAPE 值
    minMapeDf2 = weekDf2.groupby(['media', 'country']).agg(
        minMape=('mape_revenue', 'min')
    ).reset_index()

    minMapeDf2 = minMapeDf2.merge(weekDf2, on=['media', 'country'], how='left')
    minMapeDf2 = minMapeDf2[minMapeDf2['mape_revenue'] == minMapeDf2['minMape']]
    minMapeDf2 = minMapeDf2.drop_duplicates(subset=['media', 'country'])

    # 合并日MAPE
    minMapeDf2 = minMapeDf2.merge(dayDf2, on=['media', 'country', 'group_name'], suffixes=('_week', '_day'))

    # 选择需要的列
    resultDf = minMapeDf2[['media', 'country', 'group_name', 'minMape', 'mape_revenue_day']]
    resultDf.rename(columns={'mape_revenue_day': 'dayMape'}, inplace=True)

    return resultDf

def getConfigurations(platform, lastSundayStr):
    print(f"获取配置：platform={platform}, lastSundayStr={lastSundayStr}")
    app_package = 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147'
    day = pd.to_datetime(lastSundayStr, format='%Y%m%d')
    startDay = day - pd.Timedelta(weeks=8)
    startDayStr = startDay.strftime('%Y%m%d')

    sql = f'''
WITH d1_purchase_data AS (
	SELECT
		install_day,
		game_uid,
		SUM(revenue_value_usd) AS revenue_1d
	FROM
		dwd_overseas_revenue_allproject
	WHERE
		app = 502
		AND app_package = '{app_package}'
		AND zone = 0
		AND day >= {startDayStr}
		AND install_day BETWEEN {startDayStr} AND {lastSundayStr}
		AND DATEDIFF(
			FROM_UNIXTIME(event_time),
			FROM_UNIXTIME(CAST(install_timestamp AS BIGINT)),
			'dd'
		) = 0
	GROUP BY
		install_day,
		game_uid
),
ranked_data AS (
	SELECT
		revenue_1d,
		NTILE(100) OVER (
			ORDER BY
				revenue_1d
		) AS percentile_rank
	FROM
		d1_purchase_data
)
SELECT
    MAX(
		CASE
			WHEN percentile_rank = 12 THEN revenue_1d
		END
	) AS p12_revenue_1d,
	MAX(
		CASE
			WHEN percentile_rank = 25 THEN revenue_1d
		END
	) AS p25_revenue_1d,
	MAX(
        CASE
            WHEN percentile_rank = 37 THEN revenue_1d
        END
    ) AS p37_revenue_1d,
    MAX(
        CASE
            WHEN percentile_rank = 50 THEN revenue_1d
        END
    ) AS p50_revenue_1d,
    MAX(
        CASE
            WHEN percentile_rank = 62 THEN revenue_1d
        END
    ) AS p62_revenue_1d,
    MAX(
        CASE
            WHEN percentile_rank = 75 THEN revenue_1d
        END
    ) AS p75_revenue_1d,
    MAX(
        CASE
            WHEN percentile_rank = 87 THEN revenue_1d
        END
    ) AS p87_revenue_1d,
    MAX(
        CASE
            WHEN percentile_rank = 100 THEN revenue_1d
        END
    ) AS p100_revenue_1d
FROM
	ranked_data;
    '''
    # print("执行的SQL语句如下：\n")
    # print(sql)
    data = execSql(sql)
    
    p12 = data['p12_revenue_1d'].values[0]
    p25 = data['p25_revenue_1d'].values[0]
    p37 = data['p37_revenue_1d'].values[0]
    p50 = data['p50_revenue_1d'].values[0]
    p62 = data['p62_revenue_1d'].values[0]
    p75 = data['p75_revenue_1d'].values[0]
    p87 = data['p87_revenue_1d'].values[0]

    configurations = [
        {
            'group_name':'g1__all',
            'payUserGroupList':[
                {'name': 'all', 'min': 0, 'max': np.inf}
            ],
        },
        {
            'group_name':'g2__2',
            'payUserGroupList':[
                {'name': '0_2', 'min': 0, 'max': 2},
                {'name': '2_inf', 'min': 2, 'max': np.inf}
            ],
        },
        {
            'group_name': 'g2__percentile50',
            'payUserGroupList': [
                {'name': '0_50', 'min': 0, 'max': p50},
                {'name': '50_inf', 'min': p50, 'max': np.inf}
            ]
        },
        {
            'group_name': 'g4__percentile25_50_75',
            'payUserGroupList': [
                {'name': '0_25', 'min': 0, 'max': p25},
                {'name': '25_50', 'min': p25, 'max': p50},
                {'name': '50_75', 'min': p50, 'max': p75},
                {'name': '75_inf', 'min': p75, 'max': np.inf}
            ]
        },
        {
            'group_name': 'g8__percentile12_25_37_50_62_75_87',
            'payUserGroupList': [
                {'name': '0_12', 'min': 0, 'max': p12},
                {'name': '12_25', 'min': p12, 'max': p25},
                {'name': '25_37', 'min': p25, 'max': p37},
                {'name': '37_50', 'min': p37, 'max': p50},
                {'name': '50_62', 'min': p50, 'max': p62},
                {'name': '62_75', 'min': p62, 'max': p75},
                {'name': '75_87', 'min': p75, 'max': p87},
                {'name': '87_inf', 'min': p87, 'max': np.inf}
            ]
        }
    ]

    return configurations

def loadModel(app, media, country,group_name,pay_user_group_name,dayStr):
    print(f"加载模型：app={app}, media={media}, country={country}, group_name={group_name}, pay_user_group_name={pay_user_group_name}, day={dayStr}")
    sql = f'''
        select
            model
        from
            lastwar_predict_day1_pu_pct_by_cost_pct
        where
            day = '{dayStr}'
            and app = '{app}'
            and media = '{media}'
            and country = '{country}'
            and group_name = '{group_name}'
            and pay_user_group_name = '{pay_user_group_name}'
        '''
    # print(sql)
    models_df = execSql(sql)
    if models_df.empty:
        print("没有找到模型")
        return None
    # 取出第一个模型
    row = models_df.iloc[0]
    model = model_from_json(row['model'])
    return model


# 获得所有分平台、分国家、分媒体、group_name、pay_user_group_name的ARPPU
def getPredictArppuAndLastPu(dayStr,configurations):
    print(f"获取预测ARPPU和最后一天的PU：dayStr={dayStr}")

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
        # print("执行的SQL语句如下：\n")
        # print(sql)
        data = execSql(sql)
        return data

    def preprocessData(data0, payUserGroupList, media=None, country=None):
        """
        预处理数据，包括日期转换、过滤、聚合、重塑和特征工程。
        """
        data = data0.copy()
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
        merged_long['actual_cost_shifted'] = merged_long.groupby('pay_user_group_name')['cost'].shift(1)
        merged_long['cost_change_ratio'] = merged_long.groupby('pay_user_group_name')['cost'].pct_change()
        merged_long['actual_pu_shifted'] = merged_long.groupby('pay_user_group_name')['pu_1d'].shift(1)
        merged_long['pu_change_ratio'] = merged_long.groupby('pay_user_group_name')['pu_1d'].pct_change()
        
        # 移除第一天（无法计算变动比例）
        merged_long = merged_long.dropna(subset=['cost_change_ratio', 'pu_change_ratio'])
        
        # 7. 计算 actual_arppu 和 predicted_arppu
        # 计算实际 ARPPU
        merged_long['actual_arppu'] = merged_long['revenue_1d'] / merged_long['pu_1d']
        merged_long['actual_arppu'].replace([np.inf, -np.inf], np.nan, inplace=True)
        
        # 计算预测 ARPPU：先shift一天，再计算过去15天的均值
        merged_long['actual_arppu_shifted'] = merged_long.groupby('pay_user_group_name')['actual_arppu'].shift(1)
        merged_long['predicted_arppu'] = merged_long.groupby('pay_user_group_name')['actual_arppu_shifted'].rolling(window=15, min_periods=15).mean().reset_index(level=0, drop=True)
        
        # 8. 重命名和选择最终列
        merged_long = merged_long.rename(columns={'install_day': 'ds'})
        # 最终选择列
        df = merged_long[['ds', 'actual_cost_shifted', 'cost', 'cost_change_ratio', 'actual_pu_shifted', 'pu_1d', 'pu_change_ratio', 'pay_user_group_name', 'actual_arppu', 'predicted_arppu', 'revenue_1d']]
        
        # 添加周末特征
        df['is_weekend'] = df['ds'].dt.dayofweek.isin([5, 6]).astype(int)

        return df

    # 获取从dayStr往前推N天的数据，计算平均ARPPU作为预测的ARPPU 
    N = 15

    endDate = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=1)
    endDateStr = endDate.strftime('%Y%m%d')

    startDate = endDate - pd.Timedelta(days=N)
    startDateStr = startDate.strftime('%Y%m%d')    

    retDf = pd.DataFrame()

    for platform in ['android', 'ios']:
        for configuration in configurations:
            groupName = configuration['group_name']
            payUserGroupList = configuration['payUserGroupList']

            historical_data = getHistoricalData(startDateStr, endDateStr, platform, payUserGroupList)
            historical_data['install_day'] = pd.to_datetime(historical_data['install_day'], format='%Y%m%d')

            countryList = historical_data['country'].unique()
            mediaList = historical_data['mediasource'].unique()

            # 大盘
            allDf = preprocessData(historical_data, payUserGroupList)
            # print(allDf)
            for payUserGroup in payUserGroupList:
                payUserGroupName = payUserGroup['name']
                arppu = allDf[allDf['pay_user_group_name'] == payUserGroupName]['actual_arppu'].mean()
                lastPu = allDf[(allDf['pay_user_group_name'] == payUserGroupName) & (allDf['ds'] == endDate)]['pu_1d'].values[0]
                allRetDf = pd.DataFrame({
                    'platform': [platform],
                    'country': ['ALL'],
                    'media': ['ALL'],
                    'group_name': [groupName],
                    'pay_user_group_name': [payUserGroupName],
                    'predicted_arppu': [arppu],
                    'last_pu': [lastPu]
                })
                retDf = pd.concat([retDf, allRetDf])
            
            # 分国家
            for country in countryList:
                countryDf = preprocessData(historical_data, payUserGroupList, country=country)
                for payUserGroup in payUserGroupList:
                    payUserGroupName = payUserGroup['name']
                    arppu = countryDf[countryDf['pay_user_group_name'] == payUserGroupName]['actual_arppu'].mean()
                    # lastPu = countryDf[(countryDf['pay_user_group_name'] == payUserGroupName) & (countryDf['ds'] == endDate)]['pu_1d'].values[0]
                    filtered_df = countryDf[(countryDf['pay_user_group_name'] == payUserGroupName) & (countryDf['ds'] == endDate)]
                    if not filtered_df.empty:
                        lastPu = filtered_df['pu_1d'].iloc[0]
                    else:
                        lastPu = 0
                    countryRetDf = pd.DataFrame({
                        'platform': [platform],
                        'country': [country],
                        'media': ['ALL'],
                        'group_name': [groupName],
                        'pay_user_group_name': [payUserGroupName],
                        'predicted_arppu': [arppu],
                        'last_pu': [lastPu]
                    })
                    retDf = pd.concat([retDf, countryRetDf])


            # 分媒体 和 分国家+分媒体 只有安卓有
            if platform == 'android':
                # 分媒体
                for media in mediaList:
                    mediaDf = preprocessData(historical_data, payUserGroupList, media=media)
                    for payUserGroup in payUserGroupList:
                        payUserGroupName = payUserGroup['name']
                        arppu = mediaDf[mediaDf['pay_user_group_name'] == payUserGroupName]['actual_arppu'].mean()
                        # lastPu = mediaDf[(mediaDf['pay_user_group_name'] == payUserGroupName) & (mediaDf['ds'] == endDate)]['pu_1d'].values[0]
                        filtered_df = mediaDf[(mediaDf['pay_user_group_name'] == payUserGroupName) & (mediaDf['ds'] == endDate)]
                        if not filtered_df.empty:
                            lastPu = filtered_df['pu_1d'].iloc[0]
                        else:
                            lastPu = 0

                        mediaRetDf = pd.DataFrame({
                            'platform': [platform],
                            'country': ['ALL'],
                            'media': [media],
                            'group_name': [groupName],
                            'pay_user_group_name': [payUserGroupName],
                            'predicted_arppu': [arppu],
                            'last_pu': [lastPu]
                        })
                        retDf = pd.concat([retDf, mediaRetDf])

                # 分国家+分媒体
                for country in countryList:
                    for media in mediaList:
                        countryMediaDf = preprocessData(historical_data, payUserGroupList, media=media, country=country)
                        for payUserGroup in payUserGroupList:
                            payUserGroupName = payUserGroup['name']
                            arppu = countryMediaDf[countryMediaDf['pay_user_group_name'] == payUserGroupName]['actual_arppu'].mean()
                            # lastPu = countryMediaDf[(countryMediaDf['pay_user_group_name'] == payUserGroupName) & (countryMediaDf['ds'] == endDate)]['pu_1d'].values[0]
                            filtered_df = countryMediaDf[(countryMediaDf['pay_user_group_name'] == payUserGroupName) & (countryMediaDf['ds'] == endDate)]
                            if not filtered_df.empty:
                                lastPu = filtered_df['pu_1d'].iloc[0]
                            else:
                                lastPu = 0
                            countryMediaRetDf = pd.DataFrame({
                                'platform': [platform],
                                'country': [country],
                                'media': [media],
                                'group_name': [groupName],
                                'pay_user_group_name': [payUserGroupName],
                                'predicted_arppu': [arppu],
                                'last_pu': [lastPu]
                            })
                            retDf = pd.concat([retDf, countryMediaRetDf])

    # print(retDf)
    return retDf

def getYesterdayCost(platform,dayStr):
    print(f"获取昨日花费：platform={platform}, dayStr={dayStr}")

    tableName = 'tmp_lw_cost_and_roi_by_day' if platform == 'android' else 'tmp_lw_cost_and_roi_by_day_ios'
    yesterday = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=1)
    yesterdayStr = yesterday.strftime('%Y%m%d')

    sql = f'''
SELECT
    mediasource,
    country,
    SUM(usd) AS cost
FROM
    {tableName}
WHERE
    install_day = {yesterdayStr}
GROUP BY
    mediasource,
    country
;
    '''
    # print(sql)
    data = execSql(sql)
    return data

def main():
    global dayStr

    yesterday = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=1)
    yesterdayIsWeekend = yesterday.dayofweek in [5, 6]

    # 统计往前推N周的数据
    N = 8

    # 找到上周的周一
    currentMonday = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=pd.to_datetime(dayStr, format='%Y%m%d').dayofweek)
    currentMondayStr = currentMonday.strftime('%Y%m%d')

    lastSunday = currentMonday - pd.Timedelta(days=1)
    lastSundayStr = lastSunday.strftime('%Y%m%d')

    nWeeksAgo = pd.to_datetime(currentMonday, format='%Y%m%d') - pd.Timedelta(weeks=N)
    nWeeksAgoStr = nWeeksAgo.strftime('%Y%m%d')

    platformList = ['android', 'ios']
    
    # TODO: 目前我的配置都是用安卓算的，之后可能需要分平台
    configurations = getConfigurations('android', lastSundayStr)

    for platform in platformList:
        app_package = 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147'
        print(f'平台: {platform}')    
        # 获取最小周MAPE
        minWeekMapeDf = getMinWeekMape(nWeeksAgoStr, lastSundayStr, platform)
        print('按周的最小MAPE')
        print(minWeekMapeDf)

        predictArppuAndLastPu = getPredictArppuAndLastPu(dayStr, configurations)
        
        yesterdayCost = getYesterdayCost(platform,dayStr)

        # 大盘
        allDf = minWeekMapeDf[(minWeekMapeDf['media'] == 'ALL') & (minWeekMapeDf['country'] == 'ALL')]
        allGroupName = allDf['group_name'].values[0]
        allYesterdayCost = yesterdayCost['cost'].sum()

        print(f'大盘的group_name: {allGroupName}')
        for configuration in configurations:
            if configuration['group_name'] == allGroupName:
                payUserGroupList = configuration['payUserGroupList']
                allRet = pd.DataFrame()

                for payUserGroup in payUserGroupList:
                    payUserGroupName = payUserGroup['name']

                    model = loadModel(app_package, 'ALL', 'ALL', allGroupName, payUserGroupName, currentMondayStr)
                    if model:
                        print(f'大盘{allGroupName}->{payUserGroupName}->{currentMondayStr}模型加载成功')
                        
                        for cost_change_ratio in [-0.3,-0.2,-0.1,0,0.1,0.2,0.3]:

                            # 计算预测话费金额
                            cost = allYesterdayCost * (1 + cost_change_ratio)
                            print(f'预测花费金额: {cost}，相比昨日（{allYesterdayCost}）变化: {cost_change_ratio}')

                            inputDf = pd.DataFrame({
                                'ds': [pd.to_datetime(dayStr, format='%Y%m%d')],
                                'cost_change_ratio': [cost_change_ratio],
                                'is_weekend': [yesterdayIsWeekend]
                            })
                            # 进行预测
                            forecast = model.predict(inputDf)
                            yhat = forecast['yhat'].values[0]
                            
                            # 计算预测付费用户数
                            lastPu = predictArppuAndLastPu[(predictArppuAndLastPu['platform'] == platform) & (predictArppuAndLastPu['country'] == 'ALL') & (predictArppuAndLastPu['media'] == 'ALL') & (predictArppuAndLastPu['group_name'] == allGroupName) & (predictArppuAndLastPu['pay_user_group_name'] == payUserGroupName)]['last_pu'].values[0]
                            predictedPu = lastPu * (1 + yhat)
                            print(f'预测付费用户数: {predictedPu}，相比昨日（{lastPu}）变化: {yhat}')

                            # 计算预测ARPPU
                            predictedArppu = predictArppuAndLastPu[(predictArppuAndLastPu['platform'] == platform) & (predictArppuAndLastPu['country'] == 'ALL') & (predictArppuAndLastPu['media'] == 'ALL') & (predictArppuAndLastPu['group_name'] == allGroupName) & (predictArppuAndLastPu['pay_user_group_name'] == payUserGroupName)]['predicted_arppu'].values[0]
                            print(f'预测ARPPU: {predictedArppu}')

                            # 计算预测收入
                            predictedRevenue = predictedPu * predictedArppu
                            print(f'预测收入: {predictedRevenue}')

                            ret = pd.DataFrame({
                                'platform': [platform],
                                'country': ['ALL'],
                                'media': ['ALL'],
                                'yesterday_cost': [allYesterdayCost],
                                'cost': [cost],
                                'group_name': [allGroupName],
                                'pay_user_group_name': [payUserGroupName],
                                'cost_change_ratio': [cost_change_ratio],
                                'yesterday_pu': [lastPu],
                                'predicted_pu': [predictedPu],
                                'predicted_arppu': [predictedArppu],
                                'predicted_revenue': [predictedRevenue]
                            })

                            allRet = pd.concat([allRet, ret])

                # 对allRet进行处理
                allRet = allRet.groupby(['platform', 'country', 'media', 'cost_change_ratio']).agg({
                    'yesterday_cost': 'mean',
                    'cost': 'mean',
                    'yesterday_pu': 'sum',
                    'predicted_pu': 'sum',
                    'predicted_revenue': 'sum'
                }).reset_index()
                allRet['predicted_roi'] = allRet['predicted_revenue'] / allRet['cost']
                print(allRet)





if __name__ == '__main__':
    init()
    main()
