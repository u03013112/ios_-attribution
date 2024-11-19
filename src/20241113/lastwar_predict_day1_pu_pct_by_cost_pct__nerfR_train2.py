import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json
from datetime import datetime, timedelta

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
        dayStr = '20240817'

    print('dayStr:', dayStr)

def getHistoricalData(install_day_start, install_day_end):
    table_name = 'lastwar_predict_day1_pu_pct_by_cost_pct__nerfR_historical_data2'
    
    # 构建SQL查询语句
    sql = f'''
SELECT
    install_day,
    platform,
    media,
    country,
    group_name,
    max_r,
    pay_user_group_name,
    pu_change_ratio,
    cost_change_ratio,
    is_weekend
FROM
    {table_name}
WHERE
    day BETWEEN '{install_day_start}' AND '{install_day_end}'
;
    '''
    print("执行的SQL语句如下：\n")
    print(sql)
    
    # 执行SQL查询并返回结果
    data = execSql(sql)
    
    return data

def train_model(train_df):
    """
    训练 Prophet 模型。
    """
    # 创建和训练Prophet模型
    model = Prophet()
    model.add_regressor('cost_change_ratio')
    model.add_regressor('is_weekend')

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
            Column(name='platform', type='string', comment=''),
            Column(name='media', type='string', comment=''),
            Column(name='country', type='string', comment=''),
            Column(name='group_name', type='string', comment='g3__2_10'),
            Column(name='pay_user_group_name', type='string', comment='like:0~2,2~10 or 10~inf'),
            Column(name='model', type='string', comment='')
        ]
        
        partitions = [
            Partition(name='day', type='string', comment='postback time,like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_train2', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def deletePartition(dayStr):
    if 'o' in globals():
        t = o.get_table('lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_train2')
        t.delete_partition('day=%s'%(dayStr), if_exists=True)
        print(f"Partition day={dayStr} deleted.")
    else:
        print('deletePartition failed, o is not defined')

def writeTable(df, dayStr):
    print('try to write table:')
    print(df.head(5))
    if len(df) == 0:
        print('No data to write.')
        return
    if 'o' in globals():
        t = o.get_table('lastwar_predict_day1_pu_pct_by_cost_pct__nerf_r_train2')
        with t.open_writer(partition='day=%s'%(dayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
        print(f"Data written to table partition day={dayStr}.")
    else:
        print('writeTable failed, o is not defined')
        print(dayStr)
        print(df)

def main():
    global dayStr

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

    # 获取当前平台的历史数据
    historical_data = getHistoricalData(startDateStr, lastSundayStr)
    # 由于模型只涉及付费用户数，与付费金额无关，所以max_r没啥用，就用最大的这一档就可以了
    historical_data = historical_data[historical_data['max_r'] == 1e10]

    historical_data.rename(columns={'install_day':'ds','pu_change_ratio':'y'}, inplace=True)
    # 
    groupData = historical_data.groupby(['platform','media','country','group_name','max_r','pay_user_group_name'])

    modelDf = pd.DataFrame()

    for (platform, media, country, group_name, max_r, pay_user_group_name), group_data0 in groupData:

        print(f"platform: {platform}, media: {media}, country: {country}, group_name: {group_name}, max_r: {max_r}, pay_user_group_name: {pay_user_group_name}")
        if platform == 'ios' and media != 'ALL':
            print('ios的media不是ALL，跳过')
            continue

        model = train_model(group_data0)
        if model is None:
            continue

        modelDf = modelDf.append({
            'app': 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147',
            'platform': platform,
            'media': media,
            'country': country,
            'group_name': group_name,
            'pay_user_group_name': pay_user_group_name,
            'model': model_to_json(model),
        }, ignore_index=True)

    # 写入表格
    writeTable(modelDf, mondayStr)

if __name__ == "__main__":
    import logging

    logging.getLogger("prophet").setLevel(logging.WARNING)
    logging.getLogger("cmdstanpy").disabled=True

    init()
    createTable()
    
    # 获取当前日期
    currentDate = datetime.strptime(dayStr, '%Y%m%d')
    # 找到本周一
    currentMonday = currentDate - timedelta(days=currentDate.weekday())
    currentMondayStr = currentMonday.strftime('%Y%m%d')
    
    # 删除指定分区
    deletePartition(currentMondayStr)

    main()