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

def getHistoricalData(install_day_start, install_day_end, platform='android'):
    # 根据平台选择不同的表名
    table_name = 'tmp_lw_cost_and_roi_by_day' if platform == 'android' else 'tmp_lw_cost_and_roi_by_day_ios'
    sql = f'''
    select
        install_day,
        mediasource,
        country,
        sum(usd) as usd,
        sum(d1) as d1,
        sum(ins) as ins,
        sum(pud1) as pud1
    from
        {table_name}
    where
        install_day between {install_day_start}
        and {install_day_end}
    group by
        install_day, mediasource, country;
    '''
    print(sql)
    data = execSql(sql)
    return data

def preprocessData(data, media=None, country=None):
    # 转换 'install_day' 列为日期格式
    data['install_day'] = pd.to_datetime(data['install_day'], format='%Y%m%d')

    # 过滤数据
    if media:
        data = data[data['mediasource'] == media]
    if country:
        data = data[data['country'] == country]

    # 按 'install_day' 分组并汇总所需列
    aggregated_data = data.groupby('install_day').agg({
        'usd': 'sum',
        'd1': 'sum'
    }).reset_index()

    # 创建数据框
    df = pd.DataFrame({
        'date': aggregated_data['install_day'],
        'ad_spend': aggregated_data['usd'],
        'revenue': aggregated_data['d1']
    })

    # 按日期排序
    df = df.sort_values('date', ascending=True)

    # 更改列名以适应Prophet模型
    df = df.rename(columns={'date': 'ds', 'revenue': 'y'})

    # 移除含NaN的行
    df = df.dropna()

    return df

def train(train_df):
    inputCols = ['ds','ad_spend']
    outputCols = ['y']

    allCols = inputCols + outputCols

    train_df = train_df[allCols]
    train_df = train_df.dropna()

    if len(train_df) < 30:
        print('>> !! train_df is empty or not enough data !!')
        print('train_df:')
        print(train_df)
        return None

    # 创建和训练Prophet模型
    model = Prophet()
    model.add_regressor('ad_spend')
    model.fit(train_df)

    print('train_df:')
    print(train_df)

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
        ]
        
        partitions = [
            Partition(name='day', type='string', comment='postback time,like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('lastwar_predict_revenue_day1_by_spend', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def deletePartition(dayStr):
    if 'o' in globals():
        t = o.get_table('lastwar_predict_revenue_day1_by_spend')
        t.delete_partition('day=%s'%(dayStr), if_exists=True)
        print(f"Partition day={dayStr} deleted.")
    else:
        print('deletePartition failed, o is not defined')

def writeTable(df, dayStr):
    print('try to write table:')
    print(df.head(5))
    if 'o' in globals():
        t = o.get_table('lastwar_predict_revenue_day1_by_spend')
        with t.open_writer(partition='day=%s'%(dayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
        print(f"Data written to table partition day={dayStr}.")
    else:
        print('writeTable failed, o is not defined')
        print(dayStr)
        print(df)

def main(group_by_media=False, group_by_country=False):
    global dayStr

    # 找到本周的周一
    monday = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=pd.to_datetime(dayStr, format='%Y%m%d').dayofweek)
    mondayStr = monday.strftime('%Y%m%d')

    # 向前推8周
    start_date = monday - pd.Timedelta(weeks=8)
    startDateStr = start_date.strftime('%Y%m%d')

    lastSunday = monday - pd.Timedelta(days=1)
    lastSundayStr = lastSunday.strftime('%Y%m%d')

    platformList = ['android', 'ios']
    appDict = {'android': 'com.fun.lastwar.gp', 'ios': 'id6448786147'}

    mediaList = ['Facebook Ads', 'applovin_int', 'googleadwords_int'] if group_by_media else [None]
    countryList = ['GCC', 'JP', 'KR', 'T1', 'T2', 'T3', 'TW', 'US'] if group_by_country else [None]

    modelDf = pd.DataFrame(columns=['app', 'media', 'country', 'model'])

    for platform in platformList:
        app = appDict[platform]
        # 获取当前平台的历史数据
        historical_data = getHistoricalData(startDateStr, lastSundayStr, platform)
        # 按照分组进行遍历
        for media in mediaList:
            for country in countryList:
                print('\n\n')
                print(f"platform: {platform}, app: {app}, media: {media}, country: {country}")
                # 数据预处理
                df = preprocessData(historical_data, media, country)

                # 训练模型
                model = train(df)

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
                        'model': model_json
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
    # 依次调用 main 函数
    main(False, False)
    main(True, False)
    main(False, True)
    main(True, True)
