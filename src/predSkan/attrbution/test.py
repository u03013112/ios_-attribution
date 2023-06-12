import sys
sys.path.append('/src')
from src.maxCompute import execSql

import pandas as pd

def test00():
    sql = '''
        select 
            *
        from ods_platform_appsflyer_skad_details
        where 
            day >= '20230601'
            and app_id = 'id1479198816'
            and skad_conversion_value > 0
        ;
    '''
    df = execSql(sql)
    df.to_csv('/src/data/zk2/skad.csv', index=False)
    return df

def test01():
    df = pd.read_csv('/src/data/zk2/skad.csv')
    # 针对不同的媒体，不同skad_conversion_value，计算event_name 为 'af_skad_revenue'和'af_purchase'的数量，命名为count
    df = df.groupby(['media_source', 'skad_conversion_value', 'event_name']).size().reset_index(name='count')
    df = df.loc[(df['event_name'] == 'af_skad_revenue') | (df['event_name'] == 'af_purchase')]
    df.to_csv('/src/data/zk2/skad_count.csv', index=False)

def test02():
    df = pd.read_csv('/src/data/zk2/skad_count.csv')
    # 统计不同媒体，不同的event_name，count的和
    df = df.groupby(['media_source', 'event_name']).agg({'count': 'sum'}).reset_index()
    df.to_csv('/src/data/zk2/skad_count_sum.csv', index=False)

if __name__ == '__main__':
    test00()
    test01()
    test02()
