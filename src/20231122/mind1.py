# 简单版本
# 查看lastwar的所有用户中，拥有gaid的用户中，有多少用户是topwar的用户
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getAllLastwarUidGaid():
    sql = '''
    select
        gameuid as uid,
        mappingvalue as gaid
    from
        rg_bi.usermapping
    where
        mappingtype = 'gaid'
    ;
    '''
    print(sql)
    df = execSql(sql)
    return df

def main():
    # df = getAllLastwarUidGaid()
    # df.to_csv('/src/data/lastwarUidGaid.csv',index=False)

    df = pd.read_csv('/src/data/lastwarUidGaid.csv')
    
    df = df.loc[df['gaid'].notnull()]
    
    df = df[
        (df['gaid'].str.len() == 36) &
        (df['gaid'] != '00000000-0000-0000-0000-000000000000')
    ]
    

    df = df[['gaid','uid']]
    df.to_csv('/src/data/lastwarUidGaid2.csv',index=False)


    df = df[['uid','gaid']]
    df.to_csv('/src/data/lastwarUidGaid3.csv',index=False)


def debug():
    df = pd.read_csv('/src/data/lastwarUidGaid2.csv')

    print(len(df))
    # 打印不重复的 gaid 的数量
    print(df['gaid'].nunique())
    print(df['uid'].nunique())


if __name__ == '__main__':
    # main()
    debug()