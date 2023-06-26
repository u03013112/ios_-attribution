import pandas as pd

def data():
    mediaList = [
        'googleadwords_int',
        'Facebook Ads',
        'bytedanceglobal_int',
        'snapchat_int',
        # 'other'
    ]
    # 源数据格式
    # userDf = pd.read_csv('/src/data/zk/attribution1ReStep1.csv')

    userDf = pd.read_csv('/src/data/zk/attribution1ReStep24hours.csv')

    # Geo版本
    # userDf = pd.read_csv('/src/data/zk/attribution1ReStep2.csv')
    # 由于geo版本没有r2usd,所以需要将r1usd复制一份
    # userDf['r2usd'] = userDf['r1usd']
    # 由于geo版本的cv中带有国家信息，国家信息在cv的个位数中，所以需要将cv//10，将个位数舍去
    userDf['cv'] = userDf['cv'] // 10
    userDf = userDf.groupby(['install_timestamp','cv']).sum().reset_index()

    # 目标数据格式
    # select dt,uid,hour24price,d6,ad_mediasource from rg_ai_bj.ads_train_needpredictusers_cnwx_rebuild where dt = '20230601' limit 100;

    # 由于userDf把用户切分了，所以需要按照一定的规则把用户合并起来。
    # 将r1usd进行cv化
    # 然后进行cv统计
    # r7usd求和
    userDf['install_date'] = pd.to_datetime(userDf['install_timestamp'], unit='s').dt.date
    # df列install_timestamp,cv,user_count,r1usd,r7usd,googleadwords_int count,Facebook Ads count,bytedanceglobal_int count,snapchat_int count
    # 新增加一列 'other count'
    userDf['other count'] = 1 - userDf[[media + ' count' for media in mediaList]].sum(axis=1)
    userDf.loc[userDf['other count']<0,'other count'] = 0
    # print(userDf.head(10))
    mediaList.append('other')

    df = userDf.copy()
    for media in mediaList:
        media_count_col = media + ' count'
        df[media + ' r1usd'] = df['r1usd'] * df[media_count_col]
        df[media + ' r2usd'] = df['r2usd'] * df[media_count_col]
        df[media + ' r3usd'] = df['r3usd'] * df[media_count_col]
        df[media + ' r7usd'] = df['r7usd'] * df[media_count_col]
        df[media_count_col] *= df['user_count']

    df = df.groupby(['install_date','cv']).agg('sum').reset_index()
    
    cvDf = df[['install_date','cv'] + [media + ' count' for media in mediaList]]
    cvDf = cvDf.melt(id_vars=['install_date','cv'], var_name='media', value_name='cv_count')
    cvDf['media'] = cvDf['media'].str.replace(' count', '')

    userDf_r1usd = df[['install_date','cv'] + [media + ' r1usd' for media in mediaList]]
    userDf_r1usd = userDf_r1usd.melt(id_vars=['install_date','cv'], var_name='media', value_name='r1usd')
    userDf_r1usd['media'] = userDf_r1usd['media'].str.replace(' r1usd', '')

    userDf_r2usd = df[['install_date','cv'] + [media + ' r2usd' for media in mediaList]]
    userDf_r2usd = userDf_r2usd.melt(id_vars=['install_date','cv'], var_name='media', value_name='r2usd')
    userDf_r2usd['media'] = userDf_r2usd['media'].str.replace(' r2usd', '')

    userDf_r3usd = df[['install_date','cv'] + [media + ' r3usd' for media in mediaList]]
    userDf_r3usd = userDf_r3usd.melt(id_vars=['install_date','cv'], var_name='media', value_name='r3usd')
    userDf_r3usd['media'] = userDf_r3usd['media'].str.replace(' r3usd', '')
    
    userDf = cvDf.merge(userDf_r1usd, on=['install_date', 'media','cv'])
    userDf = userDf.merge(userDf_r2usd, on=['install_date', 'media','cv'])
    userDf = userDf.merge(userDf_r3usd, on=['install_date', 'media','cv'])
    userDf.to_csv('/src/data/zk2/androidCv3X.csv', index=False )
    

def data2():
    df = pd.read_csv('/src/data/zk/check1_mmm.csv')
    df = df[['install_date','media','r7usd_raw']]
    df.rename(columns={'r7usd_raw':'r7usd'}, inplace=True)
    df.to_csv('/src/data/zk2/androidCv3Y.csv', index=False )

def dataFix():
    df = pd.read_csv('/src/data/zk2/androidCv3X.csv')
    # df 列 media 中 snapchat_int 改为 other
    df.loc[df['media']=='snapchat_int','media'] = 'other'
    df = df.groupby(['install_date','media','cv']).agg('sum').reset_index()
    df.to_csv('/src/data/zk2/androidCv3Xf.csv', index=False )

def data2Fix():
    df = pd.read_csv('/src/data/zk2/androidCv3Y.csv')
    # df 列 media 中 snapchat_int 改为 other
    df.loc[df['media']=='snapchat_int','media'] = 'other'
    df = df.groupby(['install_date','media']).agg('sum').reset_index()
    df.to_csv('/src/data/zk2/androidCv3Yf.csv', index=False )

if __name__ == '__main__':
    data()
    data2()
    dataFix()
    data2Fix()