# 尝试针对af预测方式的评测
import pandas as pd

def test(df):
    logFile = '/src/data/doc/cv/afPltv2.csv'

    with open(logFile, 'w') as f:
        f.write('sample_n,t,idfa,media,mape,corr\n')

    mediaList = [
        'googleadwords_int',
        'Facebook Ads',
        'bytedanceglobal_int'
    ]

    for n in (100,200,300,400,500,1000,2000,3000,4000,5000):
        for t in (0,7,14,30):
            for idfa in (.2,.25,.3,.35):
                for media in mediaList:
                    df0 = df.loc[
                        (df.sample_n == n) &
                        (df.t == t) &
                        (df.idfa == idfa) &
                        (df.media == media)
                    ]
                    df1 = df0.groupby(['install_date']).agg({
                        'mape':'mean',
                        'py':'mean',
                        'y':'mean'
                    })
                    mape = df1['mape'].mean()
                    corr = df1.corr()

                    # print(corr)
                    line = '%d,%d,%f,%s,%f,%f\n'%(n,t,idfa,media,mape,corr['y'].values[1])
                    with open(logFile, 'a') as f:
                        f.write(line)

if __name__ == '__main__':
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        print('正常 模式')

    df = pd.read_csv('/src/data/doc/cv/afPltv1.csv')
    test(df)