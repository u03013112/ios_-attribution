import pandas as pd

def lastwarDataStep1():
    df = pd.read_csv('事件分析_全量数据_20231001_20240228.csv')
    # 去除无用的列
    df = df[['新首次支付时间','lwu_android_gaid']]
    # 按照lwu_android_gaid去重，保留最早的新首次支付时间
    df = df.sort_values(by='新首次支付时间').drop_duplicates(subset='lwu_android_gaid',keep='first')

    print(df.head())
    df.to_csv('/src/data/lastwarDataStep1.csv',index=False)

def lastwarDataStep2():
    df = pd.read_csv('/src/data/lastwarDataStep1.csv')
    # 列名不能为空，只能包含小写字母、数字、下划线，且以小写字母开头，长度不能超过50个字符
    df.columns = ['first_pay_time','app_gaid']
    # 数据表会以第一列作为主键关联
    df = df[['app_gaid','first_pay_time']]

    df.to_csv('/src/data/lastwarDataStep2.csv',index=False)

def lastwarDataStep3():
    df = pd.read_csv('20240229-lw step2_全量数据_20230801_20240228.csv')
    df = df[['lw_first_pay_time_GAID','最后支付时间','GAID']]
    df.rename(columns={
        'lw_first_pay_time_GAID':'lw_first_pay_time',
        '最后支付时间':'tw_last_pay_time'
    },inplace=True)

    df['lw_first_pay_time'] = pd.to_datetime(df['lw_first_pay_time'])
    df['tw_last_pay_time'] = pd.to_datetime(df['tw_last_pay_time'])

    df.to_csv('/src/data/lastwarDataStep3.csv',index=False)

    # 找到lw_first_pay_time>tw_last_pay_time 并且 tw_last_pay_time > (lw_first_pay_time - 7天)的数据
    df1 = df[(df['lw_first_pay_time'] > df['tw_last_pay_time']) & (df['tw_last_pay_time'] > (df['lw_first_pay_time'] - pd.Timedelta(days=7)))]
    df1.to_csv('/src/data/lastwarDataStep3_1.csv',index=False)

    # 找到lw_first_pay_time>tw_last_pay_time 并且 tw_last_pay_time > (lw_first_pay_time - 30天)的数据
    df2 = df[(df['lw_first_pay_time'] > df['tw_last_pay_time']) & (df['tw_last_pay_time'] > (df['lw_first_pay_time'] - pd.Timedelta(days=30)))]
    df2.to_csv('/src/data/lastwarDataStep3_2.csv',index=False)

def lastwarDataStep4():
    df0 = pd.read_csv('/src/data/lastwarDataStep2.csv')
    df0 = df0[['app_gaid']]
    df0.rename(columns={'app_gaid':'lw_android_gaid'},inplace=True)
    df0.to_csv('/src/data/lastwarDataStep4_C.csv',index=False)

    df1 = pd.read_csv('/src/data/lastwarDataStep3_1.csv')
    df1 = df1[['GAID']]
    df1.rename(columns={'GAID':'lw_android_gaid'},inplace=True)
    df1.to_csv('/src/data/lastwarDataStep4_A.csv',index=False)

    df2 = pd.read_csv('/src/data/lastwarDataStep3_2.csv')
    df2 = df2[['GAID']]
    df2.rename(columns={'GAID':'lw_android_gaid'},inplace=True)
    df2.to_csv('/src/data/lastwarDataStep4_B.csv',index=False)




if __name__ == "__main__":
    # lastwarDataStep1()
    # lastwarDataStep2()
    # lastwarDataStep3()
    lastwarDataStep4()