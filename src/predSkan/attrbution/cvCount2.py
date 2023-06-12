# 尝试搞清楚 AF是否把付费金额与付费次数不匹配的事件舍弃了

import pandas as pd
import sys
sys.path.append('/src')
from src.maxCompute import execSql

# 为了速度，先筛查一天的数据
# 暂时查一下2023-05-01的数据

def getDataFromMC():
    sql = '''
        select
            customer_user_id,
            install_time,
            event_timestamp,
            install_timestamp,
            event_revenue_usd
        from
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            and event_name = 'af_purchase'
            and zone = 0
            and day >= 20230501
            and day < 20230503
            and install_time >= '2023-05-01'
            and install_time < '2023-05-02 00:00:00'
        ;
    '''
    df = execSql(sql)
    return df

def main():
    df = getDataFromMC()
    df.to_csv('/src/data/cvCount20230501.csv',index=False)

    df = pd.read_csv('/src/data/cvCount20230501.csv')

    cvMapDf = pd.read_csv('/src/afCvMap2304.csv')
    cvMapDf = cvMapDf.loc[cvMapDf.conversion_value < 32]
    # cvMapDf 拥有列 app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change,postback_sequence_index,coarse_conversion_value,lock_window_type,lock_window_time
    # 将cvMapDf 拆分为两个df，一个是付费金额的cvRevenueMapDf，一个是付费次数的cvCounterMapDf
    cvRevenueMapDf = cvMapDf.loc[cvMapDf.event_name == 'af_skad_revenue']
    cvRevenueMapDf = cvRevenueMapDf[['conversion_value','min_event_revenue','max_event_revenue']]
    cvCounterMapDf = cvMapDf.loc[cvMapDf.event_name == 'af_purchase']
    cvCounterMapDf = cvCounterMapDf[['conversion_value','min_event_counter','max_event_counter']]

    # cvRevenueMapDf 添加一行，conversion_value = 0, min_event_revenue = -1, max_event_revenue = 0
    cvRevenueMapDf = cvRevenueMapDf.append({'conversion_value':0,'min_event_revenue':-1,'max_event_revenue':0},ignore_index=True)
    # cvCounterMapDf 添加一行，conversion_value = 0, min_event_counter = 0, max_event_counter = 0
    cvCounterMapDf = cvCounterMapDf.append({'conversion_value':0,'min_event_counter':0,'max_event_counter':0},ignore_index=True)
    # cvCounterMapDf 添加一行，conversion_value = 1, min_event_counter = 0, max_event_counter = 1
    cvCounterMapDf = cvCounterMapDf.append({'conversion_value':1,'min_event_counter':0,'max_event_counter':1},ignore_index=True)

    # df 拥有列customer_user_id,install_time,event_timestamp,install_timestamp,event_revenue_usd
    # 按照不同的customer_user_id对df进行分组
    # 每个customer_user_id做如下操作：
    # 按照event_timestamp升序排列，只保留event_timestamp - install_timestamp <= 1 * 24 * 3600的行
    # 从第一个事件开始，计算该用户的累计付费金额和付费次数
    # 通过累计付费金额，获得该用户的cv
    # 通过cv获得该用户的合法付费次数范围
    # 获取cv和合法付费次数范围参照afCheck的逻辑
    # 如果付费次数在合法范围内，则目前的累计付费金额与付费次数是合法的
    # 否则，目前的累计付费金额与付费次数是不合法的
    # 计算每个用户最终的合法付费金额和总付费金额
    # 记录在一个新的df中，列 拥有列customer_user_id,r1usdValid,r1usdTotal
    # 保存该df到'/src/data/cvCount20230501Ret.csv'
    df['install_timestamp'] = pd.to_datetime(df['install_timestamp'], unit='s')
    df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], unit='s')
    df = df[df['event_timestamp'] - df['install_timestamp'] <= pd.Timedelta(days=1)]

    result_df = pd.DataFrame(columns=['customer_user_id', 'r1usdValid', 'r1usdTotal'])
    if __debug__:
        print('debug mode，这会比较慢，中间过程会保存')
        user_dfs = []  # 用于存储所有user_df的列表

    for user_id in df['customer_user_id'].unique():
        user_df = df[df['customer_user_id'] == user_id].sort_values(by='event_timestamp')
        user_df = user_df.copy()
        user_df['cumulative_revenue'] = user_df['event_revenue_usd'].cumsum()
        user_df['cumulative_count'] = user_df.reset_index().index + 1
        if __debug__:
            user_df['cv'] = 0
            user_df['min_counter'] = 0
            user_df['max_counter'] = 0
            user_df['is_valid'] = 0

        valid_revenue = 0
        total_revenue = user_df['event_revenue_usd'].sum()

        for index, row in user_df.iterrows():
            cv = cvRevenueMapDf.loc[(cvRevenueMapDf['min_event_revenue'] < row['cumulative_revenue']) &
                                    (cvRevenueMapDf['max_event_revenue'] >= row['cumulative_revenue']), 'conversion_value'].values[0]
            min_counter = cvCounterMapDf.loc[cvCounterMapDf['conversion_value'] == cv, 'min_event_counter'].values[0]
            max_counter = cvCounterMapDf.loc[cvCounterMapDf['conversion_value'] == cv, 'max_event_counter'].values[0]
            if __debug__:
                user_df.loc[index, 'cv'] = cv
                user_df.loc[index, 'min_counter'] = min_counter
                user_df.loc[index, 'max_counter'] = max_counter

            if min_counter <= row['cumulative_count'] <= max_counter:
                valid_revenue = row['cumulative_revenue']
                if __debug__:
                    user_df.loc[index, 'is_valid'] = 1

        result_df = result_df.append({'customer_user_id': user_id, 'r1usdValid': valid_revenue, 'r1usdTotal': total_revenue}, ignore_index=True)
        if __debug__:
            user_dfs.append(user_df)  # 将每个user_df添加到列表中

    result_df.to_csv('/src/data/cvCount20230501Ret.csv', index=False)
    if __debug__:
        debug_df = pd.concat(user_dfs, ignore_index=True)
        debug_df.to_csv('/src/data/cvCount20230501Debug.csv', index=False)


def debug():
    df = pd.read_csv('/src/data/cvCount20230501Ret.csv')
    print('r1usdTotal:',df['r1usdTotal'].sum())
    print('r1usdValid:',df['r1usdValid'].sum())
    print('(r1usdTotal-r1usdValid)/r1usdTotal:',(df['r1usdTotal'].sum()-df['r1usdValid'].sum())/df['r1usdTotal'].sum())

    df = pd.read_csv('/src/data/cvCount20230501.csv')
    df = df[df['event_timestamp'] - df['install_timestamp'] <= 1*24*3600]
    print(df['event_revenue_usd'].sum())

    # 打印df中不同uid的数量
    print(df['customer_user_id'].unique().shape[0])

def tmp():
    df = pd.read_csv('/src/data/cvCount20230501Debug.csv')

    # 添加 'tmp' 列
    df['tmp'] = 0

    # 获得所有不同的 customer_user_id
    unique_customer_user_ids = df['customer_user_id'].unique()

    total_revenue_sum = 0

    # 遍历 customer_user_id
    for user_id in unique_customer_user_ids:
        user_df = df[df['customer_user_id'] == user_id]

        # 统计用户付费次数
        user_payment_count = len(user_df)

        # 找到用户付费次数对应的最高行
        valid_rows = user_df[(user_df['min_counter'] <= user_payment_count)]

        # 检查是否有满足条件的行
        if not valid_rows.empty:
            valid_row = valid_rows.iloc[-1]

            # 将这一行的 'tmp' 列值设为 1
            df.loc[valid_row.name, 'tmp'] = 1

        # 累加每个用户的累计最大值
        total_revenue_sum += user_df['cumulative_revenue'].max()

    # 计算累计付费金额的合法比例
    valid_revenue_sum = df[df['tmp'] == 1]['cumulative_revenue'].sum()
    valid_revenue_ratio = valid_revenue_sum / total_revenue_sum

    # 保存 df 到新的 CSV 文件
    df.to_csv('/src/data/cvCount20230501Debug2.csv', index=False)

    return valid_revenue_ratio



if __name__ == '__main__':
    # main()
    # debug()

    valid_revenue_ratio = tmp()
    print("累计付费金额的合法比例:", valid_revenue_ratio)