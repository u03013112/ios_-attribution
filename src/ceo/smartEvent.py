# 付费档位计算
import datetime
import pandas as pd

import sys
sys.path.append('/src')
from src.tools import getFilename
from src.maxCompute import execSql

# 获得指定时间范围内
# 具体的付费信息，应该是groupby uid的，每一行有付费次数、付费总金额 还有安装日期
# 暂时先这样统计就好
# 过滤了媒体为FB，并且国家限定
def getDataFromMaxCompute(sinceTimeStr,unitlTimeStr):
    sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y%m%d')
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
    
    sinceTimeStr2 = sinceTime.strftime("%Y-%m-%d")
    unitlTimeStr2 = unitlTime.strftime("%Y-%m-%d") + ' 23:59:59'
    # 为了获得完整的7日回收，需要往后延长7天
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
    unitlTimeStr = (unitlTime+datetime.timedelta(days=7)).strftime('%Y%m%d')

    sql='''
        select
            customer_user_id,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date,
            sum(
                case
                    when event_timestamp - install_timestamp <= 7 * 24 * 3600 then cast (1 as double)
                    else 0
                end
            ) as r7count,
            sum(
                case
                    when event_timestamp - install_timestamp <= 7 * 24 * 3600 then cast (event_revenue_usd as double)
                    else 0
                end
            ) as r7usd
        from
            ods_platform_appsflyer_events
        where
            app_id = 'com.topwar.gp'
            and event_name = 'af_purchase'
            and zone = 0
            and day >= % s
            and day <= % s
            and install_time >= "%s"
            and install_time <= "%s"
            and country_code in ("US","CA","AU","GB","UK","NZ","DE","FR","KR")
        group by
            install_date,
            customer_user_id
    '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    print(sql)
    # return
    pd_df = execSql(sql)
    return pd_df

# 计算R2
# 根据指定区间，将真实付费转为新的档位，然后再用档位的平均价格算出yp
# 用真实付费pt与yp计算出R2
# 需要每个用户的7日内真实付费总金额
def getR2():
    ret = []

# 获得每个事件的回收
# 返回数据中，不同转化数量的人，平均每个转化的回收价值
# 比如只转化1次的人，平均转化价值是多少
def getRevenuePerEvent(dataFrame):
    user_count = []
    avg = []
    # 总体
    user_count.append(len(dataFrame))
    countSum = dataFrame['r7count'].sum()
    usdSum = dataFrame['r7usd'].sum()
    avg.append(usdSum/countSum)

    for count in range(1,41):
        df = dataFrame.loc[dataFrame.r7count == count]
        user_count.append(len(df))
        countSum = df['r7count'].sum()
        usdSum = df['r7usd'].sum()
        if countSum > 0:
            avg.append(usdSum/countSum)
        else:
            avg.append(0)
    
    return pd.DataFrame(data = {
        'event_count':range(41),
        'user_count':user_count,
        'avg':avg,
    })

if __name__ == '__main__':
    # df = getDataFromMaxCompute('20221001','20221031')
    # df.to_csv(getFilename('se_20221001_20221031'))
    df = pd.read_csv(getFilename('se_20221001_20221031'))
    df2 = getRevenuePerEvent(df)
    df2.to_csv(getFilename('se2_20221001_20221031'))