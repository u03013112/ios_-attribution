# 尝试检查线上项目的结论
import pandas as pd

import sys
sys.path.append('/src')
from src.tools import afCvMapDataFrame
from src.maxCompute import execSql
from src.tools import getFilename

# 检测android 3测7 rt为什么不准

# 检测数据是否正确
# 这是从线上直接拷贝来的代码
def getTotalData2(dayStr):
    whenStr = ''
    for i in range(len(afCvMapDataFrame)):
        min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
        max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
        if pd.isna(min_event_revenue) or pd.isna(max_event_revenue):
            continue
        whenStr += 'when r1usd>%d and r1usd<=%d then %d\n'%(min_event_revenue, max_event_revenue,i)

    sql = '''
        select
            cv,
            count(*) as count,
            sum(r1usd) as sumR1usd,
            sum(r7usd) as sumR7usd,
            install_date
        from
            (
                select
                    uid,
                    case
                        when r1usd = 0
                        or r1usd is null then 0 %s
                        else 63
                    end as cv,
                    r1usd,
                    r7usd,
                    install_date
                from
                    (
                        select
                            install_date,
                            uid,
                            sum(if(life_cycle <= 2, revenue_value_usd, 0)) as r1usd,
                            sum(if(life_cycle <= 6, revenue_value_usd, 0)) as r7usd
                        from
                            (
                                select
                                    game_uid as uid,
                                    to_char(
                                        to_date(install_day, "yyyymmdd"),
                                        "yyyy-mm-dd"
                                    ) as install_date,
                                    revenue_value_usd,
                                    DATEDIFF(
                                        to_date(day, 'yyyymmdd'),
                                        to_date(install_day, 'yyyymmdd'),
                                        'dd'
                                    ) as life_cycle
                                from
                                    dwd_base_event_purchase_afattribution_realtime
                                where
                                    app_package = "com.topwar.gp"
                                    and app = 102
                                    and zone = 0
                                    and window_cycle = 9999
                                    and install_day = %s
                            )
                        group by
                            install_date,
                            uid
                    )
            )
        group by
            cv,
            install_date;
    '''%(whenStr,dayStr)

    print(sql)
    pd_df = execSql(sql)
    return pd_df

def test01():
    df = getTotalData2('20230124')
    print(df.sum())

# test01()

df = pd.read_csv(getFilename('AndroidDataRt3_20220501_20230201'))
groupbyDf = df.groupby('install_date',as_index=False).agg('sum')
groupbyDf = groupbyDf.sort_values('install_date')
groupbyDf.to_csv(getFilename('AndroidDataRt3_re'))