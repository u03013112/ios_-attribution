# 对于lw，48小时比24小时优势小，对tw进行研究
import os
import io
import pandas as pd
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.maxCompute import execSql


# 1，相同的时间段，lw与tw的 24小时，48小时，7日，14日的相关性差异
def getLwIosSumPayData(startDayStr,endDayStr):
    filename = f'/src/data/lwIosPayData_48hours_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):    
        sql = f'''
SELECT
    install_day,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp - install_timestamp between 0
                and 24 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as 24h_revenue_usd,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp - install_timestamp between 0
                and 48 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as 48h_revenue_usd,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp - install_timestamp between 0
                and 7 * 24 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as 168h_revenue_usd,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp - install_timestamp between 0
                and 14 * 24 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as 336h_revenue_usd
FROM
    rg_bi.ads_lastwar_ios_purchase_adv
WHERE
    install_day between {startDayStr} and {endDayStr}
GROUP BY
    install_day;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename)
    return df

# 抽样，抽样比例rand<1
def getLwIosPayDataRAND(startDayStr,endDayStr,rand = 0.1):
    filename = f'/src/data/lwIosPayData_48hours_{startDayStr}_{endDayStr}_rand{rand}.csv'
    if not os.path.exists(filename):    
        sql = f'''
SET
  odps.sql.timezone = Africa / Accra;

set
  odps.sql.hive.compatible = true;

set odps.sql.executionengine.enable.rand.time.seed=true;

@t1 :=
SELECT
  install_day,
  game_uid as uid,
  COALESCE(
    SUM(
      CASE
        WHEN event_timestamp - install_timestamp between 0
        and 24 * 3600 THEN revenue_value_usd
        ELSE 0
      END
    ),
    0
  ) as 24h_revenue_usd,
  COALESCE(
    SUM(
      CASE
        WHEN event_timestamp - install_timestamp between 0
        and 48 * 3600 THEN revenue_value_usd
        ELSE 0
      END
    ),
    0
  ) as 48h_revenue_usd,
  COALESCE(
    SUM(
      CASE
        WHEN event_timestamp - install_timestamp between 0
        and 7 * 24 * 3600 THEN revenue_value_usd
        ELSE 0
      END
    ),
    0
  ) as 168h_revenue_usd,
  COALESCE(
    SUM(
      CASE
        WHEN event_timestamp - install_timestamp between 0
        and 14 * 24 * 3600 THEN revenue_value_usd
        ELSE 0
      END
    ),
    0
  ) as 336h_revenue_usd
FROM
  rg_bi.ads_lastwar_ios_purchase_adv
WHERE
  install_day between {startDayStr}
  and {endDayStr}
GROUP BY
  game_uid,
  install_day;

select
  install_day,
  sum(24h_revenue_usd) as 24h_revenue_usd,
  sum(48h_revenue_usd) as 48h_revenue_usd,
  sum(168h_revenue_usd) as 168h_revenue_usd,
  sum(336h_revenue_usd) as 336h_revenue_usd
FROM
  @t1
where
  rand() < {rand}
group by
  install_day;
'''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename)
    return df

# 抽样，抽样num个用户，num是每天的用户数
def getLwIosPayDataRANDNum(startDayStr,endDayStr,num = 1000,readDb = False):
    filename = f'/src/data/lwIosPayData_48hours_{startDayStr}_{endDayStr}_randNum{num}.csv'
    if readDb or not os.path.exists(filename):    
        sql = f'''
SET
  odps.sql.timezone = Africa / Accra;

set
  odps.sql.hive.compatible = true;

set 
  odps.sql.executionengine.enable.rand.time.seed=true;

@t1 :=
SELECT
  install_day,
  game_uid as uid,
  COALESCE(
    SUM(
      CASE
        WHEN event_timestamp - install_timestamp between 0
        and 24 * 3600 THEN revenue_value_usd
        ELSE 0
      END
    ),
    0
  ) as 24h_revenue_usd,
  COALESCE(
    SUM(
      CASE
        WHEN event_timestamp - install_timestamp between 0
        and 48 * 3600 THEN revenue_value_usd
        ELSE 0
      END
    ),
    0
  ) as 48h_revenue_usd,
  COALESCE(
    SUM(
      CASE
        WHEN event_timestamp - install_timestamp between 0
        and 7 * 24 * 3600 THEN revenue_value_usd
        ELSE 0
      END
    ),
    0
  ) as 168h_revenue_usd,
  COALESCE(
    SUM(
      CASE
        WHEN event_timestamp - install_timestamp between 0
        and 14 * 24 * 3600 THEN revenue_value_usd
        ELSE 0
      END
    ),
    0
  ) as 336h_revenue_usd
FROM
  rg_bi.ads_lastwar_ios_purchase_adv
WHERE
  install_day between {startDayStr}
  and {endDayStr}
GROUP BY
  game_uid,
  install_day;

@t2 := SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY install_day ORDER BY RAND()) AS row_num
FROM
    @t1
;

select
  install_day,
  count(*) as user_num,
  sum(24h_revenue_usd) as 24h_revenue_usd,
  sum(48h_revenue_usd) as 48h_revenue_usd,
  sum(168h_revenue_usd) as 168h_revenue_usd,
  sum(336h_revenue_usd) as 336h_revenue_usd
FROM
  @t2
where
  row_num <= {num}
group by
  install_day;
'''
        # print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename)
    return df


def getTwIosSumPayData(startDayStr,endDayStr):
    filename = f'/src/data/twIosPayData_48hours_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):    
        sql = f'''
SELECT
    install_day,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp - install_timestamp between 0
                and 24 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as 24h_revenue_usd,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp - install_timestamp between 0
                and 48 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as 48h_revenue_usd,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp - install_timestamp between 0
                and 7 * 24 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as 168h_revenue_usd,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp - install_timestamp between 0
                and 14 * 24 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as 336h_revenue_usd
FROM
    rg_bi.ads_topwar_ios_purchase_adv
WHERE
    install_day between {startDayStr} and {endDayStr}
GROUP BY
    install_day;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename)
    return df

def getTwIosPayDataRAND(startDayStr,endDayStr,rand = 0.1):
    filename = f'/src/data/twIosPayData_48hours_{startDayStr}_{endDayStr}_rand{rand}.csv'
    if not os.path.exists(filename):    
        sql = f'''
SET
  odps.sql.timezone = Africa / Accra;

set
  odps.sql.hive.compatible = true;

set odps.sql.executionengine.enable.rand.time.seed=true;

@t1 :=
SELECT
  install_day,
  game_uid as uid,
  COALESCE(
    SUM(
      CASE
        WHEN event_timestamp - install_timestamp between 0
        and 24 * 3600 THEN revenue_value_usd
        ELSE 0
      END
    ),
    0
  ) as 24h_revenue_usd,
  COALESCE(
    SUM(
      CASE
        WHEN event_timestamp - install_timestamp between 0
        and 48 * 3600 THEN revenue_value_usd
        ELSE 0
      END
    ),
    0
  ) as 48h_revenue_usd,
  COALESCE(
    SUM(
      CASE
        WHEN event_timestamp - install_timestamp between 0
        and 7 * 24 * 3600 THEN revenue_value_usd
        ELSE 0
      END
    ),
    0
  ) as 168h_revenue_usd,
  COALESCE(
    SUM(
      CASE
        WHEN event_timestamp - install_timestamp between 0
        and 14 * 24 * 3600 THEN revenue_value_usd
        ELSE 0
      END
    ),
    0
  ) as 336h_revenue_usd
FROM
  rg_bi.ads_topwar_ios_purchase_adv
WHERE
  install_day between {startDayStr}
  and {endDayStr}
GROUP BY
  game_uid,
  install_day;

select
  install_day,
  sum(24h_revenue_usd) as 24h_revenue_usd,
  sum(48h_revenue_usd) as 48h_revenue_usd,
  sum(168h_revenue_usd) as 168h_revenue_usd,
  sum(336h_revenue_usd) as 336h_revenue_usd
FROM
  @t1
where
  rand() < {rand}
group by
  install_day;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename)
    return df

# 抽样，抽样num个用户，num是每天的用户数
def getTwIosPayDataRANDNum(startDayStr,endDayStr,num = 1000):
    filename = f'/src/data/twIosPayData_48hours_{startDayStr}_{endDayStr}_randNum{num}.csv'
    if not os.path.exists(filename):    
        sql = f'''
SET
  odps.sql.timezone = Africa / Accra;

set
  odps.sql.hive.compatible = true;

set odps.sql.executionengine.enable.rand.time.seed=true;

@t1 :=
SELECT
  install_day,
  game_uid as uid,
  COALESCE(
    SUM(
      CASE
        WHEN event_timestamp - install_timestamp between 0
        and 24 * 3600 THEN revenue_value_usd
        ELSE 0
      END
    ),
    0
  ) as 24h_revenue_usd,
  COALESCE(
    SUM(
      CASE
        WHEN event_timestamp - install_timestamp between 0
        and 48 * 3600 THEN revenue_value_usd
        ELSE 0
      END
    ),
    0
  ) as 48h_revenue_usd,
  COALESCE(
    SUM(
      CASE
        WHEN event_timestamp - install_timestamp between 0
        and 7 * 24 * 3600 THEN revenue_value_usd
        ELSE 0
      END
    ),
    0
  ) as 168h_revenue_usd,
  COALESCE(
    SUM(
      CASE
        WHEN event_timestamp - install_timestamp between 0
        and 14 * 24 * 3600 THEN revenue_value_usd
        ELSE 0
      END
    ),
    0
  ) as 336h_revenue_usd
FROM
  rg_bi.ads_topwar_ios_purchase_adv
WHERE
  install_day between {startDayStr}
  and {endDayStr}
GROUP BY
  game_uid,
  install_day;

@t2 := SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY install_day ORDER BY RAND()) AS row_num
FROM
    @t1
;

select
  install_day,
  count(*) as user_num,
  sum(24h_revenue_usd) as 24h_revenue_usd,
  sum(48h_revenue_usd) as 48h_revenue_usd,
  sum(168h_revenue_usd) as 168h_revenue_usd,
  sum(336h_revenue_usd) as 336h_revenue_usd
FROM
  @t2
where
  row_num <= {num}
group by
  install_day;
'''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename)
    return df

def setp1():
    startDayStr = '20240101'
    endDayStr = '20240310'

    lwDf = getLwIosSumPayData(startDayStr,endDayStr)
    twDf = getTwIosSumPayData(startDayStr,endDayStr)

    print('lw 相关系数')
    lwCorr = lwDf.corr()
    print(lwCorr.loc[['168h_revenue_usd', '336h_revenue_usd'], ['24h_revenue_usd', '48h_revenue_usd']])

    print('tw 相关系数')
    twCorr = twDf.corr()
    print(twCorr.loc[['168h_revenue_usd', '336h_revenue_usd'], ['24h_revenue_usd', '48h_revenue_usd']])


    lwRANDDf = getLwIosPayDataRAND(startDayStr,endDayStr)
    twRANDDf = getTwIosPayDataRAND(startDayStr,endDayStr)

    print('lw 随机抽样10% 相关系数')
    lwRANDCorr = lwRANDDf.corr()
    print(lwRANDCorr.loc[['168h_revenue_usd', '336h_revenue_usd'], ['24h_revenue_usd', '48h_revenue_usd']])

    print('tw 随机抽样10% 相关系数')
    twRANDCorr = twRANDDf.corr()
    print(twRANDCorr.loc[['168h_revenue_usd', '336h_revenue_usd'], ['24h_revenue_usd', '48h_revenue_usd']])
          

    lwRANDNumDf = getLwIosPayDataRANDNum(startDayStr,endDayStr,20000)
    twRANDNumDf = getTwIosPayDataRANDNum(startDayStr,endDayStr,20000)

    print('lw 随机抽样20000 相关系数')
    lwRANDNumCorr = lwRANDNumDf.corr()
    print(lwRANDNumCorr.loc[['168h_revenue_usd', '336h_revenue_usd'], ['24h_revenue_usd', '48h_revenue_usd']])

    print('tw 随机抽样20000 相关系数')
    twRANDNumCorr = twRANDNumDf.corr()
    print(twRANDNumCorr.loc[['168h_revenue_usd', '336h_revenue_usd'], ['24h_revenue_usd', '48h_revenue_usd']])

def debug1():
    startDayStr = '20240101'
    endDayStr = '20240310'

    for i in range(10):
        lwRANDNumDf = getLwIosPayDataRANDNum(startDayStr,endDayStr,20000,readDb = True)

        print('lw 随机抽样20000 相关系数')
        lwRANDNumCorr = lwRANDNumDf.corr()
        print(lwRANDNumCorr.loc[['168h_revenue_usd', '336h_revenue_usd'], ['24h_revenue_usd', '48h_revenue_usd']])



# 2，找到相关性差异的来源
    

if __name__ == '__main__':
    # setp1()
    debug1()