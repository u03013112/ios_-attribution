# 引入所需头文件
import pandas as pd
# 写一个函数，名为getDataFromAF，参数1名为cvMap。
# cvMap是pandas的DataFrame
# whenStr = ''
# 遍历cvMap,i做索引
# min_event_revenue = cvMap.min_event_revenue[i]
# max_event_revenue = cvMap.max_event_revenue[i]
# 如果 min_event_revenue 或 max_event_revenue 是空值就continue
# whenStr += 'when r1usd>%d and r1usd<=%d then %d\n'%(min_event_revenue, max_event_revenue,i)
def getDataFromAF(cvMap):
    whenStr = ''
    for i in range(len(cvMap)):
        min_event_revenue = cvMap.min_event_revenue[i]
        max_event_revenue = cvMap.max_event_revenue[i]
        if pd.isnull(min_event_revenue) or pd.isnull(max_event_revenue):
            continue
        whenStr += 'when r1usd>%d and r1usd<=%d then %d\n' % (min_event_revenue, max_event_revenue, i)
    return whenStr



# 写一个sql字符串，名为sql
# 从ods_platform_appsflyer_events表获取appsflyer_id as uid
# install_time 字符串截断 从"yyyy-mm-dd hh:mi:ss"截断成"yyyy-mm-dd" as install_date
# event_timestamp - install_timestamp <= 1 * 24 * 3600 的 event_revenue_usd 求和 as r1usd
# event_timestamp - install_timestamp <= 7 * 24 * 3600 的 event_revenue_usd 求和 as r7usd
# media_source as media
# where
#     app_id = 'com.topwar.gp'
#     and zone = 0
#     and day >= 20220501
#     and day <= 20230301
#     and install_time >= '2022-05-01'
#     and install_time < '2023-02-01'

sql = """
SELECT appsflyer_id as uid,
       SUBSTRING(install_time, 1, 10) as install_date,
       SUM(CASE WHEN event_timestamp - install_timestamp <= 1 * 24 * 3600 THEN event_revenue_usd ELSE 0 END) as r1usd,
       SUM(CASE WHEN event_timestamp - install_timestamp <= 7 * 24 * 3600 THEN event_revenue_usd ELSE 0 END) as r7usd,
       media_source as media
FROM ods_platform_appsflyer_events
WHERE
    app_id = 'com.topwar.gp'
    AND zone = 0
    AND day >= 20220501
    AND day <= 20230301
    AND install_time >= '2022-05-01'
    AND install_time < '2023-02-01'
GROUP BY appsflyer_id, media_source, SUBSTRING(install_time, 1, 10)
"""