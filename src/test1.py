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

mediaList = [
    {'name':'bytedance','codeList':['bytedanceglobal_int'],'sname':'Bd'},
    {'name':'facebook','codeList':['Social_facebook','restricted','Facebook Ads','facebook','FacebookAds'],'sname':'Fb'},
    {'name':'google','codeList':['googleadwords_int'],'sname':'Gg'},
    {'name':'snapchat','codeList':['snapchat_int'],'sname':'Sc'},
    {'name':'unknown','codeList':[],'sname':'Og'}
]

# 写一个函数名为addMediaGroup，参数名为df
# df是pandas的DataFrame
# 在df中添加一列名为media_group，默认值unknown
# df中media列的值 如果属于mediaList的某个成员的codeList中，则这一行中的media_group等于这个成员的name
def addMediaGroup(df):
    # Initialize the media_group column with default value 'unknown'
    df['media_group'] = 'unknown'

    # Define the mediaList
    mediaList = [
        {'name': 'group1', 'codeList': ['code1', 'code2', 'code3']},
        {'name': 'group2', 'codeList': ['code4', 'code5', 'code6']},
        # Add more groups if needed
    ]

    # Iterate through the mediaList and update the media_group column accordingly
    for group in mediaList:
        df.loc[df['media'].isin(group['codeList']), 'media_group'] = group['name']

    return df

# 写一个函数名为dataFill，参数名为df
# df是pandas的DataFrame，拥有列'install_date','cv','media_group'
# 要求install_date和media_group的每个不同的值都要有对应的从0到63的cv对应行，如果没有，添加一行

def dataFill(df):
    # Get unique values of 'install_date' and 'media_group'
    install_dates = df['install_date'].unique()
    media_groups = df['media_group'].unique()

    # Create a new DataFrame with all possible combinations of 'install_date', 'cv', and 'media_group'
    new_df = pd.DataFrame(columns=['install_date', 'cv', 'media_group'])
    for install_date in install_dates:
        for media_group in media_groups:
            for cv in range(64):
                new_df = new_df.append({'install_date': install_date, 'cv': cv, 'media_group': media_group}, ignore_index=True)

    # Merge the original DataFrame with the new DataFrame, filling missing values with 0
    merged_df = pd.merge(new_df, df, on=['install_date', 'cv', 'media_group'], how='left').fillna(0)

    return merged_df