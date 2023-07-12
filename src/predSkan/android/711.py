# 主要国家的用户质量
# 可以将一些相邻国家放在一起，台湾、香港、澳门
# 7，30，60，90，180日用户质量

# 媒体质量
# fb，gg，tk
# 同上
import json
import pandas as pd

import sys
sys.path.append('/src')

# 2022年6月之前的数据
# 2022-01-01~2022-06-01

from src.predSkan.lize.demoSS import ssSql

# 用数数获得支付金额
# 由于需要180天数据，AF不一定全面，所以用数数来获取
def ss01():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0,count(data_map_1) over () group_num_1,count(data_map_2) over () group_num_2,count(data_map_3) over () group_num_3,count(data_map_4) over () group_num_4 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", amount_1) filter (where amount_1 is not null and is_finite(amount_1) ) data_map_1,map_agg("$__Date_Time", amount_2) filter (where amount_2 is not null and is_finite(amount_2) ) data_map_2,map_agg("$__Date_Time", amount_3) filter (where amount_3 is not null and is_finite(amount_3) ) data_map_3,map_agg("$__Date_Time", amount_4) filter (where amount_4 is not null and is_finite(amount_4) ) data_map_4,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0,internal_amount_1 amount_1,internal_amount_2 amount_2,internal_amount_3 amount_3,internal_amount_4 amount_4 from (select group_0,"$__Date_Time",cast(coalesce(SUM(if((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 7),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_0,cast(coalesce(SUM(if((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 3E+1),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_1,cast(coalesce(SUM(if((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 6E+1),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_2,cast(coalesce(SUM(if((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 9E+1),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_3,cast(coalesce(SUM(if((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 1.8E+2),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_4 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("order_id@status" = -1), 0, ("order_currency@usdvalue" * "order_money_amount"))) as double) "#vp@usd_amount" from (select *, try_cast(try((date_diff('day', date("internal_u@ctime"), date("#event_time")) + 1)) as double) "#vp@lifetime",try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select a.*, b."ctime" "internal_u@ctime" from (select * from (select "#event_name","#event_time","order_id","order_currency","#user_id","platform","order_money_amount","$part_date","$part_event" from v_event_2) logic_table left join ta_dim."dim_2_0_1240" on logic_table."order_currency" = "dim_2_0_1240"."order_currency@order_currency" left join ta_dim."dim_2_0_1242" on logic_table."order_id" = "dim_2_0_1242"."order_id@order_id") a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20211225) b on a."#user_id"=b."#user_id"))))) ta_ev inner join (select *, "country" group_0 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#update_time","#event_date","#user_id","ctime","country","firstplatform" from v_user_2) where "#event_date" > 20211225))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ("$part_event" in ('PAY_SUCCESS_REALTIME')) and (((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 7)) or ((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 3E+1)) or ((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 6E+1)) or ((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 9E+1)) or ((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 1.8E+2))) and ((("$part_date" between '2021-12-31' and '2023-01-02') and ("@vpc_tz_#event_time" >= timestamp '2022-01-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-01-01'))) and ((ta_u."firstplatform" IN ('GooglePlay')) and ((ta_u."#vp@ctime_utc0" >= cast('2022-01-01 00:00:00' as timestamp) AND ta_u."#vp@ctime_utc0" <= cast('2022-06-01 23:59:59' as timestamp))))) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC
    '''
    lines = ssSql(sql=sql)
    # print(lines[0:10])
    # 返回值类似：
    # [ 
    #     '["QA",{"1981-01-01 00:00:00":1850963.0370310459},{"1981-01-01 00:00:00":8066525.606620111},{"1981-01-01 00:00:00":8829844.555934437},{"1981-01-01 00:00:00":8919761.973545574},{"1981-01-01 00:00:00":8930364.457218457},1850963.0370310459,216,216,216,216,216]', 
    #     '["US",{"1981-01-01 00:00:00":625580.1084732119},{"1981-01-01 00:00:00":1621832.069525447},{"1981-01-01 00:00:00":2560304.7399923205},{"1981-01-01 00:00:00":3376716.4508603215},{"1981-01-01 00:00:00":4899758.319011365},625580.1084732119,216,216,216,216,216]', 
    #     '["KR",{"1981-01-01 00:00:00":460040.35108669417},{"1981-01-01 00:00:00":1443255.2569563754},{"1981-01-01 00:00:00":2558670.3098786273},{"1981-01-01 00:00:00":3462478.6657366534},{"1981-01-01 00:00:00":5244846.632070654},460040.35108669417,216,216,216,216,216]', 
    #     '["JP",{"1981-01-01 00:00:00":233486.65813364135},{"1981-01-01 00:00:00":668142.0543253109},{"1981-01-01 00:00:00":1246988.9746725867},{"1981-01-01 00:00:00":1809602.9438379565},{"1981-01-01 00:00:00":3207119.815650103},233486.65813364135,216,216,216,216,216]', 
    #     '["NL",{"1981-01-01 00:00:00":165518.94055290683},{"1981-01-01 00:00:00":448840.0486526649},{"1981-01-01 00:00:00":791973.6463042843},{"1981-01-01 00:00:00":908318.571558636},{"1981-01-01 00:00:00":1003062.0433179213},165518.94055290683,216,216,216,216,216]', 
    #     '["DE",{"1981-01-01 00:00:00":127596.37724884227},{"1981-01-01 00:00:00":308192.68664645014},{"1981-01-01 00:00:00":494592.6664010321},{"1981-01-01 00:00:00":633331.336567545},{"1981-01-01 00:00:00":905907.1233588547},127596.37724884227,216,216,216,216,216]', 
    #     '["GB",{"1981-01-01 00:00:00":102094.58750395787},{"1981-01-01 00:00:00":235886.287198263},{"1981-01-01 00:00:00":364640.8058789465},{"1981-01-01 00:00:00":468074.27536902175},{"1981-01-01 00:00:00":674152.2205342331},102094.58750395787,216,216,216,216,216]', 
    #     '["IL",{"1981-01-01 00:00:00":95123.70470677993},{"1981-01-01 00:00:00":385445.3628646829},{"1981-01-01 00:00:00":506197.7841068047},{"1981-01-01 00:00:00":532436.2673600051},{"1981-01-01 00:00:00":543030.5207152888},95123.70470677993,216,216,216,216,216]', 
    #     '["FR",{"1981-01-01 00:00:00":92650.8268718085},{"1981-01-01 00:00:00":223150.65571007848},{"1981-01-01 00:00:00":341169.603015538},{"1981-01-01 00:00:00":430304.4796795142},{"1981-01-01 00:00:00":648407.9659916877},92650.8268718085,216,216,216,216,216]', 
    #     '["TW",{"1981-01-01 00:00:00":91210.77276049659},{"1981-01-01 00:00:00":220252.11533401653},{"1981-01-01 00:00:00":367666.3773236729},{"1981-01-01 00:00:00":491638.10172814183},{"1981-01-01 00:00:00":698184.2730631767},91210.77276049659,216,216,216,216,216]'
    # ]
    # 初始化一个空的DataFrame
    df = pd.DataFrame(columns=['code', 'r7usd', 'r30usd', 'r60usd', 'r90usd', 'r180usd'])

    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue
        # 国家代码
        code = lineJ[0]
        # r7usd
        r7usd = next(iter(lineJ[1].values())) 
        # r30usd
        r30usd = next(iter(lineJ[2].values()))
        # r60usd
        r60usd = next(iter(lineJ[3].values()))
        # r90usd
        r90usd = next(iter(lineJ[4].values()))
        # r180usd
        r180usd = next(iter(lineJ[5].values()))

        # 创建一个新的DataFrame并添加到原始DataFrame中
        df_new = pd.DataFrame([[code, r7usd, r30usd, r60usd, r90usd, r180usd]], columns=df.columns)
        df = df.append(df_new, ignore_index=True)

    df.to_csv('/src/data/711data1.csv', index=False)

def main1():
    # 读取数据
    df = pd.read_csv('/src/data/711data1.csv')
    # 计算所有国家的和，计算r30usd/r7usd,r60usd/r7usd,r90usd/r7usd,r180usd/r7usd
    dfTotal = df.sum()
    print('大盘均值：r30usd/r7usd = %.2f, r60usd/r7usd = %.2f, r90usd/r7usd = %.2f, r180usd/r7usd = %.2f' % (dfTotal['r30usd']/dfTotal['r7usd'], dfTotal['r60usd']/dfTotal['r7usd'], dfTotal['r90usd']/dfTotal['r7usd'], dfTotal['r180usd']/dfTotal['r7usd']))

    # 对国家进行一定的汇总
    # df中添加一列，默认等于code
    df['code2'] = df['code']
    # GCC: 中东6国都算作GCC
    df.loc[df['code'].isin(['BH', 'KW', 'OM', 'QA', 'SA', 'AE']), 'code2'] = 'GCC'
    # CN:台湾，香港，澳门都算作CN
    df.loc[df['code'].isin(['TW', 'HK', 'MO']), 'code2'] = 'CN'
    # EU: 欧盟28国都算作EU
    df.loc[df['code'].isin(['AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE', 'GB']), 'code2'] = 'EU'
    # SEA（东南亚）: ID, MY, PH, SG, TH, VN
    df.loc[df['code'].isin(['ID', 'MY', 'PH', 'SG', 'TH', 'VN']), 'code2'] = 'SEA'

    # 按照code2进行分组，计算每组的均值，只计算按照code2分组后按照r7usd排序后的前10个国家的r30usd/r7usd,r60usd/r7usd,r90usd/r7usd,r180usd/r7usd
    # 对DataFrame按照code2进行分组
    # 对DataFrame按照code2进行分组并计算每个分组的总和
    grouped_sum = df.groupby('code2').sum()

    # 按照r7usd排序并选择前10个分组
    top_10_groups = grouped_sum.sort_values(by='r7usd', ascending=False).head(10)

    # 计算指标
    top_10_groups['r30usd/r7usd'] = top_10_groups['r30usd'] / top_10_groups['r7usd']
    top_10_groups['r60usd/r7usd'] = top_10_groups['r60usd'] / top_10_groups['r7usd']
    top_10_groups['r90usd/r7usd'] = top_10_groups['r90usd'] / top_10_groups['r7usd']
    top_10_groups['r180usd/r7usd'] = top_10_groups['r180usd'] / top_10_groups['r7usd']

    top_10_groups.drop(['r7usd', 'r30usd', 'r60usd', 'r90usd', 'r180usd'], axis=1, inplace=True)

    # 打印结果
    print(top_10_groups)

def ss02():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0,count(data_map_1) over () group_num_1,count(data_map_2) over () group_num_2,count(data_map_3) over () group_num_3,count(data_map_4) over () group_num_4 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", amount_1) filter (where amount_1 is not null and is_finite(amount_1) ) data_map_1,map_agg("$__Date_Time", amount_2) filter (where amount_2 is not null and is_finite(amount_2) ) data_map_2,map_agg("$__Date_Time", amount_3) filter (where amount_3 is not null and is_finite(amount_3) ) data_map_3,map_agg("$__Date_Time", amount_4) filter (where amount_4 is not null and is_finite(amount_4) ) data_map_4,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0,internal_amount_1 amount_1,internal_amount_2 amount_2,internal_amount_3 amount_3,internal_amount_4 amount_4 from (select group_0,"$__Date_Time",cast(coalesce(SUM(if((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 7),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_0,cast(coalesce(SUM(if((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 3E+1),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_1,cast(coalesce(SUM(if((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 6E+1),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_2,cast(coalesce(SUM(if((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 9E+1),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_3,cast(coalesce(SUM(if((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 1.8E+2),ta_ev."#vp@usd_amount")), 0) as double) internal_amount_4 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("order_id@status" = -1), 0, ("order_currency@usdvalue" * "order_money_amount"))) as double) "#vp@usd_amount" from (select *, try_cast(try((date_diff('day', date("internal_u@ctime"), date("#event_time")) + 1)) as double) "#vp@lifetime",try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone" from (select a.*, b."ctime" "internal_u@ctime" from (select * from (select "#event_name","#event_time","order_id","order_currency","#user_id","platform","order_money_amount","$part_date","$part_event" from v_event_2) logic_table left join ta_dim."dim_2_0_1240" on logic_table."order_currency" = "dim_2_0_1240"."order_currency@order_currency" left join ta_dim."dim_2_0_1242" on logic_table."order_id" = "dim_2_0_1242"."order_id@order_id") a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20211225) b on a."#user_id"=b."#user_id"))))) ta_ev inner join (select *, "ad_mediasource" group_0 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select * from (select "#update_time","#event_date","#user_id","ctime","ad_mediasource","firstplatform" from v_user_2) where "#event_date" > 20211225))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ("$part_event" in ('PAY_SUCCESS_REALTIME')) and (((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 7)) or ((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 3E+1)) or ((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 6E+1)) or ((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 9E+1)) or ((( ( "$part_event" IN ( 'PAY_SUCCESS_REALTIME' ) ) )) and (ta_ev."#vp@lifetime" <= 1.8E+2))) and ((("$part_date" between '2021-12-31' and '2023-01-02') and ("@vpc_tz_#event_time" >= timestamp '2022-01-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-01-01'))) and ((ta_u."firstplatform" IN ('GooglePlay')) and ((ta_u."#vp@ctime_utc0" >= cast('2022-01-01 00:00:00' as timestamp) AND ta_u."#vp@ctime_utc0" <= cast('2022-06-01 23:59:59' as timestamp))))) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC
    '''
    lines = ssSql(sql=sql)
    
    # 初始化一个空的DataFrame
    df = pd.DataFrame(columns=['media', 'r7usd', 'r30usd', 'r60usd', 'r90usd', 'r180usd'])

    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue
        media = lineJ[0]
        # r7usd
        r7usd = next(iter(lineJ[1].values())) 
        # r30usd
        r30usd = next(iter(lineJ[2].values()))
        # r60usd
        r60usd = next(iter(lineJ[3].values()))
        # r90usd
        r90usd = next(iter(lineJ[4].values()))
        # r180usd
        r180usd = next(iter(lineJ[5].values()))

        # 创建一个新的DataFrame并添加到原始DataFrame中
        df_new = pd.DataFrame([[media, r7usd, r30usd, r60usd, r90usd, r180usd]], columns=df.columns)
        df = df.append(df_new, ignore_index=True)

    df.to_csv('/src/data/711data2.csv', index=False)

def main2():
    # 读取数据
    df = pd.read_csv('/src/data/711data2.csv')

    dfTotal = df.sum()
    print('大盘均值：r30usd/r7usd = %.2f, r60usd/r7usd = %.2f, r90usd/r7usd = %.2f, r180usd/r7usd = %.2f' % (dfTotal['r30usd']/dfTotal['r7usd'], dfTotal['r60usd']/dfTotal['r7usd'], dfTotal['r90usd']/dfTotal['r7usd'], dfTotal['r180usd']/dfTotal['r7usd']))

    # df中media列的值，'restricted' replace 为 'Facebook Ads'
    df['media'].replace('restricted', 'Facebook Ads', inplace=True)
    df['media'].fillna('organic', inplace=True)
    df = df.groupby('media').sum().reset_index()

    # 只对facebook,google,tiktok感兴趣
    df = df[df['media'].isin(['Facebook Ads', 'googleadwords_int', 'bytedanceglobal_int','organic'])]

    # 按照r7usd排序并选择前10个分组
    top_10_groups = df.sort_values(by='r7usd', ascending=False).head(10)

    # 计算指标
    top_10_groups['r30usd/r7usd'] = top_10_groups['r30usd'] / top_10_groups['r7usd']
    top_10_groups['r60usd/r7usd'] = top_10_groups['r60usd'] / top_10_groups['r7usd']
    top_10_groups['r90usd/r7usd'] = top_10_groups['r90usd'] / top_10_groups['r7usd']
    top_10_groups['r180usd/r7usd'] = top_10_groups['r180usd'] / top_10_groups['r7usd']

    top_10_groups.drop(['r7usd', 'r30usd', 'r60usd', 'r90usd', 'r180usd'], axis=1, inplace=True)

    # 打印结果
    # 设置display.width选项
    pd.set_option('display.width', 1000)
    print(top_10_groups.to_string(index=False))

    
if __name__ == '__main__':
    # ss01()
    # main1()

    # ss02()
    main2()