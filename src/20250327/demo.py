# 试玩素材打点分析
import os
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql



def getData1():
    filename = '/src/data/20250327_getData1.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = '''
select
	event_name,
	get_json_object(event_params_json, '$.value') as value,
	get_json_object(event_params_json, '$.media') as media,
	get_json_object(event_params_json, '$.material') as material,
	get_json_object(device_json, '$.operating_system') as os,
	count(*) as cnt
from
	ods_platform_playable_ga4
where
	day between '20250101'
	and '20250326'
	and event_name in ('loading', 'game_start', 'game_end', 'actionbar')
	and get_json_object(device_json, '$.operating_system') in ('Android', 'iOS')
	and get_json_object(event_params_json, '$.media') in ('Moloco')
	and get_json_object(event_params_json, '$.appid') in ('LW')
group by
	event_name,
	get_json_object(event_params_json, '$.value'),
	get_json_object(event_params_json, '$.media'),
	get_json_object(event_params_json, '$.material'),
	get_json_object(device_json, '$.operating_system')
;
        '''
        df = execSql(sql)
        df.to_csv(filename, index=False)
    
    return df

# 漏斗分析
def analysis1():
    df = getData1()
    # 计算每个素材的漏斗分析
    # 漏斗：loading -> game_start -> game_end -> actionbar
    # 先不管其他列，按照素材分组，计算每个素材的漏斗分析，其他列舍弃
    
    # 按照素材分组
    grouped = df.groupby('material')
    
    # 初始化一个列表来存储漏斗分析结果
    funnel_results = []

    # 定义漏斗步骤
    funnel_steps = ['loading', 'game_start', 'game_end', 'actionbar']

    for material, group in grouped:
        # 初始化一个字典来存储每个素材的漏斗数据
        funnel_data = {'Material': material}
        
        for step in funnel_steps:
            # 获取每个步骤的计数
            count = group[group['event_name'] == step]['cnt'].sum()
            funnel_data[step] = count
        
        # 过滤掉 actionbar < 10 的素材
        if funnel_data['actionbar'] >= 10:
            funnel_results.append(funnel_data)
    
    # 将结果保存到 DataFrame
    result_df = pd.DataFrame(funnel_results, columns=['Material', 'loading', 'game_start', 'game_end', 'actionbar'])
    
    # 保存到 CSV
    result_df.to_csv('/src/data/20250327_analysis1.csv', index=False)

def getData2():
    filename = '/src/data/20250327_getData2.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = '''
select
    get_json_object(event_params_json, '$.material') as material,
    count(distinct case when event_name = 'loading' then get_json_object(event_params_json, '$.uuid') end) as loading,
    count(distinct case when event_name = 'game_start' then get_json_object(event_params_json, '$.uuid') end) as game_start,
    count(distinct case when event_name = 'game_end' then get_json_object(event_params_json, '$.uuid') end) as game_end,
    count(distinct case when event_name = 'actionbar' then get_json_object(event_params_json, '$.uuid') end) as actionbar,
    count(distinct case when event_name = 'actionbar' and get_json_object(event_params_json, '$.uuid') in (
        select get_json_object(event_params_json, '$.uuid') from ods_platform_playable_ga4
        where 
            event_name = 'game_end'
            and day between '20250101' and '20250326'
    ) then get_json_object(event_params_json, '$.uuid') end) as actionbar_with_game_end
from
    ods_platform_playable_ga4
where
    day between '20250101' and '20250326'
    and event_name in ('loading', 'game_start', 'game_end', 'actionbar')
    and get_json_object(device_json, '$.operating_system') in ('Android', 'iOS')
    and get_json_object(event_params_json, '$.media') = 'Moloco'
    and get_json_object(event_params_json, '$.appid') = 'LW'
group by
    get_json_object(event_params_json, '$.material')
having
    count(distinct case when event_name = 'actionbar' then get_json_object(event_params_json, '$.uuid') end) >= 10
;

        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    
    return df

def analysis2():
    df = getData2()
    
    # 保存到 CSV
    df.to_csv('/src/data/20250327_analysis2.csv', index=False)

def getData3():
    filename = '/src/data/20250327_getData3.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = '''
select
    floor((game_start_timestamp - loading_timestamp) / 1000000) as delta_time_second,
    count(distinct uuid) as uuid_cnt
from (
    select
        get_json_object(event_params_json, '$.uuid') as uuid,
        max(case when event_name = 'loading' then event_timestamp end) as loading_timestamp,
        max(case when event_name = 'game_start' then event_timestamp end) as game_start_timestamp
    from
        ods_platform_playable_ga4
    where
        day between '20250101' and '20250326'
        and event_name in ('loading', 'game_start')
        and get_json_object(device_json, '$.operating_system') in ('Android', 'iOS')
        and get_json_object(event_params_json, '$.media') = 'Moloco'
        and get_json_object(event_params_json, '$.appid') = 'LW'
    group by
        get_json_object(event_params_json, '$.uuid')
) as t
where
    loading_timestamp is not null
    and game_start_timestamp is not null
group by
    floor((game_start_timestamp - loading_timestamp) / 1000000)
;
        '''
        df = execSql(sql)
        df.to_csv(filename, index=False)
    
    return df

def analysis3():
    df = getData3()
    df = df[(df['delta_time_second'] > 0) & (df['delta_time_second'] < 3600)]

    # 保存到 CSV
    df.to_csv('/src/data/20250327_analysis3.csv', index=False)

def getData4():
    filename = '/src/data/20250327_getData4.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = '''
        select
            t1.value,
            floor(t1.game_duration / 10000) as duration_group,
            count(distinct t1.uuid) as uuid_cnt,
            sum(case when t2.actionbar = 1 then 1 else 0 end) / count(distinct t1.uuid) as conversion_rate
        from (
            select
                get_json_object(event_params_json, '$.uuid') as uuid,
                get_json_object(event_params_json, '$.value') as value,
                get_json_object(event_params_json, '$.game_duration') as game_duration
            from
                ods_platform_playable_ga4
            where
                day between '20250101' and '20250326'
                and event_name = 'game_end'
                and get_json_object(event_params_json, '$.media') = 'Moloco'
                and get_json_object(event_params_json, '$.appid') = 'LW'
        ) as t1
        left join (
            select
                get_json_object(event_params_json, '$.uuid') as uuid,
                1 as actionbar
            from
                ods_platform_playable_ga4
            where
                day between '20250101' and '20250326'
                and event_name = 'actionbar'
                and get_json_object(event_params_json, '$.media') = 'Moloco'
                and get_json_object(event_params_json, '$.appid') = 'LW'
        ) as t2
        on t1.uuid = t2.uuid
        group by
            t1.value,
            floor(t1.game_duration / 10000)
        ;
        '''
        df = execSql(sql)
        df.to_csv(filename, index=False)
    
    return df

def analysis4():
    df = getData4()
    
    # 保存到 CSV
    df.to_csv('/src/data/20250327_analysis4.csv', index=False)

def data4_value():
    filename = '/src/data/20250327_data4_value.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = '''
        select
            t1.value,
            count(distinct t1.uuid) as uuid_cnt,
            sum(case when t2.actionbar = 1 then 1 else 0 end) / count(distinct t1.uuid) as conversion_rate
        from (
            select
                get_json_object(event_params_json, '$.uuid') as uuid,
                get_json_object(event_params_json, '$.value') as value
            from
                ods_platform_playable_ga4
            where
                day between '20250101' and '20250326'
                and event_name = 'game_end'
                and get_json_object(device_json, '$.operating_system') in ('Android', 'iOS')
                and get_json_object(event_params_json, '$.media') = 'Moloco'
                and get_json_object(event_params_json, '$.appid') = 'LW'
        ) as t1
        left join (
            select
                get_json_object(event_params_json, '$.uuid') as uuid,
                1 as actionbar
            from
                ods_platform_playable_ga4
            where
                day between '20250101' and '20250326'
                and event_name = 'actionbar'
                and get_json_object(device_json, '$.operating_system') in ('Android', 'iOS')
                and get_json_object(event_params_json, '$.media') = 'Moloco'
                and get_json_object(event_params_json, '$.appid') = 'LW'
        ) as t2
        on t1.uuid = t2.uuid
        group by
            t1.value
        ;
        '''
        df = execSql(sql)
        df.to_csv(filename, index=False)
    
    return df

def data4_duration():
    filename = '/src/data/20250327_data4_duration.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = '''
        select
            floor(t1.game_duration / 10000) as duration_group,
            count(distinct t1.uuid) as uuid_cnt,
            sum(case when t2.actionbar = 1 then 1 else 0 end) / count(distinct t1.uuid) as conversion_rate
        from (
            select
                get_json_object(event_params_json, '$.uuid') as uuid,
                get_json_object(event_params_json, '$.game_duration') as game_duration
            from
                ods_platform_playable_ga4
            where
                day between '20250101' and '20250326'
                and event_name = 'game_end'
                and get_json_object(device_json, '$.operating_system') in ('Android', 'iOS')
                and get_json_object(event_params_json, '$.media') = 'Moloco'
                and get_json_object(event_params_json, '$.appid') = 'LW'
        ) as t1
        left join (
            select
                get_json_object(event_params_json, '$.uuid') as uuid,
                1 as actionbar
            from
                ods_platform_playable_ga4
            where
                day between '20250101' and '20250326'
                and event_name = 'actionbar'
                and get_json_object(device_json, '$.operating_system') in ('Android', 'iOS')
                and get_json_object(event_params_json, '$.media') = 'Moloco'
                and get_json_object(event_params_json, '$.appid') = 'LW'
        ) as t2
        on t1.uuid = t2.uuid
        group by
            floor(t1.game_duration / 10000)
        ;
        '''
        df = execSql(sql)
        df.to_csv(filename, index=False)
    
    return df

def analysis_value():
    df = data4_value()
    df.to_csv('/src/data/20250327_analysis_value.csv', index=False)

def analysis_duration():
    df = data4_duration()
    df.to_csv('/src/data/20250327_analysis_duration.csv', index=False)


if __name__ == '__main__':
    # analysis1()
    # analysis2()
    # analysis3()
    # analysis4()
    analysis_value()
    analysis_duration()