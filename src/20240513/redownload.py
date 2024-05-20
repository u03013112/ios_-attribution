# 分campaign比对 redownload分布





sql = '''
select
    skad_ad_network_id,
    skad_campaign_id,
    sum(
    case
    when skad_redownload = 'false' then 1
    else 0
    end
    ) as first_install_count,
    count(*) as total_install_count
from
    ods_platform_appsflyer_skad_postbacks_copy
where
    day between '20240430' and '20240514'
    and app_id = '6448786147'
group by
    skad_ad_network_id,
    skad_campaign_id
;
'''

