# GPT上下文，有关Max Compute SQL

## AF事件表
表名：ods_platform_appsflyer_events
建表语句：CREATE EXTERNAL TABLE IF NOT EXISTS rg_bi.ods_platform_appsflyer_events(install_time STRING COMMENT '安装时间',install_timestamp BIGINT COMMENT '安装时间Unix毫秒数',event_time STRING COMMENT '事件时间',event_timestamp BIGINT COMMENT '事件时间Unix毫秒数',event_name STRING COMMENT '事件名称',event_value STRING COMMENT '事件值',event_revenue DOUBLE COMMENT '事件收入金额',event_revenue_currency STRING COMMENT '事件收入货币类型',event_revenue_usd DOUBLE COMMENT '事件收入美元金额',channel STRING COMMENT '版位',media_source STRING COMMENT '媒体渠道',campaign STRING COMMENT '广告系列',campaign_id STRING COMMENT '广告系列ID',adset STRING COMMENT '广告组',adset_id STRING COMMENT '广告组ID',ad STRING COMMENT '广告',ad_id STRING COMMENT '广告ID',country_code STRING COMMENT '国家代码',appsflyer_id STRING COMMENT 'AF唯一ID',advertising_id STRING COMMENT '安卓GAID',idfa STRING COMMENT 'IOSIDFA',android_id STRING COMMENT '安卓ID',customer_user_id STRING COMMENT '游戏自定义ID',imei STRING COMMENT 'IMEI',idfv STRING COMMENT 'IOSIDFV',platform STRING COMMENT '平台',app_id STRING COMMENT '应用ID',app_name STRING COMMENT '应用名字',bundle_id STRING COMMENT 'BundleID') PARTITIONED BY (app STRING COMMENT '项目',zone STRING COMMENT '时区',`day` STRING COMMENT '日期') STORED BY 'com.aliyun.odps.CsvStorageHandler' LOCATION 'oss://oss-us-west-1-internal.aliyuncs.com/bi-base-data/maxcompute/external/af/events/' TBLPROPERTIES ('comment'='AF原始数据事件表(安装付费)');
内容范例：select * from ods_platform_appsflyer_events where day = 20230401 limit 1;
install_time,install_timestamp,event_time,event_timestamp,event_name,event_value,event_revenue,event_revenue_currency,event_revenue_usd,channel,media_source,campaign,campaign_id,adset,adset_id,ad,ad_id,country_code,appsflyer_id,advertising_id,idfa,android_id,customer_user_id,imei,idfv,platform,app_id,app_name,bundle_id,app,zone,day
2022-12-31 18:58:38,1672513118,2023-04-01 23:59:59,1680393599,app_launch,eyJsb2NhbF9kZXZpY2VpZCI6IjRFMkVCM0U5LUM1NzQtNDk0Ny1CMEVDLUY5RTk4NDg3REE2QSIsInJnX2dhbWV1aWQiOiI4OTA4NDg3MTczMzgifQ==,\N,USD,\N,\N,\N,\N,\N,\N,\N,\N,\N,UZ,1672467558143-4239574,\N,\N,\N,890848717338,\N,4E2EB3E9-C574-4947-B0EC-F9E98487DA6A,ios,id1479198816,Top War: Battle Game,com.rivergame.worldbattleGlobal,102,0,20230401

主要列解释：
    app_id = 'com.topwar.gp' 安卓海外
    app_id = 'id1479198816' iOS海外
    zone = 0 没有特别要求，需求的就是zone为0的数据，这个过滤要一直有
    install_time 是用户安装时间，格式'2022-12-31 18:58:38'
    install_timestamp 是安装时间的unix秒数，格式'1672513118'
    event_time 事件发生时间，格式与install_time一致
    event_timestamp 事件发生时间戳，格式与install_timestamp一致
    day 是分区，过滤中一定要有，否则sql报错，格式‘20230401’，另外day是事件发生时间，不是安装用户时间。
    customer_user_id 是uid或者叫用户id
    appsflyer_id 是af id，与uid类似但不完全一致
    idfa 是在iOS时部分用户有的苹果用户标志，这个标志是唯一的，但是不是每个用户都有
    idfv 是在iOS时部分用户有的苹果用户标志，这个标志是唯一的，但是不是每个用户都有
    event_name 是事件名称
    主要事件名解释：
        af_purchase 是用户7日内的所有支付事件
        install 是安装（激活）
        af_purchase_oldusers 是用户7日后的所有支付事件
media_source 是媒体来源，这个是在af后台设置的，比如：facebook，google，twitter，instagram，youtube，tiktok，snapchat
注意：
day不是安装日期，只是事件发生的日期。
一般我习惯用这种方式获得安装日期（天）
to_char(
    to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
    "yyyy-mm-dd"
) as install_date
在要求限制安装日期时，要同时限制这个install_date,也要限制day（因为max compute要求必须有day的限制）

书写sql范例：
请书写sql：
从AF事件表中获得
海外安卓，安装日期在'2022-01-01'~'2023-04-01'
获得用户的uid，安装日期（精确到日），24小时内支付总金额，168小时内支付总金额。

# 从AF事件表中获取用户数据
# 安装日期在2022-05-01~2023-02-27之间
# 用户7日内付费金额
# 海外安卓
# 用uid做用户区分
# 按照安装日期（天）与媒体进行汇总


ods_platform_appsflyer_skad_details
AF SKAN表
拥有列：app_id,skad_ad_network_id,skad_campaign_id,skad_conversion_value,skad_version,skad_transaction_id,ad_network_campaign_id,ad_network_campaign_name,skad_redownload,skad_source_app_id,country_code,city,postal_code,dma,ip,region,timestamp,ad_network_timestamp,skad_attribution_signature,ad_network_adset_name,ad_network_adset_id,ad_network_ad_name,ad_network_ad_id,ad_network_source_app_id,skad_did_win,skad_fidelity_type,install_date,install_type,media_source,event_name,skad_revenue,event_uuid,skad_ambiguous_event,skad_mode,event_value,ad_network_channel,af_prt,min_revenue,max_revenue,min_event_counter,max_event_counter,min_time_post_install,max_time_post_install,day。
主要列解释：
    app_id = 'com.topwar.gp' 安卓海外
    app_id = 'id1479198816' iOS海外
    day 是分区，过滤中一定要有，否则sql报错，格式‘20230401’，另外day是报告生成时间，不是安装用户时间。报告生成一般是用户安装后的几天，所以如果需要限定安装日期，请将day往后多延长几天，建议产长10天。
    skad_conversion_value 简称cv，是用户首日付费金额的分档，0~63，共64个档位。
    install_date 是AF推断的安装日期，简称安装日期，格式‘2023-03-30’
    media_source 是媒体来源，这个是在af后台设置的，比如：facebook，google，twitter，instagram，youtube，tiktok，snapchat
    event_name 事件名，主要事件名：af_purchase，af_skad_revenue，af_skad_redownload，af_skad_install。其中af_skad_revenue = af_skad_redownload + af_skad_install。一般的，不做特殊要求，事件名要加过滤，事件要in ('af_skad_redownload','af_skad_install')
