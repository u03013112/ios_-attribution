# GPT上下文，有关Max Compute SQL

## AF事件表
表名：ods_platform_appsflyer_events
建表语句：CREATE EXTERNAL TABLE IF NOT EXISTS rg_bi.ods_platform_appsflyer_events(install_time STRING COMMENT '安装时间',install_timestamp BIGINT COMMENT '安装时间Unix毫秒数',event_time STRING COMMENT '事件时间',event_timestamp BIGINT COMMENT '事件时间Unix毫秒数',event_name STRING COMMENT '事件名称',event_value STRING COMMENT '事件值',event_revenue DOUBLE COMMENT '事件收入金额',event_revenue_currency STRING COMMENT '事件收入货币类型',event_revenue_usd DOUBLE COMMENT '事件收入美元金额',channel STRING COMMENT '版位',media_source STRING COMMENT '媒体渠道',campaign STRING COMMENT '广告系列',campaign_id STRING COMMENT '广告系列ID',adset STRING COMMENT '广告组',adset_id STRING COMMENT '广告组ID',ad STRING COMMENT '广告',ad_id STRING COMMENT '广告ID',country_code STRING COMMENT '国家代码',appsflyer_id STRING COMMENT 'AF唯一ID',advertising_id STRING COMMENT '安卓GAID',idfa STRING COMMENT 'IOSIDFA',android_id STRING COMMENT '安卓ID',customer_user_id STRING COMMENT '游戏自定义ID',imei STRING COMMENT 'IMEI',idfv STRING COMMENT 'IOSIDFV',platform STRING COMMENT '平台',app_id STRING COMMENT '应用ID',app_name STRING COMMENT '应用名字',bundle_id STRING COMMENT 'BundleID') PARTITIONED BY (app STRING COMMENT '项目',zone STRING COMMENT '时区',`day` STRING COMMENT '日期') STORED BY 'com.aliyun.odps.CsvStorageHandler' LOCATION 'oss://oss-us-west-1-internal.aliyuncs.com/bi-base-data/maxcompute/external/af/events/' TBLPROPERTIES ('comment'='AF原始数据事件表(安装付费)');
内容范例：select * from ods_platform_appsflyer_events where day = 20230401 limit 10;
install_time	install_timestamp	event_time	event_timestamp	event_name	event_value	event_revenue	event_revenue_currency	event_revenue_usd	channel	media_source	campaign	campaign_id	adset	adset_id	ad	ad_id	country_code	appsflyer_id	advertising_id	idfa	android_id	customer_user_id	imei	idfv	platform	app_id	app_name	bundle_id	app	zone	day
2022-12-31 18:58:38	1672513118	2023-04-01 23:59:59	1680393599	app_launch	eyJsb2NhbF9kZXZpY2VpZCI6IjRFMkVCM0U5LUM1NzQtNDk0Ny1CMEVDLUY5RTk4NDg3REE2QSIsInJnX2dhbWV1aWQiOiI4OTA4NDg3MTczMzgifQ==	\N	USD	\N	\N	\N	\N	\N	\N	\N	\N	\N	UZ	1672467558143-4239574	\N	\N	\N	890848717338	\N	4E2EB3E9-C574-4947-B0EC-F9E98487DA6A	ios	id1479198816	Top War: Battle Game	com.rivergame.worldbattleGlobal	102	0	20230401
2021-07-11 16:14:04	1626020044	2023-04-01 23:59:59	1680393599	app_launch	eyJsb2NhbF9kZXZpY2VpZCI6IkIwQTNBQjk1LTY5QUEtNDUwNi1BMzAwLTNFOURDN0FBREE4MiIsInJnX2dhbWV1aWQiOiI0NTQ2NTAyNTQ4MTgifQ==	\N	MXN	\N	\N	\N	\N	\N	\N	\N	\N	\N	MX	1626019762472-0395694	\N	\N	\N	454650254818	\N	B0A3AB95-69AA-4506-A300-3E9DC7AADA82	ios	id1479198816	Top War: Battle Game	com.rivergame.worldbattleGlobal	102	0	20230401
2022-07-08 00:32:35	1657240355	2023-04-01 23:59:59	1680393599	app_launch	eyJsb2NhbF9kZXZpY2VpZCI6IjY5RDk4MTI3LTA5NzgtNDJBMy1BOEYyLTlCMDI4RDM1NTYwMyIsInJnX2dhbWV1aWQiOiI4MTkzODA4OTM4NjgifQ==	\N	USD	\N	\N	\N	\N	\N	\N	\N	\N	\N	US	1657052300315-6998127	\N	\N	\N	819380893868	\N	69D98127-0978-42A3-A8F2-9B028D355603	ios	id1479198816	Top War: Battle Game	com.rivergame.worldbattleGlobal	102	0	20230401
2021-01-01 06:14:30	1609481670	2023-04-01 23:59:59	1680393599	app_launch	eyJsb2NhbF9kZXZpY2VpZCI6IjE5OUFGRkU4LUM2RkMtNEUyRS05QkJCLTlDMjQzOTM2NjJENSIsInJnX2dhbWV1aWQiOiIzNTQ0NzQzNzgxNjUifQ==	\N	USD	\N	\N	\N	\N	\N	\N	\N	\N	\N	US	1609481542089-1998642	\N	\N	\N	354474378165	\N	199AFFE8-C6FC-4E2E-9BBB-9C24393662D5	ios	id1479198816	Top War: Battle Game	com.rivergame.worldbattleGlobal	102	0	20230401
2023-02-10 07:04:05	1676012645	2023-04-01 23:59:59	1680393599	app_launch	eyJsb2NhbF9kZXZpY2VpZCI6IjEwM0Y4NUEwLTRBMDEtNEM1QS05QzNELUZBQjhCMDA1MDg2NiIsInJnX2dhbWV1aWQiOiI5MjExNjgxOTgyNzkifQ==	\N	USD	\N	\N	\N	\N	\N	\N	\N	\N	\N	US	1676009129853-5368454	\N	\N	\N	921168198279	\N	C5E368B4-EDD5-4600-8533-B55E98F851AC	ios	id1479198816	Top War: Battle Game	com.rivergame.worldbattleGlobal	102	0	20230401
2022-05-21 03:04:56	1653102296	2023-04-01 23:59:59	1680393599	app_launch	eyJsb2NhbF9kZXZpY2VpZCI6IjZFNjA0QzFELUM0NEUtNEM2NC05MENBLTQxMDIyMzQwREVFRCIsInJnX2dhbWV1aWQiOiI3OTMwNjIzNjkzNDgifQ==	\N	BRL	\N	\N	\N	\N	\N	\N	\N	\N	\N	BR	1653100775575-6604144	\N	\N	\N	793062369348	\N	6E604C1D-C44E-4C64-90CA-41022340DEED	ios	id1479198816	Top War: Battle Game	com.rivergame.worldbattleGlobal	102	0	20230401
2023-01-09 04:19:14	1673237954	2023-04-01 23:59:58	1680393598	app_launch	eyJsb2NhbF9kZXZpY2VpZCI6IkU2NTI4MDQ4LUYxMkItNDg2Ny1BNzI0LUIyOEU0QTMwMjc1NiIsInJnX2dhbWV1aWQiOiI5MDQ1NDY2NjI5NzIifQ==	\N	USD	\N	\N	\N	\N	\N	\N	\N	\N	\N	US	1673237749751-6528048	\N	\N	\N	904546662972	\N	E6528048-F12B-4867-A724-B28E4A302756	ios	id1479198816	Top War: Battle Game	com.rivergame.worldbattleGlobal	102	0	20230401
2022-07-28 09:07:55	1658999275	2023-04-01 23:59:58	1680393598	app_launch	eyJsb2NhbF9kZXZpY2VpZCI6IkM2OTJBNzNGLUZEREQtNEI3Ni1COTA2LUE1MTREQjBBOEUzMiIsInJnX2dhbWV1aWQiOiI2MDg3NjkyMTc4MzgifQ==	\N	KRW	\N	\N	\N	\N	\N	\N	\N	\N	\N	KR	1658979875861-6927347	\N	\N	\N	608769217838	\N	C692A73F-FDDD-4B76-B906-A514DB0A8E32	ios	id1479198816	Top War: Battle Game	com.rivergame.worldbattleGlobal	102	0	20230401
2021-05-27 02:50:52	1622083852	2023-04-01 23:59:58	1680393598	app_launch	eyJsb2NhbF9kZXZpY2VpZCI6IjQ2MjZGRUI5LUNERTgtNENCNi1COTlELUJCNDk0REYyQTlFNyIsInJnX2dhbWV1aWQiOiI2MzA4MDAxNTE5MjgifQ==	\N	JPY	\N	\N	\N	\N	\N	\N	\N	\N	\N	JP	1622083746117-4626984	\N	\N	\N	630800151928	\N	4626FEB9-CDE8-4CB6-B99D-BB494DF2A9E7	ios	id1479198816	Top War: Battle Game	com.rivergame.worldbattleGlobal	102	0	20230401
2023-03-08 04:29:28	1678249768	2023-04-01 23:59:58	1680393598	app_launch	eyJsb2NhbF9kZXZpY2VpZCI6IkU1NUI4QUNELUYzNjItNEVDNy1COEEwLUNDQUYxMTUyOERGOCIsInJnX2dhbWV1aWQiOiI5MzE4OTkzNTc5MjAifQ==	\N	USD	\N	\N	\N	\N	\N	\N	\N	\N	\N	US	1678249697835-5583624	\N	\N	\N	931899357920	\N	E55B8ACD-F362-4EC7-B8A0-CCAF11528DF8	ios	id1479198816	Top War: Battle Game	com.rivergame.worldbattleGlobal	102	0	20230401
主要列解释：
    app_id = 'com.topwar.gp' 安卓海外
    app_id = 'id1479198816' iOS海外
    zone = 0 没有特别要求，需求的就是zone为0的数据，这个过滤要一直有
    day 是分区，过滤中一定要有，否则sql报错，格式‘20230401’
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