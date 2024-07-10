# 调查安卓重装率

# 重装定义
# 1、相同gaid，但是不同af id的，是相同设备删除重装
# 2、相同uid，不同gaid，是同一个人在不同设备上玩，也算重装

# 统计两种重装的数量，按照天统计

sql1 = '''
-- 提取基础数据，只获取day >= 20240601的数据
@install_data :=
SELECT
	appsflyer_id AS afid,
	customer_user_id AS uid,
	advertising_id AS gaid,
	day AS install_date
FROM
	ods_platform_appsflyer_events
WHERE
	day >= 20240601
	AND app = 502
	AND zone = 0
	AND app_id = 'com.fun.lastwar.gp'
	AND event_name = 'install';

-- 提取所有事件数据
@big_data :=
SELECT
	appsflyer_id AS afid,
	customer_user_id AS uid,
	advertising_id AS gaid,
	day AS event_date
FROM
	ods_platform_appsflyer_events
WHERE
	day > 20231001
	AND app = 502
	AND zone = 0
	AND app_id = 'com.fun.lastwar.gp';

-- 统计并筛选gaid中afid对应大于1的
@reinstall_candidates :=
SELECT
	gaid,
	COUNT(DISTINCT afid) AS afid_count
FROM
	@big_data
GROUP BY
	gaid
HAVING
	COUNT(DISTINCT afid) > 1;

-- 从install_data中找到第3步中afid对应大于1的gaid，统计数量，按day分组
SELECT
	install_date,
	COUNT(*) AS reinstall_count
FROM
	@install_data
WHERE
	gaid IN (
		SELECT
			gaid
		FROM
			@reinstall_candidates
	)
GROUP BY
	install_date;
'''

sql2 = '''
-- 提取基础数据，只获取day >= 20240601的数据
@install_data :=
SELECT
	appsflyer_id AS afid,
	customer_user_id AS uid,
	advertising_id AS gaid,
	day AS install_date
FROM
	ods_platform_appsflyer_events
WHERE
	day >= 20240601
	AND app = 502
	AND zone = 0
	AND app_id = 'com.fun.lastwar.gp'
	AND event_name = 'install';

-- 提取所有事件数据
@big_data :=
SELECT
	appsflyer_id AS afid,
	customer_user_id AS uid,
	advertising_id AS gaid,
	day AS event_date
FROM
	ods_platform_appsflyer_events
WHERE
	day > 20231001
	AND app = 502
	AND zone = 0
	AND app_id = 'com.fun.lastwar.gp';

-- 统计并筛选uid中gaid对应大于1的
@reinstall_candidates :=
SELECT
	uid,
	COUNT(DISTINCT gaid) AS gaid_count
FROM
	@big_data
GROUP BY
	uid
HAVING
	COUNT(DISTINCT gaid) > 1;

-- 从install_data中找到第3步中gaid对应大于1的uid，统计数量，按day分组
SELECT
	install_date,
	COUNT(*) AS reinstall_count
FROM
	@install_data
WHERE
	uid IN (
		SELECT
			uid
		FROM
			@reinstall_candidates
	)
GROUP BY
	install_date;
'''

sql3 = '''
-- 提取基础数据，只获取day >= 20240601的数据
@install_data :=
SELECT
	appsflyer_id AS afid,
	customer_user_id AS uid,
	advertising_id AS gaid,
	day AS install_date
FROM
	ods_platform_appsflyer_events
WHERE
	day >= 20240601
	AND app = 502
	AND zone = 0
	AND app_id = 'com.fun.lastwar.gp'
	AND event_name = 'install';

-- 提取所有事件数据
@big_data :=
SELECT
	appsflyer_id AS afid,
	customer_user_id AS uid,
	advertising_id AS gaid,
	day AS event_date
FROM
	ods_platform_appsflyer_events
WHERE
	day > 20231001
	AND app = 502
	AND zone = 0
	AND app_id = 'com.fun.lastwar.gp';

-- 统计并筛选gaid中afid对应大于1的
@reinstall_candidates_by_gaid :=
SELECT
	gaid,
	COUNT(DISTINCT afid) AS afid_count
FROM
	@big_data
GROUP BY
	gaid
HAVING
	COUNT(DISTINCT afid) > 1;

-- 统计并筛选uid中gaid对应大于1的
@reinstall_candidates_by_uid :=
SELECT
	uid,
	COUNT(DISTINCT gaid) AS gaid_count
FROM
	@big_data
GROUP BY
	uid
HAVING
	COUNT(DISTINCT gaid) > 1;

-- 从install_data中找到满足任意一个条件的记录，统计数量，按day分组
SELECT
	install_date,
	COUNT(*) AS reinstall_count
FROM
	@install_data
WHERE
	gaid IN (
		SELECT
			gaid
		FROM
			@reinstall_candidates_by_gaid
	)
	OR uid IN (
		SELECT
			uid
		FROM
			@reinstall_candidates_by_uid
	)
GROUP BY
	install_date;
'''