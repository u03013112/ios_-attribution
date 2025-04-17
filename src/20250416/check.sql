-- 检查日服开服时之前服务器付费人数
-- 使用新支付作为主要支付事件

WITH server_times AS (
    SELECT 
        "lw_zone@id" AS "server_id_str",
        "lw_zone@region_type" AS "region_type",
        "lw_zone@start_time" AS "start_time",
        LEAD("lw_zone@start_time") OVER (ORDER BY "lw_zone@start_time" ASC) AS "next_start_time"
    FROM 
        ta_dim.dim_3_0_47843
    WHERE 
        "lw_zone@region_type" IN ('日服')
),
pay_users AS (
    SELECT 
        "lw_zone",
        "#account_id",
        "#event_time"
    FROM 
        v_event_3 
    WHERE 
        "$part_event" = 's_pay_new'
        AND ${PartDate:date1} 
)
SELECT 
    st."server_id_str",
    st."region_type",
    st."start_time",
    st."next_start_time",
    COUNT(DISTINCT pu."#account_id") AS "pay_user_count"
FROM 
    server_times st
LEFT JOIN 
    pay_users pu ON st."server_id_str" = pu."lw_zone"
    AND pu."#event_time" >= st."start_time"
    AND (pu."#event_time" < st."next_start_time" OR st."next_start_time" IS NULL)
GROUP BY 
    st."server_id_str", st."region_type", st."start_time", st."next_start_time"
ORDER BY 
    st."start_time" DESC;
