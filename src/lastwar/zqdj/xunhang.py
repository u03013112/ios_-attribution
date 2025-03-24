# 战区对决 续航能力评估
import json
import numpy as np
import pandas as pd

import sys
sys.path.append('/src')

from src.lastwar.ss.ss import ssSql


def getData(startDayStr = '2024-11-25', endDayStr = '2025-03-17'):
    sql = f'''
WITH battle_data AS (
    SELECT
        uid,
        "$part_date",
        MIN("#event_time") AS battle_time_min,
        MAX("#event_time") AS battle_time_max,
        COUNT(*) AS battle_count
    FROM
        (
            SELECT
                "def_uid" AS uid,
                "$part_date",
                "#event_time"
            FROM
                v_event_15
            WHERE
                "$part_event" = 's_battle'
                AND "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
                AND battle_type IN ('CROSS_THRONE')
                AND CAST(substr("def_uid", -4) AS INTEGER) BETWEEN 3
                AND 196
            UNION
            ALL
            SELECT
                "atk_uid" AS uid,
                "$part_date",
                "#event_time"
            FROM
                v_event_15
            WHERE
                "$part_event" = 's_battle'
                AND "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
                AND battle_type IN ('CROSS_THRONE')
                AND CAST(substr("def_uid", -4) AS INTEGER) BETWEEN 3
                AND 196
        ) AS combined
    GROUP BY
        uid,
        "$part_date"
),
resource_data AS (
    SELECT
        "#account_id" AS uid,
        "$part_date",
        "#event_time",
        original_num,
        cost
    FROM
        v_event_15
        LEFT JOIN ta_dim."dim_15_0_78242" ON "resource_item_id" = "dim_15_0_78242"."resource_item_id@id"
        LEFT JOIN ta_dim."dim_15_0_78243" ON "op_type" = "dim_15_0_78243"."op_type@id"
    WHERE
        "$part_event" = 's_resource_item'
        AND "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
        AND cost > 0
        AND "resource_item_id@type" = '士兵'
        AND "op_type@desc" IN ('治疗', '秒治疗')
),
all_soldier_data AS (
    SELECT
        "#account_id" AS uid,
        "$part_date",
        "#event_time",
        SUM(remain_num) AS remain_num
    FROM
        v_event_15
        LEFT JOIN ta_dim."dim_15_0_78242" ON "resource_item_id" = "dim_15_0_78242"."resource_item_id@id"
        LEFT JOIN ta_dim."dim_15_0_78243" ON "op_type" = "dim_15_0_78243"."op_type@id"
    WHERE
        "$part_event" = 's_resource_item'
        AND "$part_date" BETWEEN '{startDayStr}' AND '{endDayStr}'
        AND "resource_item_id@type" = '士兵'
    GROUP BY
        "#account_id",
        "$part_date",
        "#event_time"
),
joined_data AS (
    SELECT
        b.uid,
        b."$part_date",
        MIN(r."#event_time") AS heal_time_min,
        MAX(r."#event_time") AS heal_time_max,
        b.battle_time_min,
        b.battle_time_max,
        b.battle_count,
        COUNT(r.cost) AS heal_count,
        SUM(r.cost) AS healed_soldiers
    FROM
        battle_data b
        LEFT JOIN resource_data r ON b.uid = r.uid
        AND r."#event_time" BETWEEN b.battle_time_min
        AND b.battle_time_max
    GROUP BY
        b.uid,
        b."$part_date",
        b.battle_time_min,
        b.battle_time_max,
        b.battle_count
),
initial_soldiers2_data AS (
    SELECT
        a.uid,
        a."$part_date",
        a.remain_num AS initial_soldiers2,
        a."#event_time" AS initial_soldiers2_time
    FROM
        all_soldier_data a
        JOIN battle_data b ON a.uid = b.uid
        AND a."#event_time" < b.battle_time_min
    WHERE
        a."#event_time" = (
            SELECT
                MAX("#event_time")
            FROM
                all_soldier_data
            WHERE
                uid = a.uid
                AND "#event_time" < b.battle_time_min
        )
)
SELECT
    j.uid,
    j."$part_date",
    date_format(j.battle_time_min, '%H:%i:%s') AS battle_time_min,
    date_format(j.battle_time_max, '%H:%i:%s') AS battle_time_max,
    j.battle_count,
    date_format(j.heal_time_min, '%H:%i:%s') AS heal_time_min,
    date_format(j.heal_time_max, '%H:%i:%s') AS heal_time_max,
    j.heal_count,
    i.original_num AS initial_soldiers,
    j.healed_soldiers,
    s.initial_soldiers2,
    date_format(s.initial_soldiers2_time, '%H:%i:%s') AS initial_soldiers2_time
FROM
    joined_data j
    LEFT JOIN resource_data i ON j.uid = i.uid
    AND i."#event_time" = j.heal_time_min
    LEFT JOIN initial_soldiers2_data s ON j.uid = s.uid
    AND j."$part_date" = s."$part_date"

    '''

    print('old sql:')
    print(sql)

    # 将sql中的 
    # v_event_15-> v_event_3
    # ta_dim.dim_15_0_78242 -> ta_dim.dim_3_0_66670
    # ta_dim.dim_15_0_78243 -> ta_dim.dim_3_0_66671

    sql = sql.replace('v_event_15', 'v_event_3')
    sql = sql.replace('dim_15_0_78242', 'dim_3_0_66670')
    sql = sql.replace('dim_15_0_78243', 'dim_3_0_66671')

    print('new sql:')
    print(sql)

    lines = ssSql(sql)
    print('lines:',len(lines))
    print(lines[:10])

    data = []
    for line in lines:
        if line == '':
            continue
        j = json.loads(line)
        data.append(j)

    df = pd.DataFrame(data, columns=['uid', 'part_date', 'battle_time_min', 'battle_time_max', 'battle_count', 'heal_time_min', 'heal_time_max', 'heal_count', 'initial_soldiers', 'healed_soldiers', 'initial_soldiers2', 'initial_soldiers2_time'])
    df.to_csv(f'/src/data/zqdj_xunhang_data_{startDayStr}_{endDayStr}.csv', index=False)
    return df


if __name__ == '__main__':
    getData(startDayStr='2025-01-01', endDayStr='2025-03-23')