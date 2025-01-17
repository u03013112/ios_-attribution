WITH battle_data AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        allianceid,
        COALESCE("group", '0.0') AS groupa,
        enemyallianceid,
        CAST(COALESCE(enemygroup, 0.0) AS VARCHAR(3)) AS enemygroup,
        num,
        score,
        ROUND(MINUTE("#event_time") / 5, 1) * 5 AS min_id
    FROM
        hive.ta.v_event_15
    WHERE
        "$part_event" = 'alliance_dragon_battle_data'
        AND "$part_date" >= '2024-08-01'
        AND ROUND(MINUTE("#event_time") / 5, 1) * 5 IN (15)
        AND ${PartDate:date1}
),
alliance_data AS (
    SELECT
        date_trunc('week', "#event_time") AS wk,
        alliance_id,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamagroup
            WHEN alliance_id = teamballianceid THEN teambgroup
        END AS alliance_group,
        CASE
            WHEN alliance_id = teamaallianceid THEN teama30levelplayercount
            WHEN alliance_id = teamballianceid THEN teamb30levelplayercount
        END AS team30levelplayercount,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamaactivelevel
            WHEN alliance_id = teamballianceid THEN teambactivelevel
        END AS teamactivelevel,
        CASE
            WHEN alliance_id = teamaallianceid THEN teamahighestlevel
            WHEN alliance_id = teamballianceid THEN teambhighestlevel
        END AS teamhighestlevel,
        CASE
            WHEN alliance_id = teamaallianceid THEN servera
            WHEN alliance_id = teamballianceid THEN serverb
        END AS server_num,
        strength,
        strengthinfo,
        strengthinfo2
    FROM
        ta.v_event_15
    WHERE
        "$part_date" >= '2024-08-01'
        AND "$part_event" = 'alliance_dragon_battle_match'
        AND ${PartDate:date1}
),
battle_results AS (
    SELECT
        a.wk,
        a.allianceid AS alliance_a_id,
        a.groupa AS group_a,
        a.score AS score_a,
        a.enemyallianceid AS alliance_b_id,
        b.groupa AS group_b,
        b.score AS score_b,
        CASE
            WHEN a.score > b.score THEN 1
            WHEN a.score < b.score THEN 0
            ELSE -1
        END AS is_win,
        CASE
            WHEN a.score > b.score
            AND (COALESCE(a.score, 0.0) / COALESCE(b.score, 0.0)) - 1 < 1 THEN 1
            WHEN a.score < b.score
            AND (COALESCE(b.score, 0.0) / COALESCE(a.score, 0.0)) - 1 < 1 THEN 1
            ELSE 0
        END AS is_quality
    FROM
        battle_data a
        LEFT JOIN battle_data b ON a.enemyallianceid = b.allianceid
        AND a.wk = b.wk
        AND a.min_id = b.min_id
        AND a.enemygroup = b.groupa
    WHERE
        a.min_id = 15
        AND (
            a.num IS NOT NULL
            AND a.num != 0
        )
        AND (
            b.num IS NOT NULL
            AND b.num != 0
        )
        AND a.allianceid < a.enemyallianceid
)
SELECT
    br.wk,
    br.alliance_a_id,
    ad_a.alliance_group AS group_a,
    ad_a.team30levelplayercount AS team30levelplayercount_a,
    ad_a.teamactivelevel AS teamactivelevel_a,
    ad_a.teamhighestlevel AS teamhighestlevel_a,
    ad_a.server_num AS server_num_a,
    ad_a.strength AS strength_a,
    ad_a.strengthinfo AS strengthinfo_a,
    ad_a.strengthinfo2 AS strengthinfo2_a,
    br.score_a,
    br.alliance_b_id,
    ad_b.alliance_group AS group_b,
    ad_b.team30levelplayercount AS team30levelplayercount_b,
    ad_b.teamactivelevel AS teamactivelevel_b,
    ad_b.teamhighestlevel AS teamhighestlevel_b,
    ad_b.server_num AS server_num_b,
    ad_b.strength AS strength_b,
    ad_b.strengthinfo AS strengthinfo_b,
    ad_b.strengthinfo2 AS strengthinfo2_b,
    br.score_b,
    br.is_win,
    br.is_quality
FROM
    battle_results br
    LEFT JOIN alliance_data ad_a ON br.alliance_a_id = ad_a.alliance_id
    AND br.group_a = CAST(ad_a.alliance_group AS VARCHAR)
    AND br.wk = ad_a.wk
    LEFT JOIN alliance_data ad_b ON br.alliance_b_id = ad_b.alliance_id
    AND br.group_b = CAST(ad_b.alliance_group AS VARCHAR)
    AND br.wk = ad_b.wk;