WITH matches_dedup AS (
    SELECT
        match_id, start_time, duration, game_mode, lobby_type, cluster, radiant_win
    FROM (
        SELECT
            match_id, start_time, duration, game_mode, lobby_type, cluster, radiant_win,
            ROW_NUMBER() OVER (PARTITION BY match_id ORDER BY sys_insert_stamp DESC) AS rn
        FROM lakehouse.raw.pg__matches
        WHERE match_id IS NOT NULL
          AND game_mode = 22
          AND lobby_type = 7
          AND human_players = 10
          AND duration BETWEEN 720 AND 5400
          AND start_time IS NOT NULL
    ) t
    WHERE rn = 1
),
players_dedup AS (
    SELECT
        match_id, player_slot, account_id, hero_id, is_radiant, win,
        kills, deaths, assists, gold_per_min, xp_per_min, last_hits, denies,
        hero_damage, tower_damage, hero_healing, net_worth, gold, gold_spent, level
    FROM (
        SELECT
            match_id, player_slot, account_id, hero_id, is_radiant, win,
            kills, deaths, assists, gold_per_min, xp_per_min, last_hits, denies,
            hero_damage, tower_damage, hero_healing, net_worth, gold, gold_spent, level,
            ROW_NUMBER() OVER (PARTITION BY match_id, player_slot ORDER BY sys_insert_stamp DESC) AS rn
        FROM lakehouse.raw.pg__match_players
        WHERE match_id IS NOT NULL
          AND hero_id IS NOT NULL
          AND account_id IS NOT NULL
          AND account_id NOT IN (0, 4294967295)
    ) t
    WHERE rn = 1
),
joined AS (
    SELECT
        p.match_id, p.player_slot, p.account_id, p.hero_id, p.is_radiant, p.win,
        p.kills, p.deaths, p.assists,
        p.gold_per_min, p.xp_per_min, p.last_hits, p.denies,
        p.hero_damage, p.tower_damage, p.hero_healing,
        p.net_worth, p.gold, p.level,
        m.start_time,
        CAST(from_unixtime(m.start_time) AS DATE) AS match_date,
        m.duration,
        m.duration / 60.0 AS duration_min,
        m.cluster,
        COUNT(*) OVER (PARTITION BY p.match_id) AS named_players,
        SUM(CASE WHEN p.is_radiant THEN 1 ELSE 0 END) OVER (PARTITION BY p.match_id) AS named_radiant
    FROM players_dedup p
    JOIN matches_dedup m ON m.match_id = p.match_id
),
full_matches AS (
    SELECT * FROM joined
    WHERE named_players = 10 AND named_radiant = 5
),
match_stats AS (
    SELECT
        f.*,
        AVG(hero_damage)          OVER (PARTITION BY match_id) AS m_dmg,
        STDDEV_SAMP(hero_damage)  OVER (PARTITION BY match_id) AS s_dmg,
        AVG(gold_per_min)         OVER (PARTITION BY match_id) AS m_gpm,
        STDDEV_SAMP(gold_per_min) OVER (PARTITION BY match_id) AS s_gpm,
        AVG(xp_per_min)           OVER (PARTITION BY match_id) AS m_xpm,
        STDDEV_SAMP(xp_per_min)   OVER (PARTITION BY match_id) AS s_xpm,
        AVG(denies)               OVER (PARTITION BY match_id) AS m_den,
        STDDEV_SAMP(denies)       OVER (PARTITION BY match_id) AS s_den,
        SUM(hero_damage)          OVER (PARTITION BY match_id, is_radiant) AS team_dmg,
        RANK() OVER (PARTITION BY match_id ORDER BY hero_damage DESC) AS rank_dmg,
        RANK() OVER (PARTITION BY match_id ORDER BY gold_per_min DESC) AS rank_gpm,
        RANK() OVER (PARTITION BY match_id, is_radiant ORDER BY net_worth DESC) AS farm_role
    FROM full_matches f
),
context AS (
    SELECT
        match_id, player_slot, account_id, hero_id, is_radiant, win,
        start_time, match_date, duration_min, cluster,
        kills, deaths, assists,
        gold_per_min, xp_per_min, hero_damage, denies, last_hits, net_worth, level,
        farm_role, rank_dmg, rank_gpm,
        CASE WHEN rank_dmg = 1  THEN 1 ELSE 0 END AS is_top1_dmg,
        CASE WHEN rank_dmg <= 2 THEN 1 ELSE 0 END AS is_top2_dmg,
        CASE WHEN rank_gpm = 1  THEN 1 ELSE 0 END AS is_top1_gpm,
        (hero_damage  - m_dmg) / NULLIF(s_dmg, 0) AS z_dmg,
        (gold_per_min - m_gpm) / NULLIF(s_gpm, 0) AS z_gpm,
        (xp_per_min   - m_xpm) / NULLIF(s_xpm, 0) AS z_xpm,
        (denies       - m_den) / NULLIF(s_den, 0) AS z_den,
        hero_damage / NULLIF(team_dmg, 0) AS team_dmg_share,
        denies       / duration_min AS denies_per_min,
        last_hits    / duration_min AS lh_per_min,
        deaths       / duration_min AS deaths_per_min,
        hero_healing / duration_min AS heal_per_min,
        LEAST(gold / NULLIF(gold_per_min * duration_min, 0), 1.0) AS gold_leftover_share
    FROM match_stats
),
hero_ref AS (
    SELECT
        hero_id,
        COUNT(*) AS hero_games,
        AVG(z_dmg) AS h_dmg,
        AVG(z_gpm) AS h_gpm,
        AVG(z_den) AS h_den,
        AVG(denies_per_min) AS h_dpm,
        AVG(gold_leftover_share) AS h_gls
    FROM context
    GROUP BY hero_id
    HAVING COUNT(*) >= 100
),
role_ref AS (
    SELECT
        farm_role,
        AVG(z_dmg) AS r_dmg,
        AVG(z_gpm) AS r_gpm,
        AVG(z_den) AS r_den,
        AVG(denies_per_min) AS r_dpm,
        AVG(gold_leftover_share) AS r_gls
    FROM context
    GROUP BY farm_role
)
SELECT
    c.match_id,
    c.player_slot,
    c.account_id,
    c.hero_id,
    h.localized_name AS hero_name,
    c.is_radiant,
    c.win,
    c.start_time,
    c.match_date,
    c.duration_min,
    c.cluster,
    c.kills,
    c.deaths,
    c.assists,
    c.gold_per_min,
    c.xp_per_min,
    c.hero_damage,
    c.denies,
    c.last_hits,
    c.net_worth,
    c.level,
    c.farm_role,
    c.rank_dmg,
    c.rank_gpm,
    c.is_top1_dmg,
    c.is_top2_dmg,
    c.is_top1_gpm,
    c.z_dmg,
    c.z_gpm,
    c.z_xpm,
    c.z_den,
    c.team_dmg_share,
    c.denies_per_min,
    c.lh_per_min,
    c.deaths_per_min,
    c.heal_per_min,
    c.gold_leftover_share,
    CASE WHEN hr.hero_id IS NOT NULL THEN 1 ELSE 0 END AS hero_ref_reliable,
    COALESCE(hr.hero_games, 0) AS hero_games_count,
    c.z_dmg - COALESCE(hr.h_dmg, rr.r_dmg) AS exc_dmg,
    c.z_gpm - COALESCE(hr.h_gpm, rr.r_gpm) AS exc_gpm,
    c.z_den - COALESCE(hr.h_den, rr.r_den) AS exc_den,
    c.denies_per_min - COALESCE(hr.h_dpm, rr.r_dpm) AS exc_dpm,
    c.gold_leftover_share - COALESCE(hr.h_gls, rr.r_gls) AS exc_gls
FROM context c
LEFT JOIN hero_ref hr ON hr.hero_id = c.hero_id
LEFT JOIN role_ref rr ON rr.farm_role = c.farm_role
LEFT JOIN lakehouse.raw.pg__heroes h ON h.hero_id = c.hero_id