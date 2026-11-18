WITH mp_base AS (
    SELECT
        match_id,
        player_slot,
        account_id,
        hero_id,
        hero_name,
        CASE
            WHEN hero_roles IS NOT NULL THEN array_join(hero_roles, ', ')
            ELSE 'unknown'
        END AS hero_roles_str,
        CASE
            WHEN hero_roles IS NOT NULL AND array_join(hero_roles, ',') LIKE '%Pusher%' THEN TRUE
            ELSE FALSE
        END AS is_pusher_role,
        primary_attr,
        attack_type,
        is_radiant,
        win,
        CAST(tower_damage AS BIGINT) AS pg_tower_damage,
        duration,
        duration_bucket,
        game_mode,
        game_mode_name,
        game_mode_group,
        match_has_tde_events,
        COUNT(player_slot) OVER (PARTITION BY match_id, is_radiant) AS team_players_count
    FROM lakehouse.stage.case13_match_players
    WHERE in_pg_matches = TRUE
),
tde_player_agg AS (
    SELECT
        match_id,
        player_slot,
        SUM(CASE WHEN building_category = 'TOWER' THEN CAST(damage AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS ev_towers_damage,
        SUM(CASE WHEN building_category = 'TOWER' AND damage_source_type IN ('ability_direct', 'ability_summon') THEN CAST(damage AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS ev_towers_ability_damage,
        SUM(CASE WHEN building_category = 'TOWER' AND damage_source_type = 'summon_without_ability' THEN CAST(damage AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS ev_towers_summon_damage,
        SUM(CASE WHEN building_category = 'TOWER' AND damage_source_type = 'auto_attack' THEN CAST(damage AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS ev_towers_auto_damage,
        SUM(CASE WHEN building_category = 'TOWER' AND damage_source_type = 'item' THEN CAST(damage AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS ev_towers_item_damage,
        SUM(CASE WHEN building_category = 'BARRACKS' THEN CAST(damage AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS ev_barracks_damage,
        SUM(CASE WHEN building_category = 'FORT' THEN CAST(damage AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS ev_fort_damage,
        SUM(CASE WHEN building_category = 'FILLER' THEN CAST(damage AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS ev_fillers_damage,
        SUM(CASE WHEN building_category IN ('TOWER', 'BARRACKS', 'FORT') THEN CAST(damage AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS ev_official_building_damage,
        SUM(CAST(damage AS BIGINT)) AS ev_all_buildings_damage,
        SUM(CASE WHEN building_category = 'TOWER' THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS ev_towers_events_count
    FROM lakehouse.stage.case13_tower_damage
    WHERE is_matched_match = TRUE
      AND player_slot IS NOT NULL
    GROUP BY match_id, player_slot
),
joined AS (
    SELECT
        mp.match_id,
        mp.player_slot,
        mp.account_id,
        mp.hero_id,
        mp.hero_name,
        mp.hero_roles_str,
        mp.is_pusher_role,
        mp.primary_attr,
        mp.attack_type,
        mp.is_radiant,
        mp.win,
        mp.duration,
        mp.duration_bucket,
        mp.game_mode,
        mp.game_mode_name,
        mp.game_mode_group,
        mp.match_has_tde_events,
        mp.team_players_count,
        mp.pg_tower_damage,
        COALESCE(t.ev_towers_damage, CAST(0 AS BIGINT)) AS ev_towers_damage,
        COALESCE(t.ev_towers_ability_damage, CAST(0 AS BIGINT)) AS ev_towers_ability_damage,
        COALESCE(t.ev_towers_summon_damage, CAST(0 AS BIGINT)) AS ev_towers_summon_damage,
        COALESCE(t.ev_towers_auto_damage, CAST(0 AS BIGINT)) AS ev_towers_auto_damage,
        COALESCE(t.ev_towers_item_damage, CAST(0 AS BIGINT)) AS ev_towers_item_damage,
        COALESCE(t.ev_barracks_damage, CAST(0 AS BIGINT)) AS ev_barracks_damage,
        COALESCE(t.ev_fort_damage, CAST(0 AS BIGINT)) AS ev_fort_damage,
        COALESCE(t.ev_fillers_damage, CAST(0 AS BIGINT)) AS ev_fillers_damage,
        COALESCE(t.ev_official_building_damage, CAST(0 AS BIGINT)) AS ev_official_building_damage,
        COALESCE(t.ev_all_buildings_damage, CAST(0 AS BIGINT)) AS ev_all_buildings_damage,
        COALESCE(t.ev_towers_events_count, CAST(0 AS BIGINT)) AS ev_towers_events_count
    FROM mp_base mp
    LEFT JOIN tde_player_agg t
        ON mp.match_id = t.match_id
       AND mp.player_slot = t.player_slot
)
SELECT
    match_id,
    player_slot,
    account_id,
    hero_id,
    hero_name,
    hero_roles_str,
    is_pusher_role,
    primary_attr,
    attack_type,
    is_radiant,
    win,
    duration,
    duration_bucket,
    game_mode,
    game_mode_name,
    game_mode_group,
    match_has_tde_events,
    team_players_count,
    pg_tower_damage,
    ev_towers_damage,
    ev_towers_ability_damage,
    ev_towers_summon_damage,
    ev_towers_auto_damage,
    ev_towers_item_damage,
    ev_barracks_damage,
    ev_fort_damage,
    ev_fillers_damage,
    ev_official_building_damage,
    ev_all_buildings_damage,
    ev_towers_events_count,
    ev_towers_damage - pg_tower_damage AS damage_diff,
    CASE
        WHEN ev_towers_damage > 0
            THEN CAST(ev_towers_ability_damage + ev_towers_summon_damage AS DOUBLE) / CAST(ev_towers_damage AS DOUBLE)
        ELSE 0.0
    END AS ability_and_summon_tower_share,
    ev_towers_ability_damage + ev_towers_summon_damage AS ev_towers_ability_summon_damage,
    CASE WHEN ev_towers_damage > 0 THEN TRUE ELSE FALSE END AS has_ev_tower_damage,
    CASE
        WHEN duration > 0 THEN CAST(ev_towers_damage AS DOUBLE) / (CAST(duration AS DOUBLE) / 60.0)
        ELSE 0.0
    END AS ev_towers_damage_per_min,
    CASE
        WHEN duration > 0 THEN CAST(ev_towers_ability_damage + ev_towers_summon_damage AS DOUBLE) / (CAST(duration AS DOUBLE) / 60.0)
        ELSE 0.0
    END AS ev_towers_ability_summon_per_min
FROM joined