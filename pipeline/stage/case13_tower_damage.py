from pyspark.sql import functions as F
from pyspark.sql import Window

def dedup_latest(df, key_cols):
    meta_cols = {"src_modified_stamp", "sys_insert_stamp", "job_id"}
    payload_cols = [c for c in df.columns if c not in meta_cols and c not in key_cols]
    order_exprs = [
        F.col("src_modified_stamp").desc_nulls_last(),
        F.col("sys_insert_stamp").desc_nulls_last(),
        F.col("job_id").desc_nulls_last(),
    ]
    if payload_cols:
        order_exprs.append(F.hash(*payload_cols).desc())
    w = Window.partitionBy(*key_cols).orderBy(*order_exprs)
    return (
        df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn", "src_modified_stamp", "sys_insert_stamp", "job_id")
    )

# 1. Читаем stage_case13_match_player и считаем размер состава команды в матче
stage_mp = spark.table("lakehouse.stage.case13_match_players").withColumn(
    "team_players_count",
    F.count("player_slot").over(Window.partitionBy("match_id", "is_radiant"))
)

# 2. Статичные справочники
heroes_dim = dedup_latest(
    spark.table("lakehouse.raw.pg__heroes").select(
        "hero_id",
        F.col("localized_name").alias("hero_name"),
        F.col("roles").alias("hero_roles"),
        "src_modified_stamp", "sys_insert_stamp", "job_id"
    ),
    ["hero_id"]
)

abilities_dim = dedup_latest(
    spark.table("lakehouse.raw.pg__abilities").select(
        "ability_id",
        F.col("name").alias("ability_name"),
        F.coalesce(F.col("display_name"), F.col("name")).alias("ability_display_name"),
        "is_talent",
        "src_modified_stamp", "sys_insert_stamp", "job_id"
    ),
    ["ability_id"]
)

hero_ab_dim = dedup_latest(
    spark.table("lakehouse.raw.pg__hero_abilities").select(
        "ability_id",
        F.col("hero_id").alias("ability_owner_hero_id"),
        "src_modified_stamp", "sys_insert_stamp", "job_id"
    ),
    ["ability_id"]
).join(
    heroes_dim.select(
        F.col("hero_id").alias("ability_owner_hero_id"),
        F.col("hero_name").alias("ability_owner_hero_name")
    ),
    "ability_owner_hero_id",
    "left"
)

items_reg = dedup_latest(
    spark.table("lakehouse.raw.pg__items").select(
        "item_id", F.coalesce("localized_name", "name").alias("item_name"),
        "src_modified_stamp", "sys_insert_stamp", "job_id"
    ),
    ["item_id"]
).withColumn("prio", F.lit(1))

items_neu = dedup_latest(
    spark.table("lakehouse.raw.pg__neutral_items").select(
        "item_id", F.coalesce("localized_name", "name").alias("item_name"),
        "src_modified_stamp", "sys_insert_stamp", "job_id"
    ),
    ["item_id"]
).withColumn("prio", F.lit(2))

w_item = Window.partitionBy("item_id").orderBy("prio")
items_dim = (
    items_reg.unionByName(items_neu)
    .withColumn("rn", F.row_number().over(w_item))
    .filter(F.col("rn") == 1)
    .select("item_id", "item_name")
)

npcs_raw = dedup_latest(
    spark.table("lakehouse.raw.pg__npcs").select(
        "npc_id", "name", "team_name", "status_health",
        "src_modified_stamp", "sys_insert_stamp", "job_id"
    ),
    ["npc_id"]
)

# Строгая классификация построек через rlike
buildings_dim = npcs_raw.select(
    F.col("npc_id").alias("npcid"),
    F.col("name").alias("building_npc_name"),
    F.col("status_health").alias("building_max_hp"),
    F.when(F.col("team_name") == "GoodGuys", F.lit("radiant"))
        .when(F.col("team_name") == "BadGuys", F.lit("dire"))
        .otherwise(F.lit("neutral")).alias("building_side"),
    F.when(F.col("name").isNull(), F.lit("OTHER"))
        .when(F.col("name").rlike(r"_tower[1-4]($|_)"), F.lit("TOWER"))
        .when(F.col("name").rlike(r"_rax($|_)"), F.lit("BARRACKS"))
        .when(F.col("name").rlike(r"_fort($|_)"), F.lit("FORT"))
        .when(F.col("name").rlike(r"_fillers($|_)"), F.lit("FILLER"))
        .otherwise(F.lit("OTHER")).alias("building_category"),
    F.when(F.col("name").isNull(), F.lit("OTHER"))
        .when(F.col("name").rlike(r"_tower1($|_)"), F.lit("T1"))
        .when(F.col("name").rlike(r"_tower2($|_)"), F.lit("T2"))
        .when(F.col("name").rlike(r"_tower3($|_)"), F.lit("T3"))
        .when(F.col("name").rlike(r"_tower4($|_)"), F.lit("T4"))
        .when(F.col("name").rlike(r"_rax($|_)"), F.lit("BARRACKS"))
        .when(F.col("name").rlike(r"_fort($|_)"), F.lit("FORT"))
        .when(F.col("name").rlike(r"_fillers($|_)"), F.lit("FILLER"))
        .otherwise(F.lit("OTHER")).alias("tower_type"),
    F.when(F.col("name").rlike(r"_top$"), F.lit("top"))
        .when(F.col("name").rlike(r"_mid$"), F.lit("mid"))
        .when(F.col("name").rlike(r"_bot$"), F.lit("bot"))
        .otherwise(F.lit("base")).alias("building_lane")
)

fromnpc_dim = npcs_raw.select(
    F.col("npc_id").alias("fromnpc"),
    F.col("name").alias("fromnpc_name")
)

# 3. Проекция матчей и каскадные ключи игрока
w_m = Window.partitionBy("match_id").orderBy(F.col("player_slot").asc_nulls_last())
matches_dim = (
    stage_mp.filter(F.col("in_pg_matches") == True)
    .withColumn("_rn", F.row_number().over(w_m))
    .filter(F.col("_rn") == 1)
    .select(
        "match_id", "duration", "duration_bucket", "game_mode", "game_mode_name",
        "game_mode_group", "lobby_type", "lobby_type_name", "radiant_win"
    )
    .withColumn("is_matched_match", F.lit(True))
)

mp_by_acc = (
    stage_mp.filter(F.col("is_valid_account_id") == True)
    .groupBy("match_id", "account_id").agg(
        F.count("*").alias("cnt"),
        F.first("player_slot").alias("acc_player_slot"),
        F.first("hero_id").alias("acc_hero_id"),
        F.first("is_radiant").alias("acc_is_radiant"),
        F.first("win").alias("acc_win"),
        F.first("tower_damage").alias("acc_mp_tower_damage"),
        F.first("team_players_count").alias("acc_team_players_count")
    )
    .filter(F.col("cnt") == 1).drop("cnt")
    .withColumnRenamed("account_id", "steam_account_id")
)

mp_by_hero = (
    stage_mp.filter(F.col("hero_id") > 0)
    .groupBy("match_id", "hero_id").agg(
        F.count("*").alias("cnt"),
        F.first("account_id").alias("h_account_id"),
        F.first("player_slot").alias("h_player_slot"),
        F.first("is_radiant").alias("h_is_radiant"),
        F.first("win").alias("h_win"),
        F.first("tower_damage").alias("h_mp_tower_damage"),
        F.first("team_players_count").alias("h_team_players_count")
    )
    .filter(F.col("cnt") == 1).drop("cnt")
    .withColumnRenamed("hero_id", "attacker_hero_id")
)

# 4. Чтение и дедупликация событий фактов по event_id
tde_raw = dedup_latest(
    spark.table("lakehouse.raw.mongo__tower_damage_events").select(
        F.col("id").alias("event_id"),
        "match_id",
        "time",
        "damage",
        "steam_account_id",
        F.col("attacker").alias("attacker_hero_id"),
        "byability",
        "byitem",
        "fromnpc",
        "npcid",
        "src_modified_stamp",
        "sys_insert_stamp",
        "job_id"
    ),
    ["event_id"]
)

# 5. Обогащение с явной обработкой NULL в источнике урона
result = (
    tde_raw
    .withColumn("minute", F.floor(F.col("time") / 60).cast("int"))
    .withColumn(
        "phase",
        F.when(F.col("minute").isNull(), F.lit("unknown"))
            .when(F.col("minute") < 0, F.lit("pre-game"))
            .when(F.col("minute") < 10, F.lit("0-10"))
            .when(F.col("minute") < 20, F.lit("10-20"))
            .when(F.col("minute") < 30, F.lit("20-30"))
            .otherwise(F.lit("30+"))
    )
    .withColumn("has_byability", F.col("byability").isNotNull() & (F.col("byability") > 0))
    .withColumn("has_byitem", F.col("byitem").isNotNull() & (F.col("byitem") > 0))
    .withColumn("has_fromnpc", F.col("fromnpc").isNotNull() & (F.col("fromnpc") > 0))
    .withColumn(
        "damage_source_type",
        F.when(
            F.col("byability").isNull() | F.col("byitem").isNull() | F.col("fromnpc").isNull(),
            F.lit("unknown")
        )
        .when((F.col("byability") > 0) & (F.col("fromnpc") == 0), F.lit("ability_direct"))
        .when((F.col("byability") > 0) & (F.col("fromnpc") > 0), F.lit("ability_summon"))
        .when((F.col("byability") == 0) & (F.col("byitem") == 0) & (F.col("fromnpc") > 0), F.lit("summon_without_ability"))
        .when((F.col("byability") == 0) & (F.col("byitem") > 0), F.lit("item"))
        .when((F.col("byability") == 0) & (F.col("byitem") == 0) & (F.col("fromnpc") == 0), F.lit("auto_attack"))
        .otherwise(F.lit("unknown"))
    )
    .withColumn("ability_id", F.when(F.col("has_byability"), F.col("byability")))
    .withColumn("item_id", F.when(F.col("has_byitem"), F.col("byitem")))
    .join(F.broadcast(abilities_dim), "ability_id", "left")
    .join(F.broadcast(hero_ab_dim), "ability_id", "left")
    .join(F.broadcast(items_dim), "item_id", "left")
    .join(F.broadcast(buildings_dim), "npcid", "left")
    .join(F.broadcast(fromnpc_dim), "fromnpc", "left")
    .join(matches_dim, "match_id", "left")
    .withColumn("is_matched_match", F.coalesce(F.col("is_matched_match"), F.lit(False)))
    .withColumn("duration_bucket", F.coalesce(F.col("duration_bucket"), F.lit("unknown")))
    .join(mp_by_acc, on=["match_id", "steam_account_id"], how="left")
    .join(mp_by_hero, on=["match_id", "attacker_hero_id"], how="left")
    .withColumn(
        "player_match_method",
        F.when(F.col("acc_hero_id").isNotNull(), F.lit("account_id"))
            .when(F.col("h_player_slot").isNotNull(), F.lit("hero_id_fallback"))
            .otherwise(F.lit("unmatched"))
    )
    .withColumn("player_slot", F.coalesce(F.col("acc_player_slot"), F.col("h_player_slot")))
    .withColumn(
        "mp_hero_id",
        F.when(F.col("acc_hero_id").isNotNull(), F.col("acc_hero_id"))
            .when(F.col("h_player_slot").isNotNull(), F.col("attacker_hero_id"))
    )
    .withColumn("effective_hero_id", F.coalesce(F.col("mp_hero_id"), F.col("attacker_hero_id")))
    .withColumn("is_radiant", F.coalesce(F.col("acc_is_radiant"), F.col("h_is_radiant")))
    .withColumn("win", F.coalesce(F.col("acc_win"), F.col("h_win")))
    .withColumn("mp_tower_damage", F.coalesce(F.col("acc_mp_tower_damage"), F.col("h_mp_tower_damage")))
    .withColumn("team_players_count", F.coalesce(F.col("acc_team_players_count"), F.col("h_team_players_count")))
    .drop(
        "acc_player_slot", "acc_hero_id", "acc_is_radiant", "acc_win", "acc_mp_tower_damage", "acc_team_players_count",
        "h_account_id", "h_player_slot", "h_is_radiant", "h_win", "h_mp_tower_damage", "h_team_players_count"
    )
    .join(
        F.broadcast(heroes_dim.withColumnRenamed("hero_id", "effective_hero_id")),
        "effective_hero_id",
        "left"
    )
)

writer = (
    result.writeTo(FULL_NAME)
    .using("iceberg")
)