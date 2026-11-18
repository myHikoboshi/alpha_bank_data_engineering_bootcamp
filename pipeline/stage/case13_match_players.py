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

# 1. Статичные справочники (broadcast допустим)
heroes_dim = dedup_latest(
    spark.table("lakehouse.raw.pg__heroes").select(
        "hero_id",
        F.col("localized_name").alias("hero_name"),
        F.col("roles").alias("hero_roles"),
        "primary_attr",
        "attack_type",
        "src_modified_stamp",
        "sys_insert_stamp",
        "job_id"
    ),
    ["hero_id"]
)

game_modes_dim = dedup_latest(
    spark.table("lakehouse.raw.pg__game_modes").select(
        F.col("mode_id").alias("game_mode"),
        F.col("name").alias("game_mode_name"),
        F.col("balanced").alias("is_balanced_mode"),
        "src_modified_stamp",
        "sys_insert_stamp",
        "job_id"
    ),
    ["game_mode"]
)

lobby_types_dim = dedup_latest(
    spark.table("lakehouse.raw.pg__lobby_types").select(
        F.col("lobby_id").alias("lobby_type"),
        F.col("name").alias("lobby_type_name"),
        "src_modified_stamp",
        "sys_insert_stamp",
        "job_id"
    ),
    ["lobby_type"]
)

# 2. Дедупликация append-таблиц pg__matches и pg__match_players
matches_latest = dedup_latest(
    spark.table("lakehouse.raw.pg__matches").select(
        "match_id", "duration", "game_mode", "lobby_type",
        "human_players", "radiant_win", "start_time",
        "src_modified_stamp", "sys_insert_stamp", "job_id"
    ),
    ["match_id"]
)

matches_dim = (
    matches_latest
    .join(F.broadcast(game_modes_dim), "game_mode", "left")
    .join(F.broadcast(lobby_types_dim), "lobby_type", "left")
    .withColumn(
        "game_mode_group",
        F.when(F.col("game_mode") == 23, F.lit("Turbo"))
            .when(F.col("game_mode").isin(1, 2, 3, 4, 5, 12, 16, 17, 22), F.lit("Standard"))
            .otherwise(F.lit("Other"))
    )
    .withColumn(
        "duration_bucket",
        F.when(F.col("duration").isNull() | (F.col("duration") <= 0), F.lit("unknown"))
            .when(F.col("duration") < 1200, F.lit("<20m"))
            .when(F.col("duration") < 1800, F.lit("20-30m"))
            .when(F.col("duration") < 2400, F.lit("30-40m"))
            .otherwise(F.lit("40+m"))
    )
    .withColumn("in_pg_matches", F.lit(True))
)

mp_latest = dedup_latest(
    spark.table("lakehouse.raw.pg__match_players").select(
        "match_id", "account_id", "player_slot", "hero_id",
        "is_radiant", "win", "tower_damage",
        "src_modified_stamp", "sys_insert_stamp", "job_id"
    ),
    ["match_id", "player_slot"]
)

tde_match_ids = (
    spark.table("lakehouse.raw.mongo__tower_damage_events")
    .select("match_id")
    .distinct()
    .withColumn("match_has_tde_events", F.lit(True))
)

# 3. Сборка результата
result = (
    mp_latest
    .join(matches_dim, "match_id", "left")
    .join(tde_match_ids, "match_id", "left")
    .join(F.broadcast(heroes_dim), "hero_id", "left")
    .withColumn("in_pg_matches", F.coalesce(F.col("in_pg_matches"), F.lit(False)))
    .withColumn("match_has_tde_events", F.coalesce(F.col("match_has_tde_events"), F.lit(False)))
    .withColumn("duration_bucket", F.coalesce(F.col("duration_bucket"), F.lit("unknown")))
    .withColumn(
        "is_valid_account_id",
        (F.col("account_id").isNotNull()) & (F.col("account_id") > 0) & (F.col("account_id") != 4294967295)
    )
    .repartition("match_id")
    .sortWithinPartitions("match_id", "player_slot")
)

# 4. Запись
(
    result.writeTo(FULL_NAME)
    .using("iceberg")
    .createOrReplace()
)