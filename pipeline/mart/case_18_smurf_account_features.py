from pyspark.sql import functions as F

ctx = spark.table("lakehouse.stage.case_18_smurf_player_context")

# Эталон доминирования по позиции: нормируем share_top* и team_dmg_share,
# чтобы модель не выделяла керри просто за высокую долю урона
role_dom_ref = ctx.groupBy("farm_role").agg(
    F.avg("is_top1_dmg").alias("ref_top1"),
    F.avg("is_top2_dmg").alias("ref_top2"),
    F.avg("team_dmg_share").alias("ref_team_share"),
)

ctx_normed = (
    ctx.join(role_dom_ref, on="farm_role", how="left")
        .withColumn("exc_top1", F.col("is_top1_dmg") - F.col("ref_top1"))
        .withColumn("exc_top2", F.col("is_top2_dmg") - F.col("ref_top2"))
        .withColumn("exc_team_share", F.col("team_dmg_share") - F.col("ref_team_share"))
)

agg = ctx_normed.groupBy("account_id").agg(
    F.count("*").alias("matches_count"),
    F.sum(F.col("win").cast("int")).alias("wins"),
    F.sum((~F.col("win")).cast("int")).alias("losses_count"),

    # Превышение над эталоном героя — основной сигнал
    F.avg("exc_dmg").alias("avg_exc_dmg"),
    F.avg("exc_gpm").alias("avg_exc_gpm"),
    F.avg("exc_den").alias("avg_exc_den"),
    F.avg("exc_dpm").alias("avg_exc_dpm"),
    F.avg("exc_gls").alias("avg_exc_gls"),

    # Устойчивость от матча к матчу
    F.percentile_approx(F.col("exc_dmg"), F.lit(0.25)).alias("q25_exc_dmg"),
    F.min("exc_dmg").alias("min_exc_dmg"),
    F.stddev_samp("exc_dmg").alias("std_exc_dmg"),
    F.avg(F.when(F.col("exc_dmg") > 0, 1).otherwise(0)).alias("share_positive_exc"),

    # Доминирование, нормированное на роль
    F.avg("exc_top1").alias("avg_exc_top1"),
    F.avg("exc_top2").alias("avg_exc_top2"),
    F.avg("exc_team_share").alias("avg_exc_team_share"),

    # Поведение в проигранных матчах
    F.avg(F.when(~F.col("win"), F.col("exc_dmg"))).alias("avg_exc_dmg_loss"),

    # Роль и пул героев — описательные, не для модели
    F.avg("farm_role").alias("avg_farm_role"),
    F.stddev_samp("farm_role").alias("std_farm_role"),
    F.countDistinct("hero_id").alias("unique_heroes"),
    F.countDistinct("farm_role").alias("roles_played"),

    # Абсолютные показатели для интерпретации
    F.avg("gold_per_min").alias("avg_gpm"),
    F.avg("xp_per_min").alias("avg_xpm"),
    F.avg("denies_per_min").alias("avg_denies_per_min"),
    F.avg("deaths_per_min").alias("avg_deaths_per_min"),
    F.avg("heal_per_min").alias("avg_heal_per_min"),
    F.avg("gold_leftover_share").alias("avg_gold_leftover"),
    F.stddev_samp("gold_per_min").alias("std_gpm"),

    # Контроль
    F.avg("duration_min").alias("avg_duration_min"),
    F.min("start_time").alias("first_match_ts"),
    F.max("start_time").alias("last_match_ts"),
    F.avg("hero_ref_reliable").alias("share_reliable_hero_ref"),
)

result = (
    agg.filter(F.col("matches_count") >= 3)
    .withColumn("win_rate_raw", F.col("wins") / F.col("matches_count"))
    .withColumn("win_rate_smoothed",
                (F.col("wins") + F.lit(25.0)) / (F.col("matches_count") + F.lit(50.0)))
    .withColumn("cv_gpm", F.col("std_gpm") / F.nullif(F.col("avg_gpm"), F.lit(0)))
    .withColumn("span_hours",
                (F.col("last_match_ts") - F.col("first_match_ts")) / F.lit(3600.0))
    .withColumn("has_losses", (F.col("losses_count") > 0).cast("int"))
    .withColumn("avg_exc_dmg_loss_filled",
                F.coalesce(F.col("avg_exc_dmg_loss"), F.lit(0.0)))
    .fillna({"std_exc_dmg": 0.0, "std_farm_role": 0.0, "std_gpm": 0.0, "cv_gpm": 0.0})
    .withColumn("expected_noise_share",
                F.when(F.col("avg_exc_dmg") > 2.0, F.lit(0.05))
                    .when(F.col("avg_exc_dmg") > 1.5, F.lit(0.14))
                    .when(F.col("avg_exc_dmg") > 1.0, F.lit(0.30))
                    .otherwise(F.lit(0.42)))
    .withColumn("strong_candidate",
                ((F.col("avg_exc_dmg") > 1.5) & (F.col("q25_exc_dmg") > 0.3)).cast("int"))
    .withColumn("features_version", F.lit("v2_role_normalized"))
)

result.writeTo(FULL_NAME).using("iceberg").createOrReplace()