from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    ClusteringEvaluator,
    MulticlassClassificationEvaluator,
    RegressionEvaluator,
)
from pyspark.ml.feature import (
    HashingTF,
    IDF,
    Imputer,
    StandardScaler,
    Tokenizer,
    VectorAssembler,
)
from pyspark.ml.functions import vector_to_array
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_AIR_INPUT = "/opt/krasair/data/staging/air_cleaned.csv"
DEFAULT_WEATHER_INPUT = "/opt/krasair/data/staging/weather_cleaned.csv"
DEFAULT_NMU_INPUT = "/opt/krasair/data/staging/nmu_hourly.csv"
DEFAULT_REGRESSION_OUTPUT = "/opt/krasair/data/marts/ml_regression_predictions.csv"
DEFAULT_CLASSIFICATION_OUTPUT = "/opt/krasair/data/marts/ml_classification_predictions.csv"
DEFAULT_METRICS_OUTPUT = "/opt/krasair/data/marts/ml_metrics.csv"
DEFAULT_FEATURE_IMPORTANCE_OUTPUT = "/opt/krasair/data/marts/ml_feature_importance.csv"
DEFAULT_CLUSTERS_OUTPUT = "/opt/krasair/data/marts/ml_clusters.csv"
DEFAULT_CLUSTER_PROFILE_OUTPUT = "/opt/krasair/data/marts/ml_cluster_profile.csv"

SUP_NUMERIC = [
    "hour",
    "month",
    "weather_temperature",
    "weather_humidity",
    "precipitation",
    "snowfall",
    "surface_pressure",
    "wind_speed",
    "wind_direction",
    "nmu_severity",
]
TEXT_FEATURES_DIM = 64
SEED = 42

CLUSTER_NUMERIC = [
    "aqi",
    "pm25",
    "pm10",
    "weather_temperature",
    "weather_humidity",
    "wind_speed",
    "surface_pressure",
    "nmu_severity",
]


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("KrasAirSparkML").getOrCreate()


def read_csv(spark: SparkSession, path: str):
    return (
        spark.read.option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(path)
    )


def prepare_air(df):
    numeric = ["aqi", "pm25", "pm10", "temperature", "humidity", "pressure"]
    result = (
        df.withColumn("site_id", F.col("site_id").cast("integer"))
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("hour", F.col("hour").cast("integer"))
        .withColumn("month", F.col("month").cast("integer"))
        .withColumn(
            "is_pollution_peak",
            (F.lower(F.col("is_pollution_peak").cast("string")) == "true").cast("integer"),
        )
    )
    for column in numeric:
        result = result.withColumn(column, F.col(column).cast("double"))
    return result.select(
        "site_id",
        "timestamp",
        "hour",
        "month",
        "aqi",
        "pm25",
        "pm10",
        "is_pollution_peak",
    )


def prepare_weather(df):
    numeric = [
        "weather_temperature",
        "weather_humidity",
        "precipitation",
        "snowfall",
        "surface_pressure",
        "wind_speed",
        "wind_direction",
    ]
    result = df.withColumn("timestamp", F.to_timestamp("timestamp"))
    for column in numeric:
        result = result.withColumn(column, F.col(column).cast("double"))
    return result.select("timestamp", *numeric)


def prepare_nmu(df):
    return (
        df.withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("nmu_regime_level", F.col("regime_level").cast("integer"))
        .withColumn("nmu_present", F.lit(1))
        .select("timestamp", "nmu_regime_level", "nmu_present", "announcement_text")
    )


def build_feature_table(air, weather, nmu):
    joined = air.join(weather, on="timestamp", how="left").join(nmu, on="timestamp", how="left")
    return (
        joined.withColumn("nmu_present", F.coalesce(F.col("nmu_present"), F.lit(0)))
        .withColumn("nmu_regime_level", F.coalesce(F.col("nmu_regime_level"), F.lit(0)))
        .withColumn("is_nmu", F.col("nmu_present"))
        # Ordinal escalation: 0 = no NMU, 1 = general forecast, 2..4 = declared regimes.
        .withColumn("nmu_severity", F.col("nmu_present") + F.col("nmu_regime_level"))
        .withColumn("announcement_text", F.coalesce(F.col("announcement_text"), F.lit("")))
        .withColumn("date", F.to_date("timestamp"))
    )


def text_feature_stages():
    tokenizer = Tokenizer(inputCol="announcement_text", outputCol="tokens")
    hashing = HashingTF(inputCol="tokens", outputCol="text_tf", numFeatures=TEXT_FEATURES_DIM)
    idf = IDF(inputCol="text_tf", outputCol="text_features")
    return [tokenizer, hashing, idf]


def supervised_feature_stages():
    imputed = [f"{column}_imp" for column in SUP_NUMERIC]
    imputer = Imputer(inputCols=SUP_NUMERIC, outputCols=imputed, strategy="mean")
    assembler = VectorAssembler(inputCols=imputed + ["text_features"], outputCol="features", handleInvalid="keep")
    return [imputer, *text_feature_stages(), assembler]


def assembled_feature_names() -> list[str]:
    return list(SUP_NUMERIC) + [f"nmu_text_tfidf_{i}" for i in range(TEXT_FEATURES_DIM)]


def importance_rows(task: str, model_name: str, importances) -> list[tuple]:
    values = list(importances.toArray())
    names = assembled_feature_names()
    rows: list[tuple] = []
    text_importance = 0.0
    for name, value in zip(names, values):
        if name.startswith("nmu_text_tfidf_"):
            text_importance += float(value)
        else:
            rows.append((task, model_name, name, float(value)))
    rows.append((task, model_name, "nmu_announcement_text_tfidf", text_importance))
    return rows


def write_single_csv(df, output_path: str) -> None:
    path = Path(output_path)
    temp_dir = path.with_name(f".{path.stem}_spark_tmp")

    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    if path.exists():
        path.unlink()

    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(str(temp_dir))
    )

    part_files = sorted(temp_dir.glob("part-*.csv"))
    if not part_files:
        raise FileNotFoundError(f"Spark did not create part csv in {temp_dir}")

    shutil.move(str(part_files[0]), path)
    shutil.rmtree(temp_dir)


def run_regression(features, metrics: list[tuple]):
    data = features.where(F.col("aqi").isNotNull()).withColumn("label", F.col("aqi"))
    train, test = data.randomSplit([0.8, 0.2], seed=SEED)

    prep = supervised_feature_stages()
    evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")

    models = {
        "linear_regression": LinearRegression(featuresCol="features", labelCol="label"),
        "random_forest": RandomForestRegressor(featuresCol="features", labelCol="label", numTrees=80, maxDepth=8, seed=SEED),
    }

    fitted = {}
    for name, estimator in models.items():
        model = Pipeline(stages=[*prep, estimator]).fit(train)
        predicted = model.transform(test)
        metrics.append(("regression", name, "rmse", evaluator_rmse.evaluate(predicted)))
        metrics.append(("regression", name, "mae", evaluator_mae.evaluate(predicted)))
        metrics.append(("regression", name, "r2", evaluator_r2.evaluate(predicted)))
        fitted[name] = model

    best = fitted["random_forest"]
    predictions = (
        best.transform(data)
        .withColumn("model", F.lit("random_forest"))
        .withColumn("predicted_aqi", F.round(F.col("prediction"), 2))
        .withColumn("actual_aqi", F.round(F.col("label"), 2))
        .withColumn("abs_error", F.round(F.abs(F.col("prediction") - F.col("label")), 2))
        .select("site_id", "timestamp", "date", "hour", "model", "actual_aqi", "predicted_aqi", "abs_error")
        .orderBy("site_id", "timestamp")
    )

    rf_importances = best.stages[-1].featureImportances
    importance = importance_rows("regression", "random_forest", rf_importances)
    return predictions, importance


def run_classification(features, metrics: list[tuple]):
    data = features.where(F.col("is_pollution_peak").isNotNull()).withColumn(
        "label", F.col("is_pollution_peak")
    )
    train, test = data.randomSplit([0.8, 0.2], seed=SEED)

    prep = supervised_feature_stages()
    evaluator_auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")

    models = {
        "logistic_regression": LogisticRegression(featuresCol="features", labelCol="label", maxIter=50),
        "random_forest": RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=80, maxDepth=8, seed=SEED),
    }

    fitted = {}
    for name, estimator in models.items():
        model = Pipeline(stages=[*prep, estimator]).fit(train)
        predicted = model.transform(test)
        metrics.append(("classification", name, "areaUnderROC", evaluator_auc.evaluate(predicted)))
        metrics.append(("classification", name, "accuracy", evaluator_acc.evaluate(predicted)))
        metrics.append(("classification", name, "f1", evaluator_f1.evaluate(predicted)))
        fitted[name] = model

    best = fitted["random_forest"]
    predictions = (
        best.transform(data)
        .withColumn("model", F.lit("random_forest"))
        .withColumn("peak_probability", F.round(vector_to_array(F.col("probability"))[1], 4))
        .withColumn("predicted_peak", F.col("prediction").cast("integer"))
        .withColumn("actual_peak", F.col("label").cast("integer"))
        .select("site_id", "timestamp", "date", "hour", "model", "actual_peak", "predicted_peak", "peak_probability")
        .orderBy("site_id", "timestamp")
    )

    rf_importances = best.stages[-1].featureImportances
    importance = importance_rows("classification", "random_forest", rf_importances)
    return predictions, importance


def run_clustering(features, metrics: list[tuple], k: int):
    data = features
    for column in CLUSTER_NUMERIC:
        data = data.withColumn(column, F.col(column).cast("double"))

    imputed = [f"{column}_cimp" for column in CLUSTER_NUMERIC]
    imputer = Imputer(inputCols=CLUSTER_NUMERIC, outputCols=imputed, strategy="mean")
    assembler = VectorAssembler(inputCols=imputed, outputCol="cluster_raw", handleInvalid="keep")
    scaler = StandardScaler(inputCol="cluster_raw", outputCol="cluster_features", withMean=True, withStd=True)
    kmeans = KMeans(featuresCol="cluster_features", predictionCol="cluster", k=k, seed=SEED)

    model = Pipeline(stages=[imputer, assembler, scaler, kmeans]).fit(data)
    clustered = model.transform(data)

    silhouette = ClusteringEvaluator(featuresCol="cluster_features", predictionCol="cluster").evaluate(clustered)
    metrics.append(("clustering", "kmeans", "silhouette", silhouette))
    metrics.append(("clustering", "kmeans", "k", float(k)))

    points = (
        clustered.withColumn("cluster", F.col("cluster").cast("integer"))
        .select(
            "site_id",
            "timestamp",
            "date",
            "hour",
            "cluster",
            F.round(F.col("aqi"), 2).alias("aqi"),
            F.round(F.col("pm25"), 2).alias("pm25"),
            F.round(F.col("weather_temperature"), 2).alias("weather_temperature"),
            F.round(F.col("wind_speed"), 2).alias("wind_speed"),
            "is_nmu",
        )
        .orderBy("site_id", "timestamp")
    )

    profile = (
        clustered.groupBy("cluster")
        .agg(
            F.count("*").alias("rows_count"),
            F.round(F.avg("aqi"), 2).alias("avg_aqi"),
            F.round(F.avg("pm25"), 2).alias("avg_pm25"),
            F.round(F.avg("pm10"), 2).alias("avg_pm10"),
            F.round(F.avg("weather_temperature"), 2).alias("avg_weather_temperature"),
            F.round(F.avg("weather_humidity"), 2).alias("avg_weather_humidity"),
            F.round(F.avg("wind_speed"), 2).alias("avg_wind_speed"),
            F.round(F.avg("surface_pressure"), 2).alias("avg_surface_pressure"),
            F.round(F.avg("nmu_severity"), 3).alias("avg_nmu_severity"),
            F.round(F.avg("is_nmu"), 3).alias("nmu_share"),
            F.round(F.avg("is_pollution_peak"), 3).alias("peak_share"),
        )
        .withColumn("cluster", F.col("cluster").cast("integer"))
        .orderBy("cluster")
    )

    return points, profile


def train_models(
    *,
    air_input: str,
    weather_input: str,
    nmu_input: str,
    kmeans_k: int,
    regression_output: str,
    classification_output: str,
    metrics_output: str,
    feature_importance_output: str,
    clusters_output: str,
    cluster_profile_output: str,
) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        air = prepare_air(read_csv(spark, air_input))
        weather = prepare_weather(read_csv(spark, weather_input))
        nmu = prepare_nmu(read_csv(spark, nmu_input))

        features = build_feature_table(air, weather, nmu).cache()
        total_rows = features.count()

        metrics: list[tuple] = []
        importance: list[tuple] = []

        reg_pred, reg_importance = run_regression(features, metrics)
        cls_pred, cls_importance = run_classification(features, metrics)
        importance.extend(reg_importance)
        importance.extend(cls_importance)

        cluster_points, cluster_profile = run_clustering(features, metrics, kmeans_k)

        metrics_df = spark.createDataFrame(metrics, ["task", "model", "metric", "value"]).withColumn(
            "value", F.round(F.col("value"), 4)
        )
        importance_df = (
            spark.createDataFrame(importance, ["task", "model", "feature", "importance"])
            .withColumn("importance", F.round(F.col("importance"), 4))
            .orderBy("task", F.col("importance").desc())
        )

        write_single_csv(reg_pred, regression_output)
        write_single_csv(cls_pred, classification_output)
        write_single_csv(metrics_df, metrics_output)
        write_single_csv(importance_df, feature_importance_output)
        write_single_csv(cluster_points, clusters_output)
        write_single_csv(cluster_profile, cluster_profile_output)

        print(f"Feature rows: {total_rows}")
        print("Metrics:")
        for row in metrics_df.orderBy("task", "model", "metric").collect():
            print(f"  {row['task']:<14} {row['model']:<20} {row['metric']:<14} {row['value']}")
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--air-input", default=DEFAULT_AIR_INPUT)
    parser.add_argument("--weather-input", default=DEFAULT_WEATHER_INPUT)
    parser.add_argument("--nmu-input", default=DEFAULT_NMU_INPUT)
    parser.add_argument("--kmeans-k", type=int, default=4)
    parser.add_argument("--regression-output", default=DEFAULT_REGRESSION_OUTPUT)
    parser.add_argument("--classification-output", default=DEFAULT_CLASSIFICATION_OUTPUT)
    parser.add_argument("--metrics-output", default=DEFAULT_METRICS_OUTPUT)
    parser.add_argument("--feature-importance-output", default=DEFAULT_FEATURE_IMPORTANCE_OUTPUT)
    parser.add_argument("--clusters-output", default=DEFAULT_CLUSTERS_OUTPUT)
    parser.add_argument("--cluster-profile-output", default=DEFAULT_CLUSTER_PROFILE_OUTPUT)
    args = parser.parse_args()

    train_models(
        air_input=args.air_input,
        weather_input=args.weather_input,
        nmu_input=args.nmu_input,
        kmeans_k=args.kmeans_k,
        regression_output=args.regression_output,
        classification_output=args.classification_output,
        metrics_output=args.metrics_output,
        feature_importance_output=args.feature_importance_output,
        clusters_output=args.clusters_output,
        cluster_profile_output=args.cluster_profile_output,
    )


if __name__ == "__main__":
    main()
