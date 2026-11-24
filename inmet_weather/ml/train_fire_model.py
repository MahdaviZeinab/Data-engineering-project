"""
Machine learning on Gold: predict a "high-fire day" from the weather (logistic regression).

Question: from the weather of a state on a day (and the days before), can we tell
if it will be a high-fire day for that state?

Target:  high_fire_day = the state has more fires than on 90 % of its days in the
         TRAIN period (each state gets its own limit: 50 fires is a lot in one state
         and normal in another). The limit is learned from train data only.
Split:   by date, never at random (neighbouring days look almost the same):
           train      local_date <  --valid-from
           validation --valid-from <= local_date < --test-from  (to choose the threshold)
           test       local_date >= --test-from                 (used once, at the end)
         dev:  train Jan-Jun 2025, validation Jul-Aug 2025, test Sep-Dec 2025 (code test only)
         prod: train 2020-2023,    validation 2024,         test 2025 (the real result)
Model:   Spark ML Pipeline: state + month as one-hot, numbers scaled, logistic regression
         with more weight on the rare high-fire days.

Tables it writes (database "gold"):
  gold.fire_model_metrics      one row per run and data set (append: keeps the history)
  gold.fire_model_predictions  the test days: real value, probability, prediction
  gold.fire_model_weights      one row per feature: weight and odds ratio
and the model itself in <lake>/models/fire_lr_<run_id>.

Run (from the inmet_weather folder):
  python -m ml.train_fire_model --valid-from 2025-07-01 --test-from 2025-09-01            # dev
  python -m ml.train_fire_model --env prod --valid-from 2024-01-01 --test-from 2025-01-01
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from common.report import add_to_summary, markdown_table, report_failure
from common.spark import save_table, start_spark

DAILY = "gold.fire_weather_daily"
METRICS = "gold.fire_model_metrics"
PREDICTIONS = "gold.fire_model_predictions"
WEIGHTS = "gold.fire_model_weights"

HIGH_FIRE_QUANTILE = 0.9            # high-fire day = above the state's 90 % level (train period)
THRESHOLDS = [x / 20 for x in range(1, 20)]   # 0.05 ... 0.95, the best one is picked on validation
BASELINE_SCORE = 4                  # simple rule to beat: risk_score >= 4 means "high-fire day"

# Numbers the model sees: today's weather and the weather of the days before.
NUMBER_FEATURES = [
    "humidity_min_pct", "temp_max_c", "rain_mm", "wind_max_ms", "days_without_rain",
    "humidity_min_3d", "temp_max_3d", "rain_7d",
]


# Step 1: features
# ---------------------------------------------------------------------------
def add_features(daily):
    # Add the weather of the days BEFORE today (only the past: no information from the future)
    # and the month. Only days with weather data are used.
    before = Window.partitionBy("state").orderBy("local_date")
    last_3 = before.rowsBetween(-3, -1)
    last_7 = before.rowsBetween(-7, -1)
    df = daily.where(F.col("weather_ok"))
    df = df.withColumn("humidity_min_3d", F.avg("humidity_min_pct").over(last_3))
    df = df.withColumn("temp_max_3d", F.avg("temp_max_c").over(last_3))
    df = df.withColumn("rain_7d", F.sum("rain_mm").over(last_7))
    df = df.withColumn("month", F.month("local_date").cast("string"))
    return df.dropna(subset=NUMBER_FEATURES)          # the first days of the data have no history


# Step 2: target and split
# ---------------------------------------------------------------------------
def split_by_date(df, valid_from, test_from):
    # Train, validation and test, by date.
    train = df.where(F.col("local_date") < F.lit(valid_from).cast("date"))
    valid = df.where((F.col("local_date") >= F.lit(valid_from).cast("date"))
                     & (F.col("local_date") < F.lit(test_from).cast("date")))
    test = df.where(F.col("local_date") >= F.lit(test_from).cast("date"))
    return train, valid, test


def high_fire_limits(train):
    # Per state: the number of fires that only 10 % of its train days go above.
    return train.groupBy("state").agg(
        F.percentile_approx("fires", HIGH_FIRE_QUANTILE).alias("high_fire_limit"))


def add_target(df, limits):
    # label = 1 on a high-fire day, else 0. A state not seen in train gets no label (left out).
    df = df.join(F.broadcast(limits), "state", "inner")
    return df.withColumn("label", (F.col("fires") > F.col("high_fire_limit")).cast("double"))


def add_weights(train):
    # High-fire days are rare, so they get more weight; otherwise the model learns
    # to say "no" every time. weight = (number of 0 days) / (number of 1 days).
    counts = {r["label"]: r["count"] for r in train.groupBy("label").count().collect()}
    ratio = counts.get(0.0, 1) / max(counts.get(1.0, 0), 1)
    return train.withColumn("weight", F.when(F.col("label") == 1.0, ratio).otherwise(1.0))


# Step 3: the model
# ---------------------------------------------------------------------------
def make_pipeline():
    # state and month -> numbers -> one-hot; all features in one vector; scale; model.
    return Pipeline(stages=[
        StringIndexer(inputCols=["state", "month"], outputCols=["state_i", "month_i"],
                      handleInvalid="keep"),
        OneHotEncoder(inputCols=["state_i", "month_i"], outputCols=["state_v", "month_v"]),
        VectorAssembler(inputCols=NUMBER_FEATURES + ["state_v", "month_v"], outputCol="raw"),
        StandardScaler(inputCol="raw", outputCol="features"),
        LogisticRegression(featuresCol="features", labelCol="label", weightCol="weight",
                           maxIter=100, regParam=0.01),
    ])


# The model gives a vector [P(0), P(1)]; this takes P(1) out as a plain number.
# (A small Python function: pyspark.ml.functions.vector_to_array would also work,
#  but it needs the pandas library, which the pipeline does not install.)
second_value = F.udf(lambda vector: float(vector[1]), DoubleType())


def add_probability(predictions):
    # Keep P(high-fire day) as a plain number column.
    return predictions.withColumn("probability_high", second_value("probability"))


# Step 4: how good is it?
# ---------------------------------------------------------------------------
def scores(df, predicted):
    # Precision, recall and F1 of a 0/1 prediction column (as a Spark expression).
    row = df.select(
        F.sum(((predicted == 1) & (F.col("label") == 1)).cast("int")).alias("tp"),
        F.sum(((predicted == 1) & (F.col("label") == 0)).cast("int")).alias("fp"),
        F.sum(((predicted == 0) & (F.col("label") == 1)).cast("int")).alias("fn"),
    ).first()
    tp, fp, fn = row["tp"] or 0, row["fp"] or 0, row["fn"] or 0
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"precision": round(precision, 3), "recall": round(recall, 3), "f1": round(f1, 3)}


def best_threshold(valid):
    # The probability above which we say "high-fire day": the one with the best F1
    # on VALIDATION. The test set is not used to choose it.
    best = max(THRESHOLDS, key=lambda t: scores(valid, (F.col("probability_high") >= t).cast("int"))["f1"])
    return best


def auc(df):
    # ROC-AUC: 0.5 = guessing, 1.0 = perfect ranking of high-fire days.
    if df.select("label").distinct().count() < 2:
        return None                         # AUC needs both 0 and 1 days
    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="probability_high")
    return round(evaluator.evaluate(df), 3)


def evaluate(df, name, threshold, run_id):
    # One metrics row for a data set: the model and the simple risk_score rule.
    model = scores(df, (F.col("probability_high") >= threshold).cast("int"))
    rule = scores(df, (F.col("risk_score") >= BASELINE_SCORE).cast("int"))
    counts = df.agg(F.count("*").alias("rows"), F.sum("label").alias("positives")).first()
    return (run_id, name, counts["rows"], int(counts["positives"] or 0), auc(df), threshold,
            model["precision"], model["recall"], model["f1"],
            rule["precision"], rule["recall"], rule["f1"])


METRICS_SCHEMA = ("run_id string, data_set string, rows long, high_fire_days long, auc double, "
                  "threshold double, precision double, recall double, f1 double, "
                  "rule_precision double, rule_recall double, rule_f1 double")


def feature_weights(spark, fitted):
    # One row per feature: its weight and odds ratio = exp(weight).
    # Features are scaled, so weights can be compared: odds ratio 2.0 means
    # "one standard deviation more of this feature doubles the odds of a high-fire day".
    lr = fitted.stages[-1]
    names = (list(NUMBER_FEATURES)
             + [f"state={s}" for s in fitted.stages[0].labelsArray[0]]
             + [f"month={m}" for m in fitted.stages[0].labelsArray[1]])
    weights = lr.coefficients.toArray().tolist()
    # (the extra "unknown" slot that StringIndexer adds is dropped again by the one-hot encoder)
    rows = [(n, round(w, 4)) for n, w in zip(names, weights)]
    df = spark.createDataFrame(rows, "feature string, weight double")
    return df.withColumn("odds_ratio", F.round(F.exp("weight"), 3))


# The whole job
# ---------------------------------------------------------------------------
def run_training(spark, valid_from, test_from, run_id=None):
    # Train on train, choose the threshold on validation, measure once on test.
    # Returns (fitted model, metrics, test predictions, feature weights).
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    df = add_features(spark.table(DAILY))
    train, valid, test = split_by_date(df, valid_from, test_from)
    limits = high_fire_limits(train)
    train = add_weights(add_target(train, limits)).cache()
    valid = add_target(valid, limits)
    test = add_target(test, limits)
    if train.count() == 0 or valid.count() == 0 or test.count() == 0:
        raise ValueError("train, validation or test has no rows: check --valid-from and --test-from")

    fitted = make_pipeline().fit(train)
    valid_pred = add_probability(fitted.transform(valid)).cache()
    test_pred = add_probability(fitted.transform(test)).cache()
    threshold = best_threshold(valid_pred)

    metrics = spark.createDataFrame([
        evaluate(add_probability(fitted.transform(train)), "train", threshold, run_id),
        evaluate(valid_pred, "validation", threshold, run_id),
        evaluate(test_pred, "test", threshold, run_id),
    ], METRICS_SCHEMA)
    predictions = test_pred.select(
        "state", "local_date", "fires", "high_fire_limit", "label", "risk_score",
        F.round("probability_high", 4).alias("probability_high"),
        (F.col("probability_high") >= threshold).cast("int").alias("prediction"),
        F.lit(run_id).alias("_ml_run_id"))
    return fitted, metrics, predictions, feature_weights(spark, fitted)


def save_results(spark, lake, fitted, metrics, predictions, weights):
    # Metrics are added to the history; predictions and weights show the latest run.
    run_id = metrics.first()["run_id"]
    metrics.write.mode("append").format("parquet").saveAsTable(METRICS)
    save_table(spark, predictions, PREDICTIONS)
    save_table(spark, weights, WEIGHTS)
    path = f"{lake}/models/fire_lr_{run_id}"
    fitted.write().overwrite().save(path)
    return path


def write_report(metrics):
    # The model scores on the GitHub run page (see common/report.py).
    def short(value):
        return "-" if value is None else round(value, 3)
    rows = [(r["data_set"], r["rows"], r["high_fire_days"], short(r["auc"]), r["threshold"],
             short(r["precision"]), short(r["recall"]), short(r["f1"]), short(r["rule_f1"]))
            for r in metrics.collect()]
    add_to_summary("### Fire model (logistic regression)\n\n" + markdown_table(
        ["data set", "rows", "high-fire days", "AUC", "threshold", "precision", "recall", "F1",
         "rule F1 (risk_score >= 4)"], rows))


def main():
    # Read the command line options, start Spark, train, evaluate and save.
    parser = argparse.ArgumentParser(description="Train a logistic regression for high-fire days.")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--valid-from", required=True, help="first validation day, e.g. 2024-01-01")
    parser.add_argument("--test-from", required=True, help="first test day, e.g. 2025-01-01")
    args = parser.parse_args()

    project = Path(__file__).resolve().parents[1]          # the inmet_weather folder
    lake = os.environ.get("DAT535_OUTPUT_DIR") or str(project / "data" / "lake" / args.env)

    spark = start_spark("fire-model", lake, memory="2g")
    try:
        fitted, metrics, predictions, weights = run_training(spark, args.valid_from, args.test_from)
        path = save_results(spark, lake, fitted, metrics, predictions, weights)
        print("ML: metrics (rule_* = the simple rule risk_score >= 4, to compare)")
        metrics.show(truncate=False)
        print("ML: strongest features (odds ratio > 1 raises the risk, < 1 lowers it)")
        weights.orderBy(F.abs("weight").desc()).show(12, truncate=False)
        print("ML: model saved in", path)
        write_report(metrics)                  # the scores on the GitHub run page
        return 0
    except Exception as error:
        print("ML FAILED:", error)
        report_failure("Fire model", error)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
