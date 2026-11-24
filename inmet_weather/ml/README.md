# ML: predict high-fire days (logistic regression)

Reads `gold.fire_weather_daily` and learns, from the weather of a state on a day and the
days before, whether it will be a **high-fire day** for that state.

| | |
|---|---|
| Target | `label = 1` when the state has more fires than on 90 % of its **train** days (a limit per state, learned from train only) |
| Features | min humidity, max temperature, rain, max wind, days without rain, humidity and temperature of the last 3 days, rain of the last 7 days, state and month (one-hot) |
| Model | Spark ML `Pipeline`: `StringIndexer` → `OneHotEncoder` → `VectorAssembler` → `StandardScaler` → `LogisticRegression` (rare high-fire days get more weight) |
| Split | by date, never at random: train < `--valid-from` ≤ validation < `--test-from` ≤ test |
| Threshold | the probability cut-off with the best F1 on **validation**; the test set is used once |
| Compared with | the simple rule `risk_score >= 4` (columns `rule_*` in the metrics) |

**dev** only has 2025, so it checks that the code runs (train Jan–Jun, validation Jul–Aug,
test Sep–Dec 2025); its scores mean nothing. **prod** gives the real result
(train 2020–2023, validation 2024, test 2025).

## Output
| Spark table | Content |
|---|---|
| `gold.fire_model_metrics` | one row per run and data set: rows, high-fire days, AUC, threshold, precision, recall, F1, and the same for the rule (history: rows are added) |
| `gold.fire_model_predictions` | the test days: fires, label, probability, prediction (latest run) |
| `gold.fire_model_weights` | one row per feature: weight and odds ratio (latest run) |

The model itself is saved in `<lake>/models/fire_lr_<run_id>`.

**Reading the weights:** features are scaled, so an odds ratio of 2.0 means "one standard
deviation more of this feature doubles the odds of a high-fire day"; below 1 lowers them.
This shows a link, not a cause: the dry season is also the burning season.

## Run
```bash
python -m ml.train_fire_model --valid-from 2025-07-01 --test-from 2025-09-01             # dev
python -m ml.train_fire_model --env prod --valid-from 2024-01-01 --test-from 2025-01-01  # prod
python -m pytest -q tests/test_fire_model.py
```
