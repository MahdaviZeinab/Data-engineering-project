# INMET weather + fire pipeline (DAT535)

Weather (INMET) and fires (INPE) in Brazil, 2020–2025.

```
download ─► Bronze (raw) ─► Silver (clean) ─► Gold (dashboard + ML)
```

The ML model is built. The dashboard is not built yet, but its Gold tables are ready.


## Folders

```
inmet_weather/
├── bronze/   download and load the raw files
├── silver/   clean weather and fires
├── gold/     dashboard and ML tables
├── ml/       fire model
├── common/   shared Spark helpers
├── tests/
└── data/     all data (not in Git)
```


## Data

All data is saved in the `data/` folder.

The raw files go in `data/raw/`. The tables go in `data/lake/`.

On the VM, this is `/data/inmet/`.


## 1. Set up the VM

```bash
./lab1_install-spark.sh          # install Spark (course script)
```

```bash
sudo apt update
sudo apt install -y openjdk-17-jre-headless python3-venv git
sudo mkdir -p /data/inmet && sudo chown -R ubuntu:ubuntu /data
```

```bash
git clone https://github.com/MahdaviZeinab/Data-engineering-project.git
cd Data-engineering-project/inmet_weather
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m pytest -q
```


## 2. Connect the VM to GitHub

1. Add a self-hosted runner (Settings → Actions → Runners) with the label `openstack`.

2. Add the environments `dev` and `main` (with required reviewers).


## 3. Run the pipeline

| | Dev | Prod |
|---|---|---|
| Starts on | push to any branch except `main` | push to `main` |
| Years | 2025 | 2020–2025 |
| Approval | no | yes |

Push a branch: dev runs. Merge into `main`: prod runs after approval.


## Run without GitHub

You can also run each step yourself on the VM.

First go to the project folder:

```bash
cd inmet_weather
```

1. Bronze (download the weather and fire files, then load them):
   ```bash
   python bronze/injection.py
   python bronze/injection.py --source fire
   python -m bronze.bronze_ingest
   python -m bronze.fire_ingest
   ```

2. Silver (clean the data):
   ```bash
   python -m silver.inmet_build
   python -m silver.fire_build
   ```

3. Gold (make the dashboard tables):
   ```bash
   python -m gold.gold_build
   python -m gold.state_month_risk
   ```

4. ML (train the fire model):
   ```bash
   python -m ml.train_fire_model --valid-from 2025-07-01 --test-from 2025-09-01
   ```

Run the steps in this order. Each step needs the one before it.

To run only one year, add `--years 2025` at the end of each command, for example:

```bash
python -m bronze.bronze_ingest --years 2025
```
