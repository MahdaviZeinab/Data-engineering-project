# Running the Bronze pipeline with GitHub Actions + OpenStack

How it works:

| Workflow | Runs on | When |
|---|---|---|
| `ci.yml` (tests) | GitHub's free machine (`ubuntu-latest`) | every push to `main`, every pull request |
| `bronze-ingest.yml` (download + Spark) | **your OpenStack VM** (self-hosted runner) | when you click "Run workflow", and every day 02:00 UTC |

The VM runs a small program from GitHub (the "runner"). It connects **out** to GitHub over
HTTPS and waits for jobs. GitHub never connects in to the VM, so you do not need to open any port
except SSH for yourself.

Do the parts in this order: **A → B → C → D → E**.

---

## A. Before you start

1. **You need admin rights on the GitHub repo** `MahdaviZeinab/Data-engineering-project`.
   Only admins see *Settings → Actions → Runners*. If you are not admin, ask Zeinab to either make
   you admin or do part C (it takes 5 minutes).
2. You need access to your course OpenStack (Horizon web page) and an SSH key pair there.

---

## B. Create and prepare the OpenStack VM

### B1. Create the instance (in Horizon: *Compute → Instances → Launch Instance*)

| Setting | Choose |
|---|---|
| Image | **Ubuntu 22.04** (recommended) or 24.04 |
| Flavor | at least **4 vCPU, 8 GB RAM** (Spark is told to use 4 GB) |
| Disk | at least **40 GB** (raw data 2.2 GB + zips + Parquet + Python packages), or add a volume (B4) |
| Key pair | your key |
| Security group | allow **SSH (port 22) in**, ideally only from your IP. Outgoing traffic: allow all (default) |

Then give it a **floating IP** (*Instances → ▾ → Associate Floating IP*).

### B2. Log in

On your Mac:

```bash
chmod 600 ~/Downloads/my-key.pem          # your key file
ssh -i ~/Downloads/my-key.pem ubuntu@<FLOATING-IP>
```

### B3. Install Java, Python and tools

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y openjdk-17-jre-headless python3 python3-venv python3-pip git curl tar
java -version        # should say 17
python3 --version    # 3.10 on Ubuntu 22.04, 3.12 on 24.04
```

### B4. Make the data folder `/data/inmet`

The workflow keeps all data in `/data/inmet` (raw files, Bronze output, Python venv), outside the
GitHub work folder, so it survives between runs.

**Option 1 – simple (enough disk on the VM, see `df -h /`):**

```bash
sudo mkdir -p /data/inmet
sudo chown -R ubuntu:ubuntu /data
```

**Option 2 – separate volume (if the VM disk is small):**
In Horizon: *Volumes → Create Volume* (30 GB) → *▾ → Manage Attachments* → attach to the VM. Then on the VM:

```bash
lsblk                                   # find the new disk, usually /dev/vdb (no partitions)
sudo mkfs.ext4 /dev/vdb                 # ONLY on the new empty disk! This erases it.
sudo mkdir -p /data
sudo mount /dev/vdb /data
echo '/dev/vdb /data ext4 defaults,nofail 0 2' | sudo tee -a /etc/fstab   # mount at every boot
sudo mkdir -p /data/inmet
sudo chown -R ubuntu:ubuntu /data
df -h /data
```

If you want another folder, change `DATA_ROOT: /data/inmet` at the top of
`.github/workflows/bronze-ingest.yml`.

---

## C. Install the GitHub runner on the VM

### C1. Get the commands from GitHub

1. Open the repo on github.com → **Settings** → **Actions** → **Runners** → **New self-hosted runner**.
2. Choose **Linux** and **x64**.
3. GitHub shows a box of commands, including a **token**. The token works for 1 hour only.

### C2. Run them on the VM (still logged in with SSH as `ubuntu`)

Copy the "Download" commands exactly from the GitHub page. They look like this
(the version number on the page is the right one, use that):

```bash
mkdir ~/actions-runner && cd ~/actions-runner
curl -o actions-runner-linux-x64-X.Y.Z.tar.gz -L https://github.com/actions/runner/releases/download/vX.Y.Z/actions-runner-linux-x64-X.Y.Z.tar.gz
tar xzf ./actions-runner-linux-x64-X.Y.Z.tar.gz
```

Then the "Configure" command from the page:

```bash
./config.sh --url https://github.com/MahdaviZeinab/Data-engineering-project --token <TOKEN-FROM-PAGE>
```

It asks some questions. Answer like this:

| Question | Answer |
|---|---|
| runner group | press Enter (Default) |
| name of runner | `openstack-bronze` |
| **additional labels** | **`openstack`**  ← important, the workflow looks for this label |
| work folder | press Enter (`_work`) |

(`self-hosted`, `Linux` and `X64` labels are added automatically.)

### C3. Run it as a service (so it keeps running after you log out and after a reboot)

Do **not** use `./run.sh` for real use (it stops when you close SSH). Instead:

```bash
cd ~/actions-runner
sudo ./svc.sh install ubuntu     # run the service as user ubuntu
sudo ./svc.sh start
sudo ./svc.sh status             # should say "active (running)"
```

### C4. Check in GitHub

*Settings → Actions → Runners*: you should see **openstack-bronze** with a green dot and status
**Idle**, and labels `self-hosted`, `Linux`, `X64`, `openstack`.

---

## D. Push the workflows to GitHub (on your Mac)

The workflow files are already in the right place:
`Data-engineering-project/.github/workflows/ci.yml` and `bronze-ingest.yml`
(GitHub only reads workflows from `.github/workflows/` at the **repo root**).

```bash
cd ~/Documents/DAT535/Data-engineering-project
git pull origin main                     # get your teammates' changes first
git add -A .github inmet_weather
git status                               # check: workflows added, inmet_weather/github_workflows removed, no data/ files
git commit -m "Add CI and Bronze ingest workflows (self-hosted OpenStack runner)"
git push origin main
```

`data/` is in `.gitignore`, so the 2.2 GB of CSV files will not be pushed.

---

## E. Run it

### E1. Tests (automatic)

The push in D already starts **ci**. Open the repo → **Actions** tab → **ci** → it should get a
green check after a few minutes.

### E2. Bronze (on the VM)

1. **Actions** tab → **bronze-ingest** (left side) → **Run workflow** (right side).
2. Fill in:
   - **years**: `2025` for a first test. Later all years: `2020 2021 2022 2023 2024 2025`
   - **env**: `prod` (or `dev` to test)
3. Click the green **Run workflow** button. Open the run to watch the log.

What happens on the VM:

1. checks Java, Python and free disk
2. creates `/data/inmet/venv` the first time and installs `requirements.txt`
3. downloads and unzips the INMET files into `/data/inmet/raw/<year>/` (years already there are skipped)
4. Spark writes Bronze to `/data/inmet/lake/<env>/bronze/`
5. the run summary (`_runs/*.json`) and the file checks are attached to the run as an **artifact**
   (download it at the bottom of the run page)

### E3. Look at the result on the VM

```bash
ls /data/inmet/raw
ls /data/inmet/lake/prod/bronze
cat /data/inmet/lake/prod/bronze/_runs/*.json | tail -40
du -sh /data/inmet/*
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Job stays at **"Waiting for a runner to pick up this job"** | Runner is offline or label is wrong. On VM: `cd ~/actions-runner && sudo ./svc.sh status`. In GitHub the runner must have the label `openstack`. Add it in *Settings → Actions → Runners → openstack-bronze → Labels*. |
| `java: command not found` | `sudo apt install -y openjdk-17-jre-headless`, then `sudo ./svc.sh stop && sudo ./svc.sh start` |
| `Permission denied` in `/data/inmet` | `sudo chown -R ubuntu:ubuntu /data/inmet` |
| `No space left on device` | `df -h`. Make the disk bigger or use a volume (B4 option 2). |
| Spark crashes / `OutOfMemoryError` / runner disappears during the run | Flavor too small. Use 8 GB RAM or more, or ingest fewer years at a time. |
| `Bad years input` | Write years with spaces only: `2020 2021`, not `2020,2021`. |
| Download fails | The INMET site is sometimes slow. Just run the workflow again; finished years are skipped. |
| Token expired during `config.sh` | Click *New self-hosted runner* again to get a new token. |

## Good to know

- **Security:** never add `pull_request` as a trigger to `bronze-ingest.yml`. If the repo is
  public, code from a stranger's pull request could then run on your VM. Now only people with
  write access can start it. Also set *Settings → Actions → General → Fork pull request workflows
  → Require approval for all outside collaborators*.
- **The daily schedule** (02:00 UTC) runs with years `2025` and env `prod`. Because years that are
  already downloaded are skipped, it re-loads the same files each day. Remove the two `schedule:`
  lines in `bronze-ingest.yml` if you only want manual runs. GitHub runs schedules only from the
  `main` branch, and the VM must be on at that time.
- **Stop / remove the runner:** `sudo ./svc.sh stop`, `sudo ./svc.sh uninstall`, then
  `./config.sh remove --token <TOKEN>` (token from the runner's page in GitHub).
- If you shut down the VM, the runner shows **Offline** and bronze jobs wait (for up to 24 h)
  until it is back. Tests (`ci`) are not affected.
