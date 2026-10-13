# Beginner's guide: build, run and publish this project

This guide takes you from an empty laptop to a working project on GitHub. It is split into phases. Do them in order, and push to GitHub at the end of every phase so your commit history shows real progress.

Suggested pace: about 2 weeks at 1 to 2 hours a day. Going slower is fine. Understanding the code matters more than speed, because interviewers will ask you about it.

| Phase | When | What you finish with |
|---|---|---|
| 0. Install tools | Day 1 | Git, Docker, VS Code, GitHub account |
| 1. First push to GitHub | Day 1 to 2 | The code is on GitHub |
| 2. Start the platform | Day 2 to 3 | All containers running |
| 3. Run the pipeline in Airflow | Day 3 to 4 | Data generated, validated, transformed |
| 4. Explore the dashboard and API | Day 5 | You can see freshness, jobs and violations |
| 5. Read and understand the code | Day 6 to 8 | You can explain every file |
| 6. Run the tests | Day 8 | Green tests, CI on GitHub |
| 7. Benchmark with 5M events | Day 9 | Your real before/after runtime numbers |
| 8. Move storage to Amazon S3 | Day 10 to 11 | Data in a real S3 bucket |
| 9. Polish for your resume | Day 12 to 14 | Screenshots, README results, resume bullet |

---

## Phase 0: Install the tools (Day 1)

Install these, accepting the default options:

1. **Git**: https://git-scm.com/downloads. On Windows this also installs "Git Bash"; use it as your terminal for the commands in this guide.
2. **Docker Desktop**: https://www.docker.com/products/docker-desktop. On Windows it will ask to enable WSL 2; say yes and restart.
   After installing, open Docker Desktop → Settings → Resources and give it **at least 6 GB of memory** (8 GB is better). Spark needs it.
3. **VS Code**: https://code.visualstudio.com (a code editor).
4. **A GitHub account**: https://github.com/signup.
5. Optional, only for Phase 4's "dev mode": **Node.js 20 LTS** from https://nodejs.org.

Check that everything works. Open a terminal and run:

```bash
git --version
docker --version
docker compose version
```

Each should print a version number. If `docker` fails, make sure Docker Desktop is open and running.

Tell Git who you are (use the email of your GitHub account):

```bash
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
```

---

## Phase 1: Put the code on GitHub (Day 1 to 2)

### 1.1 Unzip the project

Unzip `dataops-monitoring-platform.zip` somewhere easy, for example your Documents folder. Open the folder in VS Code (File → Open Folder).

Open a terminal **inside the project folder**. In VS Code: Terminal → New Terminal. Check you are in the right place:

```bash
ls
```

You should see `README.md`, `docker-compose.yml`, `pipeline`, `api`, `dashboard` and so on.

### 1.2 Create an empty repository on GitHub

1. Go to https://github.com/new.
2. Repository name: `dataops-monitoring-platform`.
3. Public (so recruiters can see it).
4. **Do not** tick "Add a README", ".gitignore" or "license". The project already has them, and an extra README would cause a conflict.
5. Click **Create repository**. Keep the page open; it shows your repository URL, like `https://github.com/YOUR-USERNAME/dataops-monitoring-platform.git`.

### 1.3 Make your first commit and push

Run these one at a time in the project folder:

```bash
git init
git add .
git status
```

`git status` lists the files that will be committed. Check that `.env` is **not** in the list (it should not exist yet anyway). Then:

```bash
git commit -m "Initial project structure: pipeline, Airflow DAG, API and dashboard"
git branch -M main
git remote add origin https://github.com/YOUR-USERNAME/dataops-monitoring-platform.git
git push -u origin main
```

When you push for the first time, a browser window or popup asks you to sign in to GitHub. Sign in and approve. (On Windows and Mac, the Git installer includes a credential manager that does this.)

If instead the terminal asks for a password, GitHub no longer accepts your account password there. Create a token: GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic) → Generate new token, tick the `repo` scope, and paste the token when asked for the password.

Refresh your GitHub page. Your code is online.

### 1.4 The commit habit

From now on, after each phase:

```bash
git add .
git status          # always look before you commit
git commit -m "Short description of what you did"
git push
```

Good commit messages describe the change: "Add S3 storage mode", "Fix freshness SLA calculation". Avoid "update" or "stuff".

---

## Phase 2: Start the platform (Day 2 to 3)

### 2.1 Create your settings file

```bash
cp .env.example .env
```

On Windows Command Prompt use `copy .env.example .env` instead.

**Linux only:** run `id -u` and put the number into `AIRFLOW_UID` in `.env`. Mac and Windows users can leave it at 50000.

### 2.2 Build and start everything

```bash
docker compose up -d --build
```

The first build takes 5 to 15 minutes: it downloads Airflow, installs Java and PySpark, and builds the dashboard. Later starts take seconds.

Check the containers:

```bash
docker compose ps
```

You want `airflow-webserver`, `airflow-scheduler`, `api`, `dashboard`, `airflow-db` and `monitoring-db` to say "running" (or "healthy"). `airflow-init` should say "exited (0)"; it is a one-time setup job, and exit code 0 means it succeeded.

If something is not running, look at its logs:

```bash
docker compose logs airflow-init
docker compose logs airflow-scheduler --tail 100
```

### 2.3 Open the three web pages

- Airflow: http://localhost:8080 (login `airflow` / `airflow`). It can take a minute after startup.
- API docs: http://localhost:8000/docs
- Dashboard: http://localhost:3000. It will be mostly empty. That is expected: no pipeline has run yet.

Commit: there is nothing new to commit (your `.env` is ignored on purpose). Move on.

---

## Phase 3: Run the pipeline in Airflow (Day 3 to 4)

1. In Airflow, find `ecommerce_events_pipeline`. New DAGs start paused: click the toggle on the left to unpause it.
2. Click the ▶ (Trigger DAG) button on the right.
3. Click the DAG name, then the **Graph** view. Watch the three tasks turn green: `generate_events` → `validate_events` → `transform_events`. With 100,000 events this takes a couple of minutes.
4. Click a task square, then **Logs**. In `validate_events` you will see the row counts and how many rows broke each rule.

Look at the files it created in your project's `data/` folder:

```
data/raw/events/ingest_date=YYYY-MM-DD/part-00000.csv     <- raw CSV, open it in VS Code
data/validated/events/event_date=YYYY-MM-DD/*.parquet     <- clean data
data/quarantine/events/ingest_date=YYYY-MM-DD/*.parquet   <- bad rows
data/curated/daily_sales/...                              <- analytics tables
```

The `event_date=...` folders are **partitions**. When a later query asks for one date, Spark reads only that folder. This is one of the optimizations behind the runtime improvement.

### Make a job fail on purpose

Monitoring is only interesting when something breaks. In `.env`, set:

```
MAX_INVALID_RATIO=0.01
```

Recreate the Airflow containers so they read the new value, then trigger the DAG again:

```bash
docker compose up -d
```

About 3% of rows are bad, which is above the new 1% limit, so `validate_events` fails, retries once and fails again. Check the dashboard: the failed task and its error appear in red. Set the value back to `0.10` and run `docker compose up -d` again.

Commit any notes you added, for example a `docs/NOTES.md` with what you learned.

---

## Phase 4: Explore the dashboard and API (Day 5)

Open http://localhost:3000:

- **Top row**: the pipeline's stages. Each dataset is green (fresh), amber (more than 75% of its SLA used) or red (stale, past its 26-hour SLA).
- **Numbers**: task runs in the last 24 hours, failures, rule violations.
- **Quality violations**: how many rows broke each rule in the latest run.
- **Task runs**: every attempt of every task, with errors. Use the Failed filter.

Open http://localhost:8000/docs and try each endpoint with "Try it out". This is the same data the dashboard shows, as JSON.

You can also look at the monitoring database directly:

```bash
docker compose exec monitoring-db psql -U monitor -d monitoring -c "SELECT task_id, status, duration_seconds FROM pipeline_runs ORDER BY id DESC LIMIT 10;"
```

**Optional dev mode for the dashboard** (needs Node.js). If you want to change the React code and see changes instantly:

```bash
cd dashboard
npm install
npm run dev
```

Open http://localhost:5173. Edit a file in `dashboard/src`, save, and the page updates. Press Ctrl+C to stop. When finished, rebuild the Docker version with `docker compose up -d --build dashboard`.

---

## Phase 5: Understand the code (Day 6 to 8)

Read the files in this order. For each, write two or three sentences in your own words in `docs/NOTES.md` about what it does. This is your interview preparation.

1. `pipeline/config.py`: where every setting and folder path comes from.
2. `pipeline/generate_events.py`: how fake events are made, and which bad data is injected (`BAD_DATA_RATES`).
3. `pipeline/validate_events.py`: the quality rules (`quality_rules()`), how rows are split into valid and quarantine, and why `checked.cache()` helps (three actions reuse the same data).
4. `pipeline/transform_events.py`: the three Spark SQL queries. Try writing one more yourself, for example revenue by country.
5. `airflow/dags/ecommerce_pipeline_dag.py`: task order, retries, and the callbacks that write to `pipeline_runs`.
6. `monitoring_db/init.sql`: the four monitoring tables.
7. `api/app/main.py`: each endpoint and its SQL.
8. `dashboard/src/App.jsx` and `components/`: how the dashboard fetches and displays data.
9. `docker-compose.yml`: which containers exist and how they connect.

Questions you should be able to answer afterwards:

- Why is bad data quarantined instead of deleted?
- What is the difference between `cache()` and writing to Parquet?
- Why does the validation task fail when too many rows are bad, instead of continuing?
- What does "partition pruning" mean?
- How does the dashboard know a dataset is stale?

Commit your notes.

---

## Phase 6: Run the tests (Day 8)

Pipeline tests (run inside the scheduler container, which has Java and Spark):

```bash
docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests -v -p no:cacheprovider
```

API tests (run inside a throwaway Python container, so you do not need Python installed):

```bash
docker run --rm -v "$(pwd)/api:/app" -w /app python:3.11-slim sh -c "pip install -q -r requirements-dev.txt && python -m pytest tests -v -p no:cacheprovider"
```

On Windows PowerShell, replace `$(pwd)` with `${PWD}`.

The project also has a GitHub Actions workflow (`.github/workflows/ci.yml`). After you push, open the **Actions** tab on your GitHub repo: the tests and the dashboard build run automatically on every push. A green check mark on your repo looks good to recruiters.

---

## Phase 7: Benchmark with 5 million events (Day 9)

This produces the runtime numbers for your resume. First give Spark more memory in `.env`:

```
SPARK_DRIVER_MEMORY=4g
```

Then:

```bash
docker compose up -d
docker compose exec airflow-scheduler python -m pipeline.generate_events --date 2026-10-06 --num-events 5000000
docker compose exec airflow-scheduler python -m pipeline.benchmark --run-date 2026-10-06
```

Generating 5M events takes about a minute and around 550 MB of disk. The benchmark runs the full validate + transform pipeline twice (untuned baseline, then optimized) and prints something like:

```
Baseline:   ... s
Optimized:  ... s
Improvement: ...%
```

It also saves the result, so the dashboard's "Runtime benchmark" panel fills in. For steadier numbers, use `--repeats 3`.

**Important:** your resume should state the numbers *you* measured, not numbers from a template. Exact times depend on your laptop, so your result might be 35% or 55% rather than 42%, and the absolute times might be a few minutes rather than 30. That is fine. Write down your numbers, your event count and your machine (for example "8-core laptop, 16 GB RAM") in the README's Performance section. If an interviewer asks "how did you measure that?", you can say: same input data, same machine, warm-up run first, averaged over N runs.

---

## Phase 8: Move storage to Amazon S3 (Day 10 to 11)

Follow [AWS_SETUP.md](AWS_SETUP.md). After switching, trigger the DAG and confirm the folders appear in your S3 bucket.

Before committing, run `git status` and make sure `.env` is **not** listed. Your AWS keys live there and must never reach GitHub.

---

## Phase 9: Polish for your resume (Day 12 to 14)

1. Take screenshots of the dashboard, the Airflow graph view and a failed task. Save them in `docs/images/` and add them to the README:

   ```markdown
   ![Dashboard](docs/images/dashboard.png)
   ```

2. Fill in the Performance section of the README with your real numbers.
3. Pin the repository on your GitHub profile (Profile → Customize your pins).
4. Update your resume bullets with your measured numbers.

Ideas to extend the project later, each a good talking point: email or Slack alerts from the failure callback, a `/api/quality/history` chart in the dashboard, a second data source, or deploying the API to AWS.

---

## Everyday commands

| What | Command |
|---|---|
| Start everything | `docker compose up -d` |
| Stop everything (keeps data) | `docker compose down` |
| Rebuild after changing a Dockerfile or requirements | `docker compose up -d --build` |
| See running containers | `docker compose ps` |
| Logs of one service | `docker compose logs -f airflow-scheduler` |
| Open a shell in the scheduler | `docker compose exec airflow-scheduler bash` |
| Wipe the databases and start fresh | `docker compose down -v` (deletes Airflow history and monitoring data) |

Changes to Python files in `pipeline/` or `airflow/dags/` take effect without a rebuild, because those folders are mounted into the containers. Changes to the API or dashboard code need `docker compose up -d --build api` or `... dashboard`.

## Troubleshooting

**`port is already allocated`**: another program uses 8080, 8000, 3000 or 5433. Stop that program, or change the left number in `docker-compose.yml` (for example `"8081:8080"`) and use the new port in your browser.

**A Spark task dies with exit code 137 or "Java heap space"**: not enough memory. Give Docker Desktop more memory, or lower `NUM_EVENTS`, or raise `SPARK_DRIVER_MEMORY` (it must stay well below Docker's limit).

**`Permission denied` on `data/` or `logs/` (Linux)**: set `AIRFLOW_UID` in `.env` to the output of `id -u`, then `docker compose down && docker compose up -d`.

**The DAG does not appear in Airflow**: wait a minute, then run `docker compose logs airflow-scheduler --tail 50` and look for an import error. A Python typo in the DAG file is the usual cause.

**The dashboard says the database is unavailable**: run `docker compose ps` and check that `monitoring-db` and `api` are running.

**You changed `monitoring_db/init.sql` but nothing happened**: that file only runs when the database is first created. Run `docker compose down -v` and then `docker compose up -d` (this deletes existing monitoring data).

**`git push` is rejected with "fetch first"**: the GitHub repo has commits you do not have locally (usually because a README was created on GitHub). Run `git pull --rebase origin main`, then `git push`.

**You committed `.env` by accident**: run `git rm --cached .env`, commit and push. If it contained AWS keys, delete those keys in the AWS console right away and create new ones, because the old ones are now in your Git history.
