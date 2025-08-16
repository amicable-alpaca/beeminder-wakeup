# Wake & Focus Beeminder Sync

This project keeps a Beeminder goal called `wakeandfocus` in sync with early-morning Focusmate sessions. A local SQLite database acts as the Source of Truth (SoT) for whether each day contained a qualifying session (≥50 minutes starting by 09:15 America/New_York). The script reconciles the SoT with Beeminder, ensuring exactly one datapoint per day with the correct value and comment.

## Repository structure
- `scripts/wake_focus_sync.py` – main script that builds the SoT from Focusmate sessions and reconciles it with Beeminder.
- `data/wake_focus_sot.db` – SQLite database storing SoT records.
- `.github/workflows/wake-and-focus.yml` – GitHub Actions workflow that runs the sync daily and commits the updated database.
- `requirements.txt` – Python dependencies (only `requests`).

## Requirements
- Python 3.11+
- A Beeminder account with `wakeandfocus` and `focusmate` goals
- (Optional) virtual environment tooling such as `venv`

Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration
The script reads configuration from environment variables:

| Variable | Description |
| --- | --- |
| `BM_USERNAME` | Beeminder username (default `zarathustra`) |
| `BM_AUTH_TOKEN` | Personal auth token (required unless `DRY_RUN=1`) |
| `DRY_RUN` | `1` to avoid API writes |
| `DEBUG` | `1` for verbose logging |
| `FULL_HISTORY` | `1` to reconcile from earliest Focusmate datapoint |
| `HISTORY_DAYS` | Number of days to reconcile when `FULL_HISTORY=0` (default `90`) |
| `STRICT_PURGE` | `1` to delete `wakeandfocus` datapoints for days missing from the SoT |

## Usage
Run the sync locally in dry-run mode to see what would happen without touching Beeminder:

```bash
DRY_RUN=1 python scripts/wake_focus_sync.py
```

For a real run, supply your username and auth token:

```bash
BM_USERNAME=yourname BM_AUTH_TOKEN=token python scripts/wake_focus_sync.py
```

## Automation
The included GitHub Actions workflow (`.github/workflows/wake-and-focus.yml`) executes the sync every day around 11:00 AM America/New_York. After running, any changes to the SoT database are committed back to the repository for auditing.

