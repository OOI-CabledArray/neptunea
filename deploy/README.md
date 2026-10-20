# Deploying Neptunea on AWS

One CloudFormation template ([neptunea.yaml](neptunea.yaml)) creates
everything; deleting the stack removes it. No tools to install — it is
deployed from the AWS console.

## What it creates

| Piece | Purpose |
|---|---|
| EC2 server (`r7g.large`, 2 vCPU / 16 GB, ARM) | Runs the app as a systemd service on port 80 |
| Elastic IP | Fixed URL across stop/start |
| Security group | HTTP from one IP range only; no SSH (admin via Session Manager) |
| Two EventBridge schedules | Start 7am / stop 7pm Pacific, weekdays |
| IAM roles | Server: read data buckets + `/neptunea/*` settings. Scheduler: start/stop this server |

Rough on-demand cost in us-west-2: ~$35/month on the weekday schedule
(~$28 compute, ~$4 IP, ~$2 disk) vs ~$85/month running 24/7.

Every time the service starts (each morning, or on restart) it pulls the
latest commit on the branch, installs the exact versions in `uv.lock`, and
launches. Pushing to `main` is the deploy step.

## Before you deploy

- `uv.lock` and `.python-version` are committed and pushed — the server
  refuses to start without a lockfile matching `pyproject.toml`.
- Use region **us-west-2** (Oregon), where the data buckets live.
- The account has its default VPC (most do).
- Pick the IP range coworkers will connect from (campus/VPN CIDR). The app
  has no login and plain HTTP, so keep this narrow; off-campus users go
  through the VPN.

## 1. Store settings in Parameter Store (one time)

Console → **Systems Manager → Parameter Store → Create parameter**, tier
Standard, in us-west-2:

| Name | Type | Value |
|---|---|---|
| `/neptunea/env/OOI_USERNAME` | String | OOI API username (annotations/deployments) |
| `/neptunea/env/OOI_TOKEN` | SecureString | OOI API token |

Anything under `/neptunea/env/` becomes an environment variable for the
app. These live outside the stack, so they survive a delete/redeploy.

## 2. Create the stack

Console → **CloudFormation → Create stack → With new resources** → *Upload a
template file* → `deploy/neptunea.yaml`.

- Stack name: `neptunea`
- `AllowedCidr`: the range from above; leave the rest at defaults
- Check *I acknowledge that AWS CloudFormation might create IAM resources* →
  **Submit**

The stack finishes in ~3 minutes; the first boot then takes ~5 more to
install Python and packages.

## 3. Open it

The stack's **Outputs** tab has `Url` (`http://<ip>/neptunea`) — share
that with coworkers.

## Day to day

| Task | How |
|---|---|
| Ship a change | Push to `main`; live at next morning start, or restart now |
| Shell on the server | EC2 → Instances → `neptunea` → **Connect** → Session Manager |
| Restart / logs | `sudo systemctl restart neptunea` / `sudo journalctl -u neptunea -f` |
| First-boot log | `/var/log/cloud-init-output.log` |
| Use it after hours | EC2 → Instances → `neptunea` → Instance state → **Start** (still stops at 7pm) |
| Change hours / run 24/7 | CloudFormation → stack → **Update** → *Use existing template* → edit `StartCron`/`StopCron`, or set `ScheduleState` to `DISABLED` |
| Change dependencies | Edit `pyproject.toml`, run `uv lock`, test with `uv run neptunea`, commit both |
| Upgrade packages | `uv lock --upgrade`, test, commit `uv.lock` |
| Tear down | CloudFormation → stack → **Delete** (Parameter Store entries stay) |

## Good to know

- **A stack update may rebuild the server.** The template looks up the
  latest Amazon Linux image on each update; if it changed, CloudFormation
  replaces the instance. The URL is unchanged and the new server is
  freshly patched, but in-memory caches start cold.
- **Caches reset on every stop/start**, so the first large stream view of
  the day re-scans S3.
- **If `git pull` fails** (e.g. GitHub unreachable), the server keeps
  running the last pulled code.
- **Make changes through the template**, not by editing resources in the
  console, so the file keeps describing what is actually deployed.
