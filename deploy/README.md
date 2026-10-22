# Deploying Neptunea on AWS

One CloudFormation template ([neptunea.yaml](neptunea.yaml)) creates
everything; deleting the stack removes it. No tools to install — it is
deployed from the AWS console.

## What it creates

| Piece | Purpose |
|---|---|
| EC2 server (`r7g.large`, 2 vCPU / 16 GB, ARM) | Runs the app (systemd, `localhost:5006`) behind Caddy, which serves HTTPS with a free Let's Encrypt certificate — same setup as `qaqc.ooi-rca.net` |
| Elastic IP + Route 53 record | `neptunea.ooi-rca.net` → fixed IP, in the existing `ooi-rca.net` zone |
| Security group | Ports 80/443, open by default; no SSH (admin via Session Manager) |
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
- The account has its default VPC (most do) and the `ooi-rca.net` Route 53
  zone.
- Access is open to the internet, like the QAQC dashboard: the data is
  public and the app is read-only. Let's Encrypt must reach port 80 to issue
  and renew the certificate, so narrowing `AllowedCidr` breaks HTTPS unless
  Caddy is switched to a Route 53 (DNS) challenge.

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
- Leave the parameters at their defaults
- Check *I acknowledge that AWS CloudFormation might create IAM resources* →
  **Submit**

The stack finishes in ~3 minutes; the first boot then takes ~5 more to
install Python and packages. Caddy gets the certificate once the DNS record
resolves (usually within minutes; it retries on its own).

## 3. Open it

https://neptunea.ooi-rca.net (also the stack's **Outputs** → `Url`) — share
that with coworkers.

## Day to day

| Task | How |
|---|---|
| Ship a change | Push to `main`; live at next morning start, or restart now |
| Shell on the server | EC2 → Instances → `neptunea` → **Connect** → Session Manager |
| Restart / logs | `sudo systemctl restart neptunea` / `sudo journalctl -u neptunea -f` |
| HTTPS / certificate logs | `sudo journalctl -u caddy -f` |
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
- **Off hours, the URL doesn't answer** (browsers show "can't connect").
- **Certificates live on the server's disk**, so they survive stop/start; a
  rebuilt server requests a new one (well within Let's Encrypt limits).
- **If `git pull` fails** (e.g. GitHub unreachable), the server keeps
  running the last pulled code.
- **Make changes through the template**, not by editing resources in the
  console, so the file keeps describing what is actually deployed.
