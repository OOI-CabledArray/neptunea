# Deploying Neptunea on AWS

One CloudFormation template ([neptunea.yaml](neptunea.yaml)) creates
everything; deleting the stack removes it.

| Piece | Purpose |
|---|---|
| EC2 server (`r7g.large`, 2 vCPU / 16 GB, ARM) | Runs the app (systemd, `localhost:5006`) behind Caddy, which serves HTTPS with a free Let's Encrypt certificate — same setup as `qaqc.ooi-rca.net` |
| Elastic IP + Route 53 record | `neptunea.ooi-rca.net` → fixed IP, in the existing `ooi-rca.net` zone |
| Security group | Ports 80/443, open to the internet; no SSH (admin via Session Manager) |
| Two EventBridge schedules | Start 7am / stop 7pm Pacific, weekdays |
| IAM roles | Server: read data buckets + `/neptunea/*` settings. Scheduler: start/stop this server |

~$35/month on the weekday schedule (~$28 compute, ~$4 IP, ~$2 disk) vs
~$85/month 24/7. Each app start (every morning, or on restart) pulls `main`,
installs the exact versions in `uv.lock`, and launches — **pushing to `main`
is the deploy step.**

## First deploy

Region **us-west-2** (where the data buckets live); the account needs its
default VPC and the `ooi-rca.net` Route 53 zone.

1. **Settings** — Systems Manager → Parameter Store → Create parameter:

   | Name | Type | Value |
   |---|---|---|
   | `/neptunea/env/OOI_USERNAME` | String | OOI API username |
   | `/neptunea/env/OOI_TOKEN` | SecureString | OOI API token |

   Anything under `/neptunea/env/` becomes an app environment variable; these
   live outside the stack, so they survive a delete/redeploy.
2. **Stack** — CloudFormation → Create stack → *Upload a template file* →
   `deploy/neptunea.yaml`. Stack name `neptunea`, default parameters, check
   *I acknowledge … IAM resources* → **Submit**.
3. **Wait ~10 min** (stack ~3, first boot ~5, then Caddy gets the
   certificate once DNS resolves), then open https://neptunea.ooi-rca.net.

## Day to day

| Task | How |
|---|---|
| Ship a change | Push to `main`; live at next morning start, or [restart now](#restart-the-app-from-your-laptop) |
| Shell on the server | EC2 → Instances → `neptunea` → **Connect** → Session Manager |
| Restart / logs | `sudo systemctl restart neptunea` / `sudo journalctl -u neptunea -f` |
| HTTPS / certificate logs | `sudo journalctl -u caddy -f` |
| First-boot log | `/var/log/cloud-init-output.log` |
| Use it after hours | EC2 → Instances → `neptunea` → Instance state → **Start** (still stops at 7pm) |
| Change hours / run 24/7 | CloudFormation → stack → **Update** → *Use existing template* → edit `StartCron`/`StopCron`, or set `ScheduleState` to `DISABLED` |
| Change the template | Push, then [update the stack](#update-the-stack-after-a-template-change) |
| Tear down | CloudFormation → stack → **Delete** (Parameter Store entries stay) |

### Restart the app from your laptop

Pulls `main` and restarts (server must be running). Drops open sessions and
clears caches; back in ~30 s.

```bash
aws ssm send-command --region us-west-2 --document-name AWS-RunShellScript \
  --targets Key=tag:Name,Values=neptunea --parameters 'commands=["systemctl restart neptunea"]'
```

### Update the stack after a template change

From an up-to-date checkout; keeps existing parameter values. Each update
looks up the latest Amazon Linux image, and if it changed the server is
replaced (same URL, freshly patched, ~10 min down, cold caches) — run it off
hours.

```bash
aws cloudformation deploy --region us-west-2 --stack-name neptunea \
  --template-file deploy/neptunea.yaml --capabilities CAPABILITY_IAM
```

## Good to know

- **Make changes through the template**, not the console, so the file keeps
  describing what is deployed.
- **Access is open**, like the QAQC dashboard (public data, read-only app).
  Let's Encrypt must reach port 80, so narrowing `AllowedCidr` breaks HTTPS
  unless Caddy switches to a Route 53 (DNS) challenge.
- **Off hours the URL doesn't answer**; caches reset on every stop/start, so
  the first large stream view of the day re-scans S3.
- **Certificates live on the server's disk** and survive stop/start; a
  rebuilt server requests a new one.
- **If `git pull` fails**, the server runs the last pulled code.
