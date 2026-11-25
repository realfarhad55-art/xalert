# xalertbot — deploy doc

X-alert posting moves off the overloaded main bot into its own process.
Monbot still detects milestones; xalertbot reads monbot's queue, re-checks
the GUARD live, seals (via the main bot), and posts with video + premium
emojis + forwards/DMs exactly like the main bot used to.

## The session decision (read this first)

X alerts carry a video, and the caption needs premium custom emojis. Proven
fact: the Bot API **strips** custom-emoji entities from media captions; only
the userbot (MTProto) renders them. But a Telegram userbot session allows
**only ONE live connection**:

- `@yowziuh` is held by the **main bot** (dip / leaderboard / Most Shared /
  Binance — all userbot-sent for premium emojis).
- `@Wizard_Scan` is held by the **dropbot** (exclusively).

So xalertbot gets **its OWN third userbot account/session**. Sharing
`@yowziuh` between two processes would disconnect the main bot — that is
exactly what broke things on Nov 24.

**What the third account needs (user steps, no way around them):**
1. A third Telegram account (a spare number / account — NOT `@yowziuh`, NOT
   `@Wizard_Scan`).
2. Admin in `@WizardScan` with **Post messages** permission (a userbot can
   only post in a channel it admins).
3. **Telegram Premium on that account** for premium emojis on video
   captions. Without Premium, alerts still post with video but the caption
   falls back to plain emojis (same fallback as Most Shared after Dec 19).

**Fallback if there is no third account yet:** leave `XALERTBOT_SESSION`
empty. xalertbot still posts via the Bot API (video + caption, no premium
emojis on the caption; text-only alerts are never posted — the hold still
applies when no media is set). Set the session up later and restart.

**Deploy order with the other two split bots:** videobot first, then
xalertbot, then dipbot. All three patch `x_alert.py` in different regions,
but the dipbot handoff may touch `monbot_import_job`'s dip branch — if the
dipbot handoff is already applied and the xalertbot handoff's E1 anchor
fails, STOP and report back (do not force it).

## Files (upload to the new repo `realfarhad55-art/xalertbot`)

| File | What it is |
|---|---|
| `xalertbot.py` (262,297 B) | The bot. Posting pipeline extracted verbatim from the Nov 23 main-bot base; xalertbot-specific parts marked `XALERTBOT`. |
| `requirements.txt` | `python-telegram-bot>=21,<23`, `telethon>=1.36`, `requests>=2.28` (same pins as the dropbot). |
| `xalertbot.service` | systemd unit. |
| `xalertbot_login.py` | One-time interactive login for the third account. |
| `xalertbot.env.example` | Copy to `.env` and fill in. |
| `apply_xalertbot_handoff_nov26.py` | Main-bot patch — goes to `/opt/bot`, NOT this repo. |
| `build_xalertbot.py`, `xalertbot_test.py` | Builder + tests (dev only, do not upload to the VPS). |

## Step 1 — GitHub repo (user, web UI)

Create repo `realfarhad55-art/xalertbot` (public or private, your choice),
upload `xalertbot.py`, `requirements.txt`, `xalertbot.service`,
`xalertbot_login.py`, `xalertbot.env.example`.

## Step 2 — VPS folder + venv (one-time)

Paste each block as ONE line in Termius (multi-line pastes collapse):

`mkdir -p /opt/xalertbot/data /opt/bot/data/xalert_outbox && cd /opt/xalertbot && python3 -m venv venv && venv/bin/pip install -r requirements.txt`

Then download the repo files into `/opt/xalertbot` (GitHub web UI / `git clone`
— whichever you normally use), so the folder has `xalertbot.py`,
`xalertbot_login.py`, `xalertbot.service`, `requirements.txt`.

## Step 3 — `.env`

`cp /opt/xalertbot/xalertbot.env.example /opt/xalertbot/.env && nano /opt/xalertbot/.env`

Fill in (values from `/opt/bot/.env` — same token, same API ID/hash):

- `XALERTBOT_TOKEN` = the `@WIZARD_SCAN_BOT` token (same as the main bot's
  `BOT_TOKEN` / token value in `/opt/bot/.env`).
- `XALERTBOT_API_ID` / `XALERTBOT_API_HASH` = same values as `/opt/bot/.env`
  (they identify the app, not the account).
- `XALERTBOT_SESSION` = leave empty for now (Step 4 fills it in).

## Step 4 — Third account session (interactive, once)

`cd /opt/xalertbot && venv/bin/python xalertbot_login.py`

Type the third account's phone number, then the login code Telegram sends
(then the 2FA password if that account has one). It saves the session to
`userbot_session.txt` and prints the `XALERTBOT_SESSION=` line.

Then, in Telegram: make that third account **admin in `@WizardScan`** with
**Post messages** permission. (Without this the userbot cannot post the
video.) For premium emojis on captions, that account needs Telegram Premium.

If you skip this step, xalertbot runs in Bot API fallback mode (see above).

## Step 5 — Service (do NOT start it yet)

`cp /opt/xalertbot/xalertbot.service /etc/systemd/system/xalertbot.service && systemctl daemon-reload && systemctl enable xalertbot.service`

Do NOT start it yet — the main-bot handoff goes first (Step 6), otherwise
both bots post the same X alert for ~1 minute (duplicate risk).

## Step 6 — Main-bot handoff patch + restart

Upload `apply_xalertbot_handoff_nov26.py` to `/opt/bot`, then:

`cd /opt/bot && python3 apply_xalertbot_handoff_nov26.py /opt/bot`

Expect all 13 edits `E1`–`E9c OK` and `DONE`. If ANY line says `FAIL`,
**stop** — nothing was written (the script verifies all anchors before
writing). Report the FAIL line back instead of forcing it.

What the patch does: `monbot_import_job` stops posting X (dip only);
`pending_media_job` stops retrying held X alerts; `/forcex`, `/recheckx`,
`/fixmc`, `/fixx`, `/addmissedcall` queue an intake line for xalertbot
instead of posting; new `xalert_outbox_job` (1s) applies xalertbot's
seal/post/points records. Dip, dropped calls, video prerender, champion
rank-up, monitoring (already off) are NOT touched.

Then restart the main bot (X alerts pause for ~1 minute — monbot keeps
queueing, xalertbot picks the backlog up when it starts):

`systemctl restart bot.service`

## Step 7 — Start xalertbot

`systemctl start xalertbot.service && sleep 5 && journalctl -u xalertbot.service -n 30 --no-pager`

Expect `xalertbot LIVE - X-alert posting off the main bot` and either
`xalertbot userbot connected: @<third account>` or (no session) the
`running WITHOUT a userbot session` warning.

## Step 8 — Verify (real alert)

1. Wait for a real X alert (or ask an owner to `/forcex` a small test — it
   queues for xalertbot and posts within ~2s).
2. `journalctl -u xalertbot.service -n 50 --no-pager | grep -i "posted\|seal"` —
   expect `xalert: posted 2X <symbol> @<channel>`.
3. The `@WizardScan` post has the **video + premium emojis** (not plain).
4. Main bot log shows the outbox job working and NO X posts from it:
   `journalctl -u bot.service -n 100 --no-pager | grep -i "monbot_import.*posted\|xalert_outbox"` —
   expect no `monbot_import: posted` lines any more.
5. `ls /opt/bot/data/xalert_outbox/` — should be empty most of the time
   (records are applied + removed within ~1s).

## Rollback (if anything is wrong)

`systemctl stop xalertbot.service && cd /opt/bot && cp x_alert.py.bak_xalertbot_nov26 x_alert.py && cp commands.py.bak_xalertbot_nov26 commands.py && cp runtime.py.bak_xalertbot_nov26 runtime.py && systemctl restart bot.service`

This restores the main bot to posting X alerts itself. xalertbot stays
stopped. (The `.bak_xalertbot_nov26` files are the pristine pre-patch copies
taken by the patch script.)

## Troubleshooting

| Symptom | Check / fix |
|---|---|
| `xalertbot LIVE` never appears / service fails | `journalctl -u xalertbot.service -n 50 --no-pager` — usually `.env` path or a missing dep: `venv/bin/pip install -r requirements.txt` again. |
| `seal ack timeout ... retry next pass` every second | The main bot's `xalert_outbox_job` is not running: check the handoff patch applied (`grep -c XALERTBOT_HANDOFF_NOV26 /opt/bot/x_alert.py` should be ≥ 3) and the main bot restarted after it. |
| Alerts post but captions have NO premium emojis | No userbot session, or the third account has no Premium, or it is not admin in `@WizardScan`. Check `journalctl -u xalertbot.service | grep userbot`. |
| Duplicate X alerts (main bot + xalertbot both post) | The handoff patch did not apply (Step 6) or the main bot was not restarted after it. Stop xalertbot, verify the patch, restart the main bot, then start xalertbot. |
| `/forcex` replies "queue nahi hua" | `/opt/xalertbot/data` is not writable by the main bot's user. `mkdir -p /opt/xalertbot/data && chmod 777 /opt/xalertbot/data` (or chown to the bot user). |
| `xalert_outbox` grows, seals never apply | Main bot down or the outbox job crashed — check `journalctl -u bot.service | grep -i xalert_outbox`. Records are kept (not lost); they apply when the job runs again. |
| Details button says "no longer available" in the first ~1s | Expected gap: the Details registration goes through the outbox (≤1s). Tapping again after a second works. |

## What this does NOT cover (known gaps)

- **`/fixoldcall`** (skipcall.py, live-only file, not in the Nov 23 base):
  it posts an X alert from the main bot after fixing a record. It was NOT
  redirected — the live file is needed to patch it. If you use `/fixoldcall`,
  tell Muse so it can be redirected the same way (intake `postx` line).
- **`/testchampion`** posts a test champion video directly from the main
  bot (bypasses `send_alert`). It is an owner-only test command, not
  automatic posting — left alone; the videobot handoff owns
  `_champion_video_try_send`.
- **`/testalert`** posts a fake test alert to `TestChannel` — left alone
  (test-only, fake data).
- **Details deep links** depend on the main bot's `_xalert_details_map()`
  re-reading `xalert_details.json` per `/start` (live-only function, not in
  the Nov 23 base). If a Details tap says "no longer available" even after
  a few seconds, that map is cached in memory and the handoff needs one
  extra line in the outbox job — report it back.
- **Live anchors / live queue format / live session behaviour** were NOT
  verified (no SSH). The patch was tested on Nov 23 bases + the monbot
  patch; the live files have later patches. If an anchor fails on live,
  the script refuses to write — report the FAIL line back.
