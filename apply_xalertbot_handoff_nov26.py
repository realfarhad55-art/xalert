"""apply_xalertbot_handoff_nov26.py - Main-bot handoff to the separate xalertbot.

After this patch the main bot NEVER posts a normal (non-dip) X alert itself:
the separate xalertbot process posts them. The main bot keeps owning all its
data files (Model B): xalertbot writes small JSON records to
/opt/bot/data/xalert_outbox/ and the new xalert_outbox_job (1s) applies them.

Edits (3 files, run from /opt/bot):
  x_alert.py:
    E1 - monbot_import_job: skip non-dip queue entries (X goes to xalertbot).
    E2 - add XALERT_OUTBOX_DIR / XALERT_INTAKE_FILE + xalert_outbox_job.
    E3 - pending_media_job: early return (xalertbot retries held X alerts).
  commands.py:
    E4 - /forcex: queue a "forcex" intake line instead of posting.
    E5 - /addmissedcall: queue a "postx" intake line instead of posting.
    E6 - /recheckx: queue a "postx" line per missing level instead of posting.
    E7 - /fixmc: queue a "postx" line for the initial X instead of posting.
  runtime.py:
    E8 - register xalert_outbox_job (every 1s).
    E9 - _run_fixx: queue a "postx" line per missing level instead of posting.

NOT touched: dip alerts (send_dip_alert / monbot_import_job dip branch /
pending_dip_media_job), dropped calls, video prerender, champion rank-up,
monitoring (already off via MONBOT_MODE), /testchampion, /testdip, /testalert
(test-only commands, fake data).

REQUIRES: apply_monbot_mode_nov24.py already applied (monbot_import_job must
exist in x_alert.py and be registered in runtime.py). If it is missing the
script FAILS without writing anything.

Idempotent (MARKER check). Two-phase: ALL anchors are verified count==1
BEFORE anything is written, so a failed anchor leaves every file untouched.
Backups: x_alert.py.bak_xalertbot_nov26, commands.py.bak_xalertbot_nov26,
runtime.py.bak_xalertbot_nov26 (first edit only, pristine).
Run: python3 apply_xalertbot_handoff_nov26.py [/opt/bot]
Then: systemctl restart bot.service (xalertbot must already be running).
Rollback: cp *.bak_xalertbot_nov26 back over the files + restart bot.service
+ systemctl stop xalertbot.service.

WARNING: live /opt/bot files have post-Nov-23 patches. If any anchor FAILS
on the live files, do NOT force it - report back instead. Anchors were
verified count==1 against Nov 23 base copies + the monbot-mode patch.
"""

import os
import shutil
import sys

MARKER = "XALERTBOT_HANDOFF_NOV26"
MONBOT_MARKER = "monbot_import_job"

# --- E1: monbot_import_job skips X entries (dip only) ---
OLD1 = '''            if not call or x_val <= 0:
                continue
            if typ == "dip":'''

NEW1 = '''            if not call or x_val <= 0:
                continue
            # XALERTBOT_HANDOFF_NOV26: normal X alerts are posted by the
            # separate xalertbot now - this job only imports dip entries.
            # (Dip stays here until the dipbot handoff takes it over.)
            if typ != "dip":
                continue
            if typ == "dip":'''

# --- E2: outbox constants + xalert_outbox_job (before monbot_import_job) ---
OLD2 = '''async def monbot_import_job(context):'''

NEW2 = '''# XALERTBOT_HANDOFF_NOV26: xalertbot (separate process) posts all normal X
# alerts now. It never writes this bot's data files - it writes small JSON
# records to XALERT_OUTBOX_DIR and xalert_outbox_job (below, every 1s)
# applies them. This bot stays the SOLE writer of its data files.
# XALERT_INTAKE_FILE is where /forcex, /recheckx, /fixmc, /fixx and
# /addmissedcall queue a post request for xalertbot (see commands.py).
XALERT_OUTBOX_DIR = "/opt/bot/data/xalert_outbox"
XALERT_INTAKE_FILE = "/opt/xalertbot/data/intake.jsonl"


async def xalert_outbox_job(context):
    """Apply xalertbot's bookkeeping records (1s). Batched saves: all seal /
    post / time records in one run are flushed with a single save each."""
    import glob
    import json
    try:
        files = sorted(glob.glob(os.path.join(XALERT_OUTBOX_DIR, "*.json")))
    except Exception:
        return
    if not files:
        return
    seals_changed = False
    posts_changed = False
    times_changed = False
    for path in files:
        try:
            with open(path, encoding="utf-8") as f:
                rec = json.load(f)
        except Exception:
            logger.warning(f"xalert_outbox: bad file, removing: {path}")
            try:
                os.remove(path)
            except Exception:
                pass
            continue
        if not isinstance(rec, dict):
            try:
                os.remove(path)
            except Exception:
                pass
            continue
        t = rec.get("type")
        try:
            if t == "seal":
                ck, ms = rec.get("call_key", ""), int(rec.get("ms", 0) or 0)
                if ck and ms > 0 and ms not in sent_milestones.get(ck, set()):
                    sent_milestones.setdefault(ck, set()).add(ms)
                    seals_changed = True
            elif t == "unseal":
                ck, ms = rec.get("call_key", ""), int(rec.get("ms", 0) or 0)
                if ck and ms > 0 and ms in sent_milestones.get(ck, set()):
                    sent_milestones[ck].discard(ms)
                    seals_changed = True
            elif t == "milestone_post":
                ck = rec.get("call_key", "")
                if ck and rec.get("post_id") is not None:
                    milestone_posts.setdefault(ck, {})[str(rec.get("x_val"))] = rec["post_id"]
                    posts_changed = True
            elif t == "milestone_time":
                ck = rec.get("call_key", "")
                if ck and rec.get("ms") is not None and rec.get("at"):
                    milestone_times.setdefault(ck, {})[str(rec.get("ms"))] = rec["at"]
                    times_changed = True
            elif t == "award_points":
                try:
                    await award_points_for_milestone(
                        rec.get("channel", ""), rec.get("call_key", ""),
                        int(rec.get("x_val", 0) or 0))
                except Exception as e:
                    logger.warning(f"xalert_outbox: award failed: {e}")
            elif t == "alert_posted":
                try:
                    _inc_channel_post_count()
                except Exception:
                    pass
            elif t == "hs_state":
                try:
                    cur = _hs_load()
                    if isinstance(cur, dict) and isinstance(rec.get("state"), dict):
                        cur.update(rec["state"])
                        _hs_save(cur)
                except Exception as e:
                    logger.warning(f"xalert_outbox: hs_state failed: {e}")
            elif t == "register_details":
                try:
                    m = _xalert_details_map()
                    m[rec["sid"]] = {"call_key": rec.get("call_key", ""),
                                     "x_val": rec.get("x_val")}
                    save_json(XALERT_DETAILS_FILE, m)
                except Exception as e:
                    logger.warning(f"xalert_outbox: register_details failed: {e}")
            else:
                logger.warning(f"xalert_outbox: unknown type {t!r}, removing")
        except Exception as e:
            # Leave the file: retry next run, do not lose the record.
            logger.warning(f"xalert_outbox: record failed {path}: {e}")
            continue
        try:
            os.remove(path)
        except Exception:
            pass
    try:
        if seals_changed:
            _save_milestones()
        if posts_changed:
            _save_milestone_posts()
        if times_changed:
            _save_milestone_times()
    except Exception as e:
        logger.warning(f"xalert_outbox: save failed: {e}")


async def monbot_import_job(context):'''

# --- E3: pending_media_job early return (xalertbot retries held X alerts) ---
OLD3 = '''async def pending_media_job(context):
    """Post milestones that were held back because their media was missing.
    As soon as the owner uploads media for that X level, the alert goes out
    and the channel/leaderboard/champions/trending start showing it."""
    try:
        if not pending_media_alerts:
            return'''

NEW3 = '''async def pending_media_job(context):
    """Post milestones that were held back because their media was missing.
    As soon as the owner uploads media for that X level, the alert goes out
    and the channel/leaderboard/champions/trending start showing it."""
    # XALERTBOT_HANDOFF_NOV26: held X alerts are retried by the separate
    # xalertbot now (its own pending-media loop + own pending file). This
    # job must not post X alerts any more, or the same alert posts twice.
    return
    try:
        if not pending_media_alerts:
            return'''

# --- E4: /forcex queues an intake line ---
OLD4 = '''    call = tracked_calls[call_key]
    entry_mc = float(call.get("entry_mc", 0) or 0)
    if peak_mc <= 0 and entry_mc > 0:
        peak_mc = entry_mc * x_val
    for ms in get_milestones():
        if ms <= x_val and ms not in sent_milestones[call_key]:
            sent_milestones[call_key].add(ms)
            try: await award_points_for_milestone(call.get("channel", channel), call_key, ms)
            except Exception: pass
    if entry_mc > 0:
        _update_peak(call, x_val, peak_mc)
        call["last_ratio"] = float(x_val)
    _save_milestones(); _save_tracked()
    await send_alert(
        context.bot, call.get("channel", channel), call.get("msg_id", 0), x_val,
        call.get("chain", "SOL"), call.get("entry_fmt", "N/A"),
        fmt_mc(peak_mc) if peak_mc > 0 else "N/A", ca, call.get("symbol", "UNKNOWN"))
    await update.message.reply_text(
        f"✅ <b>{x_val}X alert posted</b>\\n\\n@{channel} — {call.get('symbol','?')}\\n"
        f"Entry: {call.get('entry_fmt','N/A')} → {fmt_mc(peak_mc) if peak_mc > 0 else 'N/A'}",
        parse_mode="HTML")'''

NEW4 = '''    call = tracked_calls[call_key]
    entry_mc = float(call.get("entry_mc", 0) or 0)
    if peak_mc <= 0 and entry_mc > 0:
        peak_mc = entry_mc * x_val
    if entry_mc > 0:
        _update_peak(call, x_val, peak_mc)
        call["last_ratio"] = float(x_val)
        _save_tracked()
    # XALERTBOT_HANDOFF_NOV26: X alerts are posted by the separate xalertbot
    # now. Queue the request for it (it seals + awards points + posts);
    # this bot no longer posts X alerts itself.
    try:
        os.makedirs(os.path.dirname(XALERT_INTAKE_FILE), exist_ok=True)
        with open(XALERT_INTAKE_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps({"type": "forcex", "call_key": call_key,
                                "x_val": x_val, "peak_mc": peak_mc}) + "\\n")
    except Exception as e:
        logger.error(f"/forcex intake write failed: {e}")
        await update.message.reply_text("⚠️ Force X queue nahi hua, dobara try karein.", parse_mode="HTML"); return
    await update.message.reply_text(
        f"✅ <b>{x_val}X alert queued</b>\\n\\n@{channel} — {call.get('symbol','?')}\\n"
        f"Entry: {call.get('entry_fmt','N/A')} → {fmt_mc(peak_mc) if peak_mc > 0 else 'N/A'}\\n"
        f"xalertbot post karega (1-2 second me).",
        parse_mode="HTML")'''

# --- E5: /addmissedcall queues a "postx" line (seal + points already done) ---
OLD5 = '''    if alert_x_val >= 2:
        await send_alert(
            context.bot, channel, call_data.get("msg_id", 0), alert_x_val,
            call_data.get("chain", chain_val), call_data.get("entry_fmt", "N/A"),
            fmt_mc(manual_ath_mc), ca, call_data.get("symbol", "UNKNOWN"))'''

NEW5 = '''    if alert_x_val >= 2:
        # XALERTBOT_HANDOFF_NOV26: X alert post goes to xalertbot via the
        # intake file (seal + points already done above, so type "postx"
        # only posts this one level and skips re-awarding).
        try:
            os.makedirs(os.path.dirname(XALERT_INTAKE_FILE), exist_ok=True)
            with open(XALERT_INTAKE_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps({"type": "postx", "call_key": call_key,
                                    "x_val": alert_x_val,
                                    "peak_mc": float(manual_ath_mc)}) + "\\n")
        except Exception as e:
            logger.error(f"/addmissedcall intake write failed: {e}")'''

# --- E6: /recheckx queues a "postx" line per missing level ---
OLD6 = '''            try:
                await send_alert(
                    context.bot, call.get("channel", channel), call.get("msg_id", 0), ms,
                    call.get("chain", "SOL"), call.get("entry_fmt", "N/A"),
                    fmt_mc(ms_mc) if ms_mc > 0 else "N/A", ca, call.get("symbol", "UNKNOWN"))
                sent_milestones[call_key].add(ms)
                posted_here += 1
            except Exception as e_a:
                logger.error(f"recheckx alert: {e_a}")'''

NEW6 = '''            try:
                # XALERTBOT_HANDOFF_NOV26: queue this one level for
                # xalertbot (type "postx": it posts it; seal + points for
                # this level were already done above, so no double award).
                os.makedirs(os.path.dirname(XALERT_INTAKE_FILE), exist_ok=True)
                with open(XALERT_INTAKE_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps({"type": "postx", "call_key": call_key,
                                        "x_val": ms, "peak_mc": float(ms_mc)}) + "\\n")
                sent_milestones[call_key].add(ms)
                posted_here += 1
            except Exception as e_a:
                logger.error(f"recheckx alert: {e_a}")'''

OLD6B = '''        lines.append(f"• {call.get('symbol','?')} — {fmt_x(ratio)} → 📣 "
                      f"{', '.join(str(m)+'X' for m in missing)} posted")'''

NEW6B = '''        lines.append(f"• {call.get('symbol','?')} — {fmt_x(ratio)} → 📣 "
                      f"{', '.join(str(m)+'X' for m in missing)} queued (xalertbot)")'''

# --- E7: /fixmc queues a "postx" line for the initial X ---
OLD7 = '''    initial_x = max(milestones_for_ratio(ratio)) if milestones_for_ratio(ratio) else 0
    initial_posted = False
    if initial_x >= 2 and initial_x not in sent_milestones[call_key]:
        initial_posted = bool(await send_alert(
            context.bot, call.get("channel", channel), call.get("msg_id", 0), initial_x,
            call.get("chain", "SOL"), call.get("entry_fmt", "N/A"), fmt_mc(owner_ath_mc),
            call.get("ca", ca), call.get("symbol", "UNKNOWN")))
        if initial_posted:
            sent_milestones[call_key].add(initial_x); _save_milestones()'''

NEW7 = '''    initial_x = max(milestones_for_ratio(ratio)) if milestones_for_ratio(ratio) else 0
    initial_posted = False
    if initial_x >= 2 and initial_x not in sent_milestones[call_key]:
        # XALERTBOT_HANDOFF_NOV26: queue this one level for xalertbot
        # (type "postx": it seals + awards + posts it).
        try:
            os.makedirs(os.path.dirname(XALERT_INTAKE_FILE), exist_ok=True)
            with open(XALERT_INTAKE_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps({"type": "postx", "call_key": call_key,
                                    "x_val": initial_x,
                                    "peak_mc": float(owner_ath_mc)}) + "\\n")
            initial_posted = "queued"
        except Exception as e:
            logger.error(f"/fixmc intake write failed: {e}")'''

OLD7B = '''        f"({'posted' if initial_posted else 'already recorded / failed'})\\n"'''

NEW7B = '''        f"({'queued for xalertbot' if initial_posted == 'queued' else ('posted' if initial_posted else 'already recorded / failed')})\\n"'''

# --- E8: register xalert_outbox_job in runtime.py ---
OLD8 = '''    app.job_queue.run_repeating(monbot_import_job, interval=1, first=5)  # MONBOT_MODE_NOV24: monbot ki milestones import karo + X alert post karo (1s: local file read cheap hai, X alert fast rehta hai)'''

NEW8 = '''    app.job_queue.run_repeating(monbot_import_job, interval=1, first=5)  # MONBOT_MODE_NOV24: monbot ki milestones import karo + X alert post karo (1s: local file read cheap hai, X alert fast rehta hai)
    app.job_queue.run_repeating(xalert_outbox_job, interval=1, first=5)  # XALERTBOT_HANDOFF_NOV26: xalertbot ke outbox records apply karo (seals/posts/points, 1s)'''

# --- E9: _run_fixx queues a "postx" line per missing level ---
OLD9 = '''    for ms in targets:
        res = await send_alert(
            context.bot, call.get("channel"), call.get("msg_id", 0), ms,
            call.get("chain", "SOL"), call.get("entry_fmt", "N/A"), cur_fmt,
            call.get("ca", ""), call.get("symbol", "UNKNOWN"))
        if res:
            # "silent" = milestone recorded but not posted (no media for that X)
            if res != "silent":
                posted.append(ms)
            else:
                silent_ms.append(ms)
            sent_milestones[call_key].add(ms)
            await award_points_for_milestone(call.get("channel"), call_key, ms)
        await asyncio.sleep(1.5)
    _save_milestones()'''

NEW9 = '''    for ms in targets:
        # XALERTBOT_HANDOFF_NOV26: queue this one level for xalertbot
        # (type "postx": it seals + awards + posts it). No sleep needed -
        # xalertbot paces posts itself.
        try:
            os.makedirs(os.path.dirname(XALERT_INTAKE_FILE), exist_ok=True)
            with open(XALERT_INTAKE_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps({"type": "postx", "call_key": call_key,
                                    "x_val": ms, "peak_mc": float(peak_mc)}) + "\\n")
            posted.append(ms)
        except Exception as e:
            logger.error(f"fixx intake write failed {ms}X: {e}")'''

OLD9B = '''        f"✅ <b>X Update Posted</b>\\n\\n@{html.escape(channel)} — "'''

NEW9B = '''        f"✅ <b>X Update Queued</b>\\n\\n@{html.escape(channel)} — "'''

OLD9C = '''        f"Posted: <b>{', '.join(str(m) + 'X' for m in posted) or 'none'}</b>"'''

NEW9C = '''        f"Queued for xalertbot: <b>{', '.join(str(m) + 'X' for m in posted) or 'none'}</b>"'''

EDITS = [
    ("x_alert.py", OLD1, NEW1, "E1"),
    ("x_alert.py", OLD2, NEW2, "E2"),
    ("x_alert.py", OLD3, NEW3, "E3"),
    ("commands.py", OLD4, NEW4, "E4"),
    ("commands.py", OLD5, NEW5, "E5"),
    ("commands.py", OLD6, NEW6, "E6"),
    ("commands.py", OLD6B, NEW6B, "E6b"),
    ("commands.py", OLD7, NEW7, "E7"),
    ("commands.py", OLD7B, NEW7B, "E7b"),
    ("runtime.py", OLD8, NEW8, "E8"),
    ("runtime.py", OLD9, NEW9, "E9"),
    ("runtime.py", OLD9B, NEW9B, "E9b"),
    ("runtime.py", OLD9C, NEW9C, "E9c"),
]


def main():
    d = sys.argv[1] if len(sys.argv) > 1 else "."
    paths = {name: os.path.join(d, name)
             for name in ("x_alert.py", "commands.py", "runtime.py")}
    for name, p in paths.items():
        if not os.path.exists(p):
            print("FAIL: %s not found in %s" % (name, d))
            return 1
    srcs = {}
    for name, p in paths.items():
        with open(p, encoding="utf-8") as f:
            srcs[name] = f.read()
    if all(MARKER in srcs[n] for n in paths):
        print("OK: already has XALERTBOT_HANDOFF_NOV26, skipping.")
        return 0
    if any(MARKER in srcs[n] for n in paths):
        print("FAIL: partial state (marker in some files only) -- inspect "
              "x_alert.py / commands.py / runtime.py before re-running.")
        return 1
    # E1/E2/E8 need the monbot patch already applied.
    if MONBOT_MARKER not in srcs["x_alert.py"]:
        print("FAIL: monbot_import_job not in x_alert.py -- apply "
              "apply_monbot_mode_nov24.py FIRST, then re-run this patch.")
        return 1
    if MONBOT_MARKER not in srcs["runtime.py"]:
        print("FAIL: monbot_import_job not registered in runtime.py -- apply "
              "apply_monbot_mode_nov24.py FIRST, then re-run this patch.")
        return 1
    # Phase 1: verify EVERY anchor count==1 before writing anything.
    for fname, old, _new, tag in EDITS:
        n = srcs[fname].count(old)
        if n != 1:
            print("FAIL: %s anchor missing/dup in %s (found %dx) -- NOT "
                  "writing anything." % (tag, fname, n))
            return 1
    # Phase 2: apply (one pristine .bak per file, first edit only).
    for fname, old, new, tag in EDITS:
        p = paths[fname]
        bak = p + ".bak_xalertbot_nov26"
        if not os.path.exists(bak):
            shutil.copy2(p, bak)
        with open(p, encoding="utf-8") as f:
            src = f.read()
        assert src.count(old) == 1, tag
        with open(p, "w", encoding="utf-8") as f:
            f.write(src.replace(old, new, 1))
        print("%s OK (%s)" % (tag, fname))
    print("DONE. Backups: *.bak_xalertbot_nov26")
    print("Next: systemctl restart bot.service (xalertbot must be running).")
    print("Verify: a real X alert posts with video + premium emojis from "
          "xalertbot (journalctl -u xalertbot), and the main bot's log shows "
          "no monbot_import X posts and xalert_outbox_job applying seals.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
