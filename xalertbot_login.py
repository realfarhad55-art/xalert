"""xalertbot_login.py - one-time login for xalertbot's OWN third account.

Run INTERACTIVELY on the VPS (once):
    cd /opt/xalertbot && venv/bin/python xalertbot_login.py

It asks for the phone number, then the SMS / Telegram login code (then the
2FA password if the account has one), and saves the session string to
userbot_session.txt in this folder (xalertbot reads it automatically).
It also prints the XALERTBOT_SESSION= line for .env.

The account used here must be a THIRD Telegram account - NOT @yowziuh (the
main bot's userbot) and NOT @Wizard_Scan (the dropbot's userbot). A Telegram
userbot session allows only ONE live connection: logging in here with one of
those two accounts would disconnect the main bot / dropbot.

API_ID / API_HASH are read from .env (reuse the main bot's values from
/opt/bot/.env - they identify the APPLICATION, not the account).
"""

import os
import sys


def _load_dotenv(path):
    d = {}
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                d[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return d


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    env = _load_dotenv(os.path.join(here, ".env"))
    try:
        api_id = int(env.get("XALERTBOT_API_ID", "0") or 0)
    except ValueError:
        api_id = 0
    api_hash = env.get("XALERTBOT_API_HASH", "")
    if not api_id or not api_hash:
        print("FAIL: XALERTBOT_API_ID / XALERTBOT_API_HASH missing in .env")
        print("Reuse the values from /opt/bot/.env (they identify the app, "
              "not the account).")
        return 1
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    print("Logging in a THIRD Telegram account for xalertbot.")
    print("Use a NEW account - NOT @yowziuh (main bot) and NOT @Wizard_Scan "
          "(dropbot).")
    phone = input("Phone number (with country code, e.g. +92...): ").strip()
    if not phone:
        print("FAIL: no phone number given.")
        return 1
    client = TelegramClient(StringSession(), api_id, api_hash)
    # start() asks for the login code (and 2FA password if set).
    client.start(phone=phone)
    me = client.get_me()
    print(f"Logged in as @{me.username or me.id} (id {me.id})")
    session_str = client.session.save()
    out = os.path.join(here, "userbot_session.txt")
    with open(out, "w", encoding="utf-8") as f:
        f.write(session_str)
    print(f"Session saved to {out} (xalertbot reads it automatically).")
    print("Optional .env line (same value):")
    print(f"XALERTBOT_SESSION={session_str}")
    print("Next: make this account admin in @WizardScan (Post messages).")
    print("For premium emojis on video captions, this account needs "
          "Telegram Premium.")
    client.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
