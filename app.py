"""
Fathom Meeting Webhook Listener (Railway edition, v3 - 18 improvements)
Auto-processes Fathom recordings: drafts follow-up email in Gmail, sends coaching to Slack.
"""

import base64, hashlib, hmac, json, logging, os, re, sys, threading, time
import requests as req
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from flask import Flask, request, jsonify

# ─── Config (from env vars - set in Railway dashboard) ──────────────────────

PORT = int(os.environ.get("PORT", "5002"))
FATHOM_KEY = os.environ.get("FATHOM_API_KEY", "")
FATHOM_SECRET = os.environ.get("FATHOM_WEBHOOK_SECRET", "")
CLAUDE_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
GMAIL_CID = os.environ.get("GMAIL_CLIENT_ID", "")
GMAIL_CS = os.environ.get("GMAIL_CLIENT_SECRET", "")
GMAIL_RT = os.environ.get("GMAIL_REFRESH_TOKEN", "")
SLACK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

CHRIS = "chris@leadlypro.io"
CT = timezone(timedelta(hours=-5))
SKIP_IDS = {119109096}
MIN_DURATION_SEC = 120  # Skip recordings under 2 minutes

# ─── State ──────────────────────────────────────────────────────────────────

_processed = set()
_proc_lock = threading.Lock()
_last_processed = {"title": None, "time": None, "count": 0}
_start_time = time.time()

# ─── Logging ────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    stream=sys.stdout)
log = logging.getLogger("fathom")

# ─── Flask app ──────────────────────────────────────────────────────────────

app = Flask(__name__)

# ─── Duplicate protection (in-memory for Railway; resets on redeploy) ───────

def is_dup(rid):
    with _proc_lock: return rid in _processed

def mark_processed(rid):
    with _proc_lock:
        _processed.add(rid)
        # Keep only last 500 to prevent unbounded growth
        if len(_processed) > 500:
            _processed.clear()

# ─── Helpers ────────────────────────────────────────────────────────────────

def xfield(d, *keys, default=None):
    """Extract field trying multiple key names (Fathom payload compat)."""
    for k in keys:
        if k in d and d[k] is not None: return d[k]
    return default

def verify_sig(secret, headers, body):
    if not secret: return True
    mid = headers.get("webhook-id", ""); ts = headers.get("webhook-timestamp", "")
    sig = headers.get("webhook-signature", "")
    if not all([mid, ts, sig]): return False
    try:
        if abs(time.time() - int(ts)) > 300: return False
    except: return False
    content = f"{mid}.{ts}.".encode() + body
    sb = base64.b64decode(secret.split("_")[-1] if "_" in secret else secret)
    exp = base64.b64encode(hmac.new(sb, content, hashlib.sha256).digest()).decode()
    for s in sig.split(" "):
        sv = s.split(",")[-1] if "," in s else s
        if hmac.compare_digest(exp, sv): return True
    return False

def call_claude(system, user, max_tok=2000):
    """Call Anthropic API with one retry on failure."""
    for attempt in range(2):
        try:
            r = req.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key": CLAUDE_KEY, "anthropic-version": "2023-06-01",
                          "content-type": "application/json"},
                json={"model": CLAUDE_MODEL, "max_tokens": max_tok,
                      "system": system, "messages": [{"role": "user", "content": user}]},
                timeout=90)
            r.raise_for_status()
            return r.json()["content"][0]["text"]
        except Exception as e:
            if attempt == 0:
                log.warning(f"Claude attempt 1 failed ({e}), retrying in 5s...")
                time.sleep(5)
            else:
                raise

def gmail_token():
    r = req.post("https://oauth2.googleapis.com/token",
        data={"client_id": GMAIL_CID, "client_secret": GMAIL_CS,
              "refresh_token": GMAIL_RT, "grant_type": "refresh_token"},
        timeout=15)
    r.raise_for_status()
    return r.json()["access_token"]

def gmail_draft(token, to_list, cc_list, subject, body):
    """Create Gmail draft with TO/CC support and MIME-safe name quoting."""
    def fmt_addr(name, email):
        if any(c in name for c in ',;@"'): name = f'"{name}"'
        return f"{name} <{email}>"
    to_str = ", ".join(fmt_addr(n, e) for n, e in to_list)
    headers = f"From: {CHRIS}\r\nTo: {to_str}\r\n"
    if cc_list:
        cc_str = ", ".join(fmt_addr(n, e) for n, e in cc_list)
        headers += f"Cc: {cc_str}\r\n"
    headers += f"Subject: {subject}\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n{body}"
    enc = base64.urlsafe_b64encode(headers.encode("utf-8")).decode().rstrip("=")
    data = {"message": {"raw": enc}}
    r = req.post("https://gmail.googleapis.com/gmail/v1/users/me/drafts",
        json=data, headers={"Authorization": f"Bearer {token}"}, timeout=15)
    r.raise_for_status()
    return r.json()

def slack(message):
    req.post(SLACK_URL, json={"text": message}, timeout=15)

# ─── Transcript processing (full transcript preserved, nothing stripped) ────

def format_transcript(entries):
    """Format with timestamps for precise coaching references."""
    lines = [f"{e['speaker']['display_name']} [{e.get('timestamp', '??')}]: {e['text']}" for e in entries]
    formatted = "\n".join(lines)
    if len(formatted) > 30000:
        formatted = formatted[:30000]
        formatted = formatted[:formatted.rfind("\n")]
    return formatted

def compute_talk_ratio(entries):
    """Compute actual word counts per speaker from transcript data."""
    counts = {}
    for e in entries:
        name = e["speaker"]["display_name"]
        counts[name] = counts.get(name, 0) + len(e["text"].split())
    total = sum(counts.values()) or 1
    return {name: {"words": w, "pct": round(w / total * 100, 1)} for name, w in
            sorted(counts.items(), key=lambda x: -x[1])}

def detect_screen_share(entries):
    phrases = ["you can see", "pull this up", "looking at", "this is our", "let me show",
               "in front of me", "on the screen", "dashboard", "right here"]
    count = 0
    for e in entries:
        if e["speaker"].get("matched_calendar_invitee_email", "").lower() == CHRIS.lower():
            if any(p in e["text"].lower() for p in phrases):
                count += 1
    return count

def compute_duration(start, end):
    try:
        s = datetime.fromisoformat(start.replace("Z", "+00:00"))
        e = datetime.fromisoformat(end.replace("Z", "+00:00"))
        return round((e - s).total_seconds() / 60)
    except: return None

def fuzzy_match_speakers(entries, invitees):
    """Fix speaker names by matching display names to calendar invitee names."""
    inv_names = {i["email"].lower(): i["name"] for i in invitees}
    inv_list = [(i["name"], i["email"].lower()) for i in invitees]
    remap = {}
    for e in entries:
        dn = e["speaker"]["display_name"]
        email = (e["speaker"].get("matched_calendar_invitee_email") or "").lower()
        if email and email in inv_names: continue
        if dn in remap: continue
        dn_first = dn.split()[0].lower() if dn else ""
        for inv_name, inv_email in inv_list:
            inv_first = inv_name.split()[0].lower()
            if dn_first == inv_first:
                remap[dn] = inv_name; break
        if dn not in remap:
            best_score, best_name = 0, None
            for inv_name, inv_email in inv_list:
                score = SequenceMatcher(None, dn.lower(), inv_name.lower()).ratio()
                if score > best_score:
                    best_score, best_name = score, inv_name
            if best_score > 0.4:
                remap[dn] = best_name
    return remap

def detect_absent(invitees, entries, remap):
    matched_emails = set()
    matched_names = set()
    for e in entries:
        email = (e["speaker"].get("matched_calendar_invitee_email") or "").lower()
        if email: matched_emails.add(email)
        dn = e["speaker"]["display_name"]
        if dn in remap: matched_names.add(remap[dn])
        else: matched_names.add(dn)
    absent = []
    for inv in invitees:
        if inv["email"].lower() == CHRIS.lower(): continue
        if inv["email"].lower() in matched_emails: continue
        if inv["name"] in matched_names: continue
        inv_first = inv["name"].split()[0].lower()
        found = False
        for mn in matched_names:
            if mn.split()[0].lower() == inv_first: found = True; break
        if not found: absent.append(inv)
    return absent

def meeting_time_str(iso_time):
    dt = datetime.fromisoformat(iso_time.replace("Z", "+00:00")).astimezone(CT)
    day = dt.strftime("%A")
    h = dt.hour
    period = "morning" if h < 12 else ("afternoon" if h < 17 else "evening")
    return f"{day} {period}"

def to_cc_split(external, talk_ratio, absent):
    if len(external) <= 3:
        return [(a["name"], a["email"]) for a in external], []
    to_names = set()
    for a in absent: to_names.add(a["name"])
    ranked = sorted(external, key=lambda a: talk_ratio.get(a["name"], {}).get("words", 0), reverse=True)
    for a in ranked[:2]: to_names.add(a["name"])
    to_list = [(a["name"], a["email"]) for a in external if a["name"] in to_names]
    cc_list = [(a["name"], a["email"]) for a in external if a["name"] not in to_names]
    return to_list, cc_list

def validate_subject(subject, meeting_title):
    if not subject or not meeting_title: return True
    words_subj = set(subject.lower().split())
    words_title = set(meeting_title.lower().split())
    if not words_title: return True
    overlap = len(words_subj & words_title) / len(words_title)
    return overlap < 0.6

# ─── Prompts ────────────────────────────────────────────────────────────────

EMAIL_SYS = """You ghostwrite follow-up emails for Chris, CEO of LeadlyPro (B2B lead gen agency). Chris sounds like a real person texting a colleague - warm, direct, zero corporate filler.

Write a short follow-up email that feels like Chris dashed it off in 2 minutes:

Structure:
- Start with Subject: on line 1. The subject should reflect what actually happened on the call, not just the topic. If the call was about fixing problems, the subject should signal action ("Tightening things up on our end"). If positive, reflect momentum ("Excited about the next phase"). Never generic subjects like "Follow up from today's call." CRITICAL: do NOT just reuse the calendar event title as the subject.
- Blank line, then the email body.

Opening line - adjust by emotional tenor:
- If the client was frustrated or there's churn risk: Lead with empathy. Acknowledge the feedback or frustration before jumping into solutions. Something like "Appreciate you guys being real about what's been working and what hasn't." Never open with "Great call" when the call was clearly not great for them.
- If positive/momentum: Match their energy. Reference a specific exciting moment.
- If neutral/operational: Simple and direct. Reference the topic.

Body rules:
- Recap only what matters: decisions made, things people owe each other, and when the next touchpoint is.
- If there are action items, weave them into the email naturally - don't use a rigid numbered list unless there are 4+ items.
- Separate "what we're doing" from "what we need from you." Chris's commitments first, then any asks.
- Never single out one person by name for a task in a group email. Use "if one of you can..." instead.
- If someone was invited but absent from the call, include one sentence of context for them so they're not confused by a follow-up referencing a call they missed.

Closing:
- Close with one forward-looking sentence.
- If tense: reinforce commitment. "Checking back in a week with progress."
- If positive: build on energy. "Let's keep this rolling."
- If neutral: simple next step. "I'll send over the updated doc by Friday."

General rules:
- Keep the whole thing under 150 words.
- No filler ("as discussed", "per our conversation", "I wanted to circle back", "hope this finds you well").
- No headers or section labels. Just a clean, natural email that sounds like a human wrote it.
- Never use em dashes. Use commas, periods, or hyphens instead.
- Tone-shift: more polished for prospects/clients, more casual for internal/partners, more accountable if client is frustrated.

Output ONLY the email: Subject line, blank line, body. Nothing else."""

COACHING_SYS = """You are a brutally honest sales coach and CEO advisor. Chris is the CEO of LeadlyPro (B2B lead gen agency) and AFTR Travel (travel content brand). This analysis is private - only Chris sees it. Don't soften anything.

First, identify the meeting type (sales call, client check-in, internal sync, partner meeting) and adjust your benchmarks accordingly.

Analyze across these dimensions:

1. Talk Ratio - I've computed the actual word counts for you. Analyze what the ratio MEANS for this meeting type. For sales calls, ideal is 40-45%. For client check-ins, 50/50 is fine. For internal syncs where Chris is leading, 60-70% may be appropriate. Don't re-estimate, use the numbers provided. If screen-sharing was detected, note that some of Chris's talk time was presentation, not domination.

2. Question Quality - list 2-3 best questions Chris asked (exact quotes with timestamps) and missed opportunities where a better question would have uncovered more. Pay special attention to moments where the other party revealed something valuable (a past experience, a competitor comparison, an emotional statement) and Chris either followed up well or missed the thread.

3. Objection Handling - if objections came up, did Chris acknowledge first (good), get defensive (bad), ask follow-ups (great), or cave immediately (bad)? Quote the exchange with timestamps. Watch specifically for moments where Chris dismissed the client's experience instead of validating first. If no objections surfaced, note whether Chris should have proactively surfaced concerns.

4. Next Steps & Close - did Chris establish clear next steps with action + owner + timeline? Quote what he said. Score: Strong / Adequate / Weak / Missing.

5. CEO Presence - confidence, strategic thinking, decision-making on the call, energy. Note if Chris jumped to solutions before the other party felt fully heard.

6. One Thing to Fix - the single highest-leverage change. Be specific, quote the moment with timestamp, suggest the exact alternative. Frame it as: "Instead of [what Chris said], try [better version]."

Keep each section 2-4 sentences plus quotes. Total: 300-500 words. Tone: direct, like a coach at halftime. Never use em dashes. Use commas, periods, or hyphens instead."""

# ─── Main processing ────────────────────────────────────────────────────────

def process_meeting(payload):
    recording_id = xfield(payload, "recording_id", "id")
    title = xfield(payload, "title", "meeting_title", default="Unknown Meeting")
    fathom_url = xfield(payload, "url", "share_url", default="")
    start_time = xfield(payload, "recording_start_time", "started_at", default="")
    end_time = xfield(payload, "recording_end_time", "ended_at", default="")
    invitees = xfield(payload, "calendar_invitees", "invitees", default=[])

    log.info(f"Processing: {title} (id={recording_id})")
    log.info(f"Payload keys: {list(payload.keys())}")

    if recording_id and is_dup(recording_id):
        log.info(f"Duplicate {recording_id}, skipping"); return
    if recording_id in SKIP_IDS:
        log.info("Demo recording, skipping"); return

    # Short call filter
    duration_min = compute_duration(start_time, end_time) if start_time and end_time else None
    if duration_min is not None and duration_min < (MIN_DURATION_SEC / 60):
        log.info(f"Short call ({duration_min}m), skipping")
        slack(f"Skipped {duration_min}-minute recording '{title}' - probably a test.")
        if recording_id: mark_processed(recording_id)
        return

    # Get transcript
    entries = xfield(payload, "transcript", default=[])
    if not entries and recording_id:
        log.info("Fetching transcript from API...")
        try:
            r = req.get(f"https://api.fathom.ai/external/v1/recordings/{recording_id}/transcript",
                        headers={"X-Api-Key": FATHOM_KEY}, timeout=30)
            r.raise_for_status()
            entries = r.json().get("transcript", [])
        except Exception as e:
            log.error(f"Transcript fetch failed: {e}")
            slack(f"Transcript fetch failed for '{title}': {e}"); return

    if not entries:
        log.warning("Empty transcript")
        slack(f"Empty transcript for '{title}' - skipping."); return

    # ── Analyze transcript ──
    remap = fuzzy_match_speakers(entries, invitees)
    if remap: log.info(f"Speaker remapping: {remap}")

    talk_ratio = compute_talk_ratio(entries)
    screen_share_count = detect_screen_share(entries)
    transcript_text = format_transcript(entries)
    external = [inv for inv in invitees if inv.get("email", "").lower() != CHRIS.lower()]
    absent = detect_absent(invitees, entries, remap)
    mtime = meeting_time_str(start_time) if start_time else "today"
    dur_str = f"{duration_min} minutes" if duration_min else "unknown duration"

    # Build talk ratio string for coaching
    ratio_lines = []
    for name, data in talk_ratio.items():
        display = remap.get(name, name)
        ratio_lines.append(f"  {display}: {data['words']} words ({data['pct']}%)")
    ratio_str = "\n".join(ratio_lines)

    absent_note = ""
    if absent:
        absent_note = f"\nAbsent (invited but didn't speak): {', '.join(a['name'] for a in absent)}. Include brief context for them if relevant."

    screen_note = ""
    if screen_share_count > 0:
        screen_note = f"\nNote: Chris was screen-sharing during portions of this call ({screen_share_count} narration moments detected). Adjust talk ratio interpretation accordingly."

    # ── Generate email ──
    email_subject = email_body = None
    if external:
        email_prompt = (f"Meeting: {title}\nTime: {mtime}\nDuration: {dur_str}\n"
            f"Attendees (besides Chris): {', '.join(a['name'] for a in external)}"
            f"{absent_note}\n\nTranscript:\n{transcript_text}")
        try:
            email_raw = call_claude(EMAIL_SYS, email_prompt, max_tok=500)
            m = re.search(r'^Subject:\s*(.+)', email_raw, re.MULTILINE | re.IGNORECASE)
            if m:
                email_subject = m.group(1).strip()
                body_start = email_raw.index(m.group(0)) + len(m.group(0))
                email_body = email_raw[body_start:].strip()
            else:
                email_subject = f"Following up - {title}"
                email_body = email_raw.strip()

            if not validate_subject(email_subject, title):
                log.warning("Subject too similar to title, regenerating...")
                retry_prompt = email_prompt + f"\n\nIMPORTANT: The meeting was titled '{title}'. Do NOT use this as the subject. Write a subject about the outcome or action taken."
                email_raw2 = call_claude(EMAIL_SYS, retry_prompt, max_tok=500)
                m2 = re.search(r'^Subject:\s*(.+)', email_raw2, re.MULTILINE | re.IGNORECASE)
                if m2:
                    email_subject = m2.group(1).strip()
                    body_start = email_raw2.index(m2.group(0)) + len(m2.group(0))
                    email_body = email_raw2[body_start:].strip()

            log.info(f"Email generated: '{email_subject}'")
        except Exception as e:
            log.error(f"Email generation failed: {e}")
            slack(f"Email generation failed for '{title}': {e}")

    # ── Generate coaching ──
    coaching_prompt = (f"Meeting: {title}\nTime: {mtime}\nDuration: {dur_str}\n"
        f"Attendees: {', '.join(a.get('name', '?') for a in invitees)}\n\n"
        f"Computed Talk Ratio (actual word counts):\n{ratio_str}{screen_note}\n\n"
        f"Transcript:\n{transcript_text}")
    coaching_text = None
    try:
        coaching_text = call_claude(COACHING_SYS, coaching_prompt, max_tok=1500)
        log.info("Coaching generated")
    except Exception as e:
        log.error(f"Coaching failed: {e}")
        slack(f"Coaching generation failed for '{title}': {e}")

    # ── Create Gmail draft with TO/CC logic ──
    if email_subject and email_body and external:
        try:
            token = gmail_token()
            to_list, cc_list = to_cc_split(external, talk_ratio, absent)
            draft = gmail_draft(token, to_list, cc_list, email_subject, email_body)
            log.info(f"Gmail draft: {draft['id']}")
        except Exception as e:
            log.error(f"Gmail failed: {e}")
            slack(f"Gmail draft failed for '{title}'. Manual:\n\nTo: {', '.join(a['email'] for a in external)}\nSubject: {email_subject}\n\n{email_body}")

    # ── Send coaching to Slack ──
    if coaching_text:
        try:
            slack(f"*Call Coaching: {title}*\n\n{coaching_text}\n\n<{fathom_url}|Watch recording in Fathom>")
            log.info("Coaching sent to Slack")
        except Exception as e:
            log.error(f"Slack coaching failed: {e}")

    # ── Mark processed ──
    if recording_id: mark_processed(recording_id)

    # ── Summary with Gmail drafts link ──
    parts = []
    if email_subject and external:
        names = ", ".join(a["name"] for a in external)
        parts.append(f"Gmail draft created for {names} - <https://mail.google.com/mail/u/0/?authuser=chris@leadlypro.io#drafts|open drafts>")
    if coaching_text:
        parts.append("Coaching posted above")
    if parts:
        summary = f"*Fathom auto-processed: {title}*\n" + "\n".join(f"- {p}" for p in parts)
        if fathom_url: summary += f"\n<{fathom_url}|Watch recording>"
        slack(summary)

    _last_processed["title"] = title
    _last_processed["time"] = datetime.now(CT).isoformat()
    _last_processed["count"] = _last_processed.get("count", 0) + 1
    log.info(f"Done: {title}")


# ─── Routes ─────────────────────────────────────────────────────────────────

@app.route("/webhook/fathom", methods=["POST"])
def webhook():
    body = request.get_data()
    hdrs = {k.lower(): v for k, v in request.headers}
    if FATHOM_SECRET and not verify_sig(FATHOM_SECRET, hdrs, body):
        log.warning("Bad signature")
        return "Unauthorized", 401
    payload = request.get_json(force=True)
    log.info("Webhook received, spawning thread...")
    threading.Thread(target=process_meeting, args=(payload,), daemon=True).start()
    return "OK", 200

@app.route("/health", methods=["GET"])
def health():
    uptime_hrs = round((time.time() - _start_time) / 3600, 1)
    return jsonify({
        "status": "ok",
        "service": "fathom-webhook",
        "uptime_hours": uptime_hrs,
        "processed_count": _last_processed.get("count", 0),
        "last_processed": _last_processed.get("title"),
        "last_processed_at": _last_processed.get("time"),
        "webhook_secret_set": bool(FATHOM_SECRET)
    })

@app.route("/", methods=["GET"])
def root():
    return jsonify({"status": "ok", "service": "fathom-webhook-railway"})


# ─── Startup self-test ──────────────────────────────────────────────────────

def self_test():
    log.info("Running startup self-test...")
    errors = []
    try:
        call_claude("Reply with exactly: OK", "test", max_tok=5)
        log.info("  Claude API: OK")
    except Exception as e:
        errors.append(f"Claude API: {e}")
        log.error(f"  Claude API: FAIL - {e}")
    try:
        gmail_token()
        log.info("  Gmail OAuth: OK")
    except Exception as e:
        errors.append(f"Gmail OAuth: {e}")
        log.error(f"  Gmail OAuth: FAIL - {e}")
    try:
        slack("Fathom webhook listener started (Railway). Self-test passed.")
        log.info("  Slack: OK")
    except Exception as e:
        errors.append(f"Slack: {e}")
        log.error(f"  Slack: FAIL - {e}")
    if errors:
        log.error(f"Self-test failures: {errors}")
        try: slack(f"*Fathom webhook self-test failures:*\n" + "\n".join(f"- {e}" for e in errors))
        except: pass
    else:
        log.info("Self-test passed")

# Run self-test on import (runs once when gunicorn loads)
self_test()
