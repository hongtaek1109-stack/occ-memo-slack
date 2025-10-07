
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
occ_memo_daily_slack.py
Daily scraper for OCC Information Memos with Slack notification support.
- Finds new corporate-action memos (symbol changes, splits/reverse splits, mergers, tenders).
- Extracts Effective Date and key fields from the PDF text.
- Outputs Excel/CSV + Korean summary.
- Posts to Slack via Incoming Webhook (simple) or Slack API (file uploads).
"""
import argparse, csv, datetime as dt, io, os, re, sys
from dataclasses import dataclass
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd

from pdfminer.high_level import extract_text as pdf_extract_text
from dateutil import parser as dateparser

try:
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
except Exception:
    WebClient = None
    SlackApiError = Exception

BASE = "https://infomemo.theocc.com"
SEARCH_URL = f"{BASE}/infomemo/search"

EVENT_KEYWORDS = {
    "reverse split": ["reverse split"],
    "split": ["stock split", "split "],
    "name/symbol change": ["name/symbol change", "symbol change", "name change"],
    "merger": ["merger", "combination", "acquisition"],
    "tender": ["tender offer"],
    "liquidation": ["liquidation"],
}

RE_MEMO_NUMBER = re.compile(r"/infomemos\?number=(\d+)")
RE_EFFECTIVE_DATE_LINE = re.compile(r"Effective Date:\s*([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})", re.I)
RE_DATE_FIELD = re.compile(r"^Date:\s*(\d{1,2}/\d{1,2}/\d{4})\s*$", re.M)
RE_MARKET_OPEN_LINE = re.compile(r"effective (?:at|before) the (?:open|opening) (?:of (?:the )?business )?(?:on )?([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})", re.I)
RE_SUBJECT = re.compile(r"^Subject:\s*(.+)$", re.M | re.I)
RE_OPT_SYM = re.compile(r"^Option Symbol[s]?:\s*(.+)$", re.M | re.I)
RE_NEW_SYM = re.compile(r"^New Symbol[s]?:\s*(.+)$", re.M | re.I)
RE_TITLED_NEW_SYM = re.compile(r"New Symbols?:\s*([A-Za-z0-9/]+)", re.I)
RE_ADJUSTED_SYM = re.compile(r"Adjusted Option Symbol[s]?:\s*([A-Za-z0-9/]+)", re.I)

def _to_iso(date_str: str) -> Optional[str]:
    try:
        d = dateparser.parse(date_str, fuzzy=True)
        return d.strftime("%Y-%m-%d")
    except Exception:
        return None

@dataclass
class MemoRow:
    memo_number: int
    title: str
    url: str
    post_date: Optional[str] = None
    effective_date: Optional[str] = None
    event_type: Optional[str] = None
    option_symbols: Optional[str] = None
    new_symbols: Optional[str] = None
    subject: Optional[str] = None
    details: Optional[str] = None

def fetch_search_html(session: requests.Session) -> str:
    r = session.get(SEARCH_URL, timeout=30)
    r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    return r.text

def parse_search_listing(html: str) -> List[MemoRow]:
    soup = BeautifulSoup(html, "html.parser")
    rows: List[MemoRow] = []
    for a in soup.find_all("a", href=True):
        m = RE_MEMO_NUMBER.search(a["href"])
        if not m:
            continue
        num = int(m.group(1))
        title = a.get_text(strip=True)
        url = BASE + a["href"] if a["href"].startswith("/infomemos") else a["href"]
        post_date = None
        effective_date = None
        try:
            parent = a.parent
            texts_back = []
            sib = parent.previous_sibling
            cnt = 0
            while sib and cnt < 12:
                if hasattr(sib, "get_text"):
                    texts_back.append(sib.get_text(" ", strip=True))
                elif isinstance(sib, str):
                    texts_back.append(sib.strip())
                sib = sib.previous_sibling
                cnt += 1
            candidates = [t for t in texts_back if t]
            candidates = list(reversed(candidates))
            dates = [t for t in candidates if re.match(r"\d{2}/\d{2}/\d{4}", t)]
            if len(dates) >= 1:
                post_date = _to_iso(dates[0])
            if len(dates) >= 2:
                effective_date = _to_iso(dates[1])
        except Exception:
            pass
        rows.append(MemoRow(memo_number=num, title=title, url=url,
                            post_date=post_date, effective_date=effective_date))
    uniq = {}
    for r in rows:
        if r.memo_number not in uniq:
            uniq[r.memo_number] = r
    return sorted(uniq.values(), key=lambda x: x.memo_number, reverse=True)

def fetch_pdf_text(session: requests.Session, url: str) -> str:
    r = session.get(url, timeout=60)
    r.raise_for_status()
    if "application/pdf" in r.headers.get("Content-Type", "") or url.lower().endswith(".pdf") or "infomemos?number=" in url:
        data = io.BytesIO(r.content)
        text = pdf_extract_text(data)
        return text
    return r.text

def classify_event(title: str, subject: Optional[str]) -> Optional[str]:
    text = (title + " " + (subject or "")).lower()
    for label, keys in EVENT_KEYWORDS.items():
        for k in keys:
            if k in text:
                return label
    if "reverse" in text and "split" in text:
        return "reverse split"
    if "stock split" in text or re.search(r"\b\d+\s*for\s*\d+\b", text):
        return "split"
    return None

def parse_pdf_fields(text: str) -> Dict[str, Optional[str]]:
    subject = None
    opt_syms = None
    new_syms = None
    eff_iso = None
    m = RE_SUBJECT.search(text)
    if m: subject = m.group(1).strip()
    m = RE_OPT_SYM.search(text)
    if m: opt_syms = m.group(1).strip()
    m = RE_NEW_SYM.search(text)
    if m: new_syms = m.group(1).strip()
    else:
        m = RE_TITLED_NEW_SYM.search(text)
        if m: new_syms = m.group(1).strip()
        else:
            m = RE_ADJUSTED_SYM.search(text)
            if m: new_syms = m.group(1).strip()
    m = RE_EFFECTIVE_DATE_LINE.search(text)
    if m:
        eff_iso = _to_iso(m.group(1))
    else:
        m = RE_DATE_FIELD.search(text)
        if m:
            eff_iso = _to_iso(m.group(1))
        else:
            m = RE_MARKET_OPEN_LINE.search(text)
            if m:
                eff_iso = _to_iso(m.group(1))
    return dict(subject=subject, option_symbols=opt_syms, new_symbols=new_syms, effective_date=eff_iso)

def build_korean_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "오늘 기준 향후 효력 예정 이슈가 없습니다."
    lines = []
    header = f"{'메모#':<8} {'종목(옵션심볼)':<30} {'변경내용':<20} {'메모 게시일':<12} {'Effective Date':<12}"
    lines.append("```")
    lines.append(header)
    lines.append("-"*len(header))
    for _, r in df.iterrows():
        memo = f"#{int(r['memo_number'])}"
        sym = f"{(r.get('option_symbols') or '')}"
        if r.get('new_symbols'):
            sym = f"{sym} → {r['new_symbols']}"
        evt = (r.get('event_type') or '').title()
        post = (r.get('post_date') or '') or ''
        eff = (r.get('effective_date') or '') or ''
        lines.append(f"{memo:<8} {sym:<30} {evt:<20} {post:<12} {eff:<12}")
    lines.append("```")
    return "\n".join(lines)

def send_slack_webhook(webhook_url: str, text: str) -> None:
    try:
        resp = requests.post(webhook_url, json={"text": text}, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"[warn] Slack webhook failed: {e}", file=sys.stderr)

def send_slack_sdk(token: str, channel: str, text: str, files=None) -> None:
    if WebClient is None:
        print("[warn] slack_sdk not installed; cannot upload via API.", file=sys.stderr)
        return
    try:
        client = WebClient(token=token)
        client.chat_postMessage(channel=channel, text=text)
        if files:
            for f in files:
                if os.path.exists(f):
                    client.files_upload_v2(channels=channel, file=f, initial_comment="첨부 파일")
    except Exception as e:
        print(f"[warn] Slack API failed: {e}", file=sys.stderr)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="./out")
    p.add_argument("--state", default="./occ_last_number.txt")
    p.add_argument("--since-posted-days", type=int, default=None)
    p.add_argument("--include", nargs="*", default=["reverse", "split", "name", "symbol", "merger"])
    p.add_argument("--exclude-past-effective", action="store_true")
    p.add_argument("--slack-webhook", default=None)
    p.add_argument("--slack-token", default=None)
    p.add_argument("--slack-channel", default=None)
    p.add_argument("--slack-upload-files", action="store_true")
    args = p.parse_args()

    os.makedirs(args.out, exist_ok=True)

    session = requests.Session()
    html = fetch_search_html(session)
    listing = parse_search_listing(html)

    today = dt.datetime.now().date()
    picked: List[MemoRow] = []

    if args.since_posted_days is not None:
        cutoff = today - dt.timedelta(days=args.since_posted_days)
        for r in listing:
            if r.post_date:
                try:
                    pd_ = dt.date.fromisoformat(r.post_date)
                except Exception:
                    continue
                if pd_ >= cutoff:
                    picked.append(r)
    else:
        try:
            last_n = 0
            if os.path.exists(args.state):
                with open(args.state, "r", encoding="utf-8") as f:
                    last_n = int(f.read().strip())
        except Exception:
            last_n = 0
        for r in listing:
            if r.memo_number > last_n:
                picked.append(r)

    if not picked:
        print("No new memos found on search page.")
        return

    results: List[MemoRow] = []
    include_text = " ".join(args.include).lower()
    for r in picked:
        try:
            text = fetch_pdf_text(session, r.url)
            fields = parse_pdf_fields(text)
            r.subject = fields.get("subject")
            r.option_symbols = fields.get("option_symbols") or r.option_symbols
            r.new_symbols = fields.get("new_symbols") or r.new_symbols
            r.effective_date = fields.get("effective_date") or r.effective_date
            r.event_type = classify_event(r.title, r.subject)

            if include_text:
                hay = (r.title + " " + (r.subject or "")).lower()
                if not any([(kw in hay) for kw in include_text.split()]):
                    continue
            results.append(r)
        except Exception as e:
            r.details = f"parse_error: {e}"
            results.append(r)

    df = pd.DataFrame([{
        "memo_number": r.memo_number,
        "post_date": r.post_date,
        "effective_date": r.effective_date,
        "event_type": r.event_type,
        "title": r.title,
        "subject": r.subject,
        "option_symbols": r.option_symbols,
        "new_symbols": r.new_symbols,
        "url": r.url,
    } for r in results])

    if args.exclude-past-effective and not df.empty:
        def iso_to_date(x):
            try:
                return dt.date.fromisoformat(x)
            except Exception:
                return None
        df["eff_d"] = df["effective_date"].apply(lambda x: iso_to_date(x) if isinstance(x, str) else None)
        df = df[df["eff_d"].isna() | (df["eff_d"] >= today)]
        df = df.drop(columns=["eff_d"])

    if not df.empty:
        df = df.sort_values(by=["memo_number"], ascending=False)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    xlsx_path = os.path.join(args.out, f"OCC_memos_{ts}.xlsx")
    csv_path  = os.path.join(args.out, f"OCC_memos_{ts}.csv")
    df.to_excel(xlsx_path, index=False)
    df.to_csv(csv_path, index=False, quoting=csv.QUOTE_MINIMAL)

    latest_csv = os.path.join(args.out, "latest.csv")
    df.to_csv(latest_csv, index=False, quoting=csv.QUOTE_MINIMAL)

    # Slack payload
    summary_lines = []
    if df.empty:
        summary_lines.append("*[OCC] 오늘 신규 이슈 없음*")
    else:
        summary_lines.append("*[OCC] 신규 기업행동/조정 메모 요약*")
        for _, row in df.iterrows():
            sym = row.get("option_symbols") or ""
            if row.get("new_symbols"):
                sym = f"{sym} → {row['new_symbols']}"
            evt = (row.get("event_type") or "").title()
            eff = row.get("effective_date") or "(미기재)"
            summary_lines.append(f"- #{int(row['memo_number'])} <{row['url']}|링크> — {sym} — {evt} — 효력일: *{eff}*")
    table_text = build_korean_table(df)
    msg = "\n".join(summary_lines) + "\n" + table_text

    print(msg)

    max_num = max([r.memo_number for r in listing]) if listing else 0
    try:
        with open(args.state, "w", encoding="utf-8") as f:
            f.write(str(max_num))
    except Exception as e:
        print(f"[warn] cannot write state file: {e}", file=sys.stderr)

    if args.slack_webhook:
        send_slack_webhook(args.slack_webhook, msg)

    if args.slack_token and args.slack_channel:
        files = [xlsx_path, csv_path] if args.slack_upload_files else None
        send_slack_sdk(args.slack_token, args.slack_channel, msg, files)

if __name__ == "__main__":
    main()
