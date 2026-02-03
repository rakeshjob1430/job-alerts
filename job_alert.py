import os
import re
import time
import urllib.parse
import requests
import smtplib
import ssl
from email.message import EmailMessage
from datetime import datetime
from typing import List, Dict, Any
from openpyxl import Workbook
from openpyxl.styles import Font

# ============== ENV (GitHub Secrets) ==============
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
# ================================================

LOCATION = "United States"

ROLE_KEYWORDS = [
    "Quality Assurance Supervisor",
    "Quality Assurance Manager",
    "QA Supervisor",
    "QA Manager",
    "Quality Supervisor",
    "Quality Manager",
    "FSQ Manager",
    "FSQ Supervisor",
    "FSQ Specialist",
    "FSQA Manager",
    "FSQA Supervisor",
    "FSQA Specialist",
    "Food Safety Manager",
    "Food Safety Supervisor",
    "Quality Specialist",
    "Quality Lead",
]

FOOD_OR_TERMS = "(food OR dairy OR meat OR beverage OR bakery OR HACCP OR SQF OR GMP OR FSQA OR USDA OR FDA)"

# Pagination / fetching more jobs
NUM_PER_PAGE = 10
MAX_PAGES = 6  # 6 pages x 10 = ~60 results per query
RETRY_STATUS = {429, 502, 503, 504}


def validate_env():
    if not SERPAPI_KEY:
        raise ValueError("SERPAPI_KEY missing")
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        raise ValueError("Email secrets missing")


def serpapi_google_jobs(query: str, location: str, start: int = 0, num: int = 10) -> List[Dict[str, Any]]:
    params = {
        "engine": "google_jobs",
        "q": query,
        "location": location,
        "api_key": SERPAPI_KEY,
        "num": num,
        "start": start,
    }

    for attempt in range(1, 6):
        try:
            r = requests.get("https://serpapi.com/search", params=params, timeout=30)
            if r.status_code in RETRY_STATUS:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r.json().get("jobs_results", []) or []
        except requests.RequestException:
            time.sleep(2 ** attempt)

    return []


def safe_pay(job: Dict[str, Any]) -> str:
    de = job.get("detected_extensions") or {}
    if isinstance(de, dict) and de.get("salary"):
        return str(de["salary"])

    for item in job.get("extensions", []) or []:
        if isinstance(item, str) and ("$" in item or "hour" in item.lower() or "year" in item.lower()):
            return item
    return "N/A"


def safe_time_posted(job: Dict[str, Any]) -> str:
    de = job.get("detected_extensions") or {}
    if isinstance(de, dict) and de.get("posted_at"):
        return str(de["posted_at"])

    for item in job.get("extensions", []) or []:
        if isinstance(item, str) and (
            "ago" in item.lower() or "today" in item.lower() or "yesterday" in item.lower()
        ):
            return item
    return "N/A"


def posted_days(time_posted: str) -> int:
    if not time_posted or time_posted == "N/A":
        return 999

    s = time_posted.lower()
    if "today" in s or "just" in s:
        return 0
    if "yesterday" in s:
        return 1

    m = re.search(r"(\d+)\s+hour", s)
    if m:
        return 0

    m = re.search(r"(\d+)\s+day", s)
    if m:
        return int(m.group(1))

    m = re.search(r"(\d+)\s+week", s)
    if m:
        return int(m.group(1)) * 7

    return 999


def safe_apply_link(job: Dict[str, Any]) -> str:
    links = job.get("related_links") or []
    if isinstance(links, list) and links:
        link = links[0].get("link")
        if link and link.startswith("http"):
            return link
    return ""


def google_apply_search_link(title: str, company: str) -> str:
    q = f'{title} {company} apply'
    return "https://www.google.com/search?q=" + urllib.parse.quote_plus(q)


def company_careers_search_link(company: str) -> str:
    q = f"{company} careers"
    return "https://www.google.com/search?q=" + urllib.parse.quote_plus(q)


def normalize_row(job: Dict[str, Any]) -> Dict[str, str]:
    title = job.get("title") or "N/A"
    company = job.get("company_name") or "N/A"
    location = job.get("location") or "N/A"
    source = job.get("via") or "Unknown"

    pay = safe_pay(job)
    time_posted = safe_time_posted(job)

    # Apply link logic: prefer direct apply link, else fallback to google search apply link
    apply_link = safe_apply_link(job)
    if not apply_link:
        apply_link = google_apply_search_link(title, company)

    careers_link = company_careers_search_link(company)

    return {
        "job_id": job.get("job_id") or f"{title}|{company}|{location}",
        "title": title,
        "company_name": company,
        "pay": pay,
        "time_posted": time_posted,
        "location": location,
        "source": source,
        "apply_link": apply_link,
        "company_careers_link": careers_link,
    }


def build_queries() -> List[str]:
    # Stronger queries that pull more “hours ago” jobs
    queries = []
    for role in ROLE_KEYWORDS:
        queries.append(f'"{role}" {FOOD_OR_TERMS}')
        queries.append(f'"{role}" (HACCP OR SQF OR GMP OR FSQA)')
    return queries


def dedupe(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for r in rows:
        key = r["job_id"]
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def create_excel(rows: List[Dict[str, str]], filename: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "Jobs"

    headers = [
        "title",
        "company_name",
        "pay",
        "time_posted",
        "location",
        "source",
        "apply_link",
        "company_careers_link",
    ]
    ws.append(headers)

    for r in rows:
        ws.append([r.get(h, "N/A") for h in headers])

    # clickable links
    link_cols = {
        "apply_link": headers.index("apply_link") + 1,
        "company_careers_link": headers.index("company_careers_link") + 1,
    }
    for row_idx in range(2, ws.max_row + 1):
        for _, col_idx in link_cols.items():
            cell = ws.cell(row=row_idx, column=col_idx)
            val = str(cell.value or "")
            if val.startswith("http"):
                cell.hyperlink = val
                cell.font = Font(color="0000FF", underline="single")

    wb.save(filename)


def send_email_with_attachment(excel_path: str, total: int):
    today = datetime.now().strftime("%Y-%m-%d")
    msg = EmailMessage()
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER
    msg["Subject"] = f"Daily Food Quality Jobs Report - {today}"

    msg.set_content(
        f"Hi,\n\nAttached is your daily Food Quality/FSQA job report.\n"
        f"Jobs included (last 7 days): {total}\n\n"
        f"Links:\n"
        f"- apply_link: direct apply link if available, otherwise Google apply search link (always works)\n"
        f"- company_careers_link: Google careers search link (always works)\n"
    )

    with open(excel_path, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=os.path.basename(excel_path),
        )

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl.create_default_context()) as server:
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)


def main():
    validate_env()

    rows: List[Dict[str, str]] = []
    queries = build_queries()

    for q in queries:
        # pagination for each query
        for page in range(MAX_PAGES):
            start = page * NUM_PER_PAGE
            jobs = serpapi_google_jobs(q, LOCATION, start=start, num=NUM_PER_PAGE)
            if not jobs:
                break
            for job in jobs:
                rows.append(normalize_row(job))

    rows = dedupe(rows)

    # last 7 days (includes "hours ago")
    rows = [r for r in rows if posted_days(r.get("time_posted", "N/A")) <= 7]

    # newest first (hours ago first)
    rows.sort(key=lambda r: posted_days(r.get("time_posted", "N/A")))

    today = datetime.now().strftime("%Y-%m-%d")
    excel_file = f"food_quality_jobs_{today}.xlsx"
    create_excel(rows, excel_file)
    send_email_with_attachment(excel_file, len(rows))


if __name__ == "__main__":
    main()
