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

# ================= ENV (GitHub Secrets) =================
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")  # Gmail App Password
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
# =======================================================

LOCATION = "United States"

# Strong, high-coverage queries (less duplication, more reach)
SEARCH_QUERIES = [
    '("FSQA" OR "Food Safety" OR "Quality Assurance" OR "Quality") (Manager OR Supervisor OR Specialist) (HACCP OR SQF OR GMP) food',
    '"Food Safety Manager" (HACCP OR SQF OR GMP) food',
    '"FSQA Manager" food',
    '"Quality Assurance Manager" food manufacturing',
    '"Quality Assurance Supervisor" food manufacturing',
    '"FSQA Supervisor" food',
    '"Quality Manager" food manufacturing',
    '"Quality Supervisor" food manufacturing',
    '"QA Manager" food manufacturing',
    '"QA Supervisor" food manufacturing',
]

# Helps keep results in food industry
FOOD_HINTS = [
    "food", "food manufacturing", "food processing", "haccp", "sqf", "fsqa", "gmp", "usda", "fsma"
]


def validate_env():
    if not SERPAPI_KEY:
        raise ValueError("SERPAPI_KEY missing (GitHub Secret).")
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        raise ValueError("EMAIL_SENDER / EMAIL_PASSWORD / EMAIL_RECEIVER missing (GitHub Secrets).")


# ---------------- SerpAPI with retry/backoff ----------------
def serpapi_google_jobs(query: str, location: str, num: int = 100) -> List[Dict[str, Any]]:
    params = {
        "engine": "google_jobs",
        "q": query,
        "location": location,
        "api_key": SERPAPI_KEY,
        "num": num,
    }

    retry_statuses = {429, 502, 503, 504}
    max_attempts = 6

    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get("https://serpapi.com/search", params=params, timeout=30)

            # retry common transient errors
            if r.status_code in retry_statuses:
                time.sleep(2 ** attempt)
                continue

            r.raise_for_status()
            return r.json().get("jobs_results", []) or []

        except requests.RequestException:
            time.sleep(2 ** attempt)

    return []


# ---------------- Helpers ----------------
def safe_pay(job: Dict[str, Any]) -> str:
    de = job.get("detected_extensions") or {}
    if isinstance(de, dict) and de.get("salary"):
        return str(de["salary"])

    ext = job.get("extensions") or []
    if isinstance(ext, list):
        for item in ext:
            if isinstance(item, str) and "$" in item:
                return item
    return "N/A"


def safe_time_posted(job: Dict[str, Any]) -> str:
    de = job.get("detected_extensions") or {}
    if isinstance(de, dict) and de.get("posted_at"):
        return str(de["posted_at"])

    ext = job.get("extensions") or []
    if isinstance(ext, list):
        for item in ext:
            if isinstance(item, str) and (
                "ago" in item.lower() or "today" in item.lower() or "yesterday" in item.lower()
            ):
                return item
    return "N/A"


def posted_minutes(time_posted: str) -> int:
    """Convert '3 hours ago', '2 days ago' etc into minutes for sorting."""
    if not time_posted or time_posted == "N/A":
        return 10**9

    s = time_posted.strip().lower()

    if "just posted" in s or "today" in s:
        return 0
    if "yesterday" in s:
        return 24 * 60

    m = re.search(r"(\d+)\s+min", s)
    if m:
        return int(m.group(1))

    m = re.search(r"(\d+)\s+hour", s)
    if m:
        return int(m.group(1)) * 60

    m = re.search(r"(\d+)\s+day", s)
    if m:
        return int(m.group(1)) * 24 * 60

    m = re.search(r"(\d+)\s+week", s)
    if m:
        return int(m.group(1)) * 7 * 24 * 60

    return 10**9


def is_within_last_7_days(time_posted: str) -> bool:
    return posted_minutes(time_posted) <= 7 * 24 * 60


def looks_food_industry(job: Dict[str, Any]) -> bool:
    text = " ".join([
        str(job.get("title") or ""),
        str(job.get("company_name") or ""),
        str(job.get("description") or ""),
    ]).lower()
    return any(h in text for h in FOOD_HINTS)


def company_careers_link(company_name: str) -> str:
    if not company_name or company_name == "N/A":
        return "N/A"
    q = f"{company_name} careers"
    return "https://www.google.com/search?q=" + urllib.parse.quote_plus(q)


def normalize_row(job: Dict[str, Any]) -> Dict[str, str]:
    company = job.get("company_name") or "N/A"
    return {
        "job_id": job.get("job_id") or "N/A",
        "title": job.get("title") or "N/A",
        "company_name": company,
        "pay": safe_pay(job),
        "time_posted": safe_time_posted(job),
        "location": job.get("location") or "N/A",
        "source": job.get("via") or "Unknown",
        "company_careers_link": company_careers_link(company),
    }


def dedupe(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for r in rows:
        key = r.get("job_id") or (r.get("title", "") + "|" + r.get("company_name", "") + "|" + r.get("location", ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def write_sheet(ws, headers, rows):
    ws.append(headers)
    for r in rows:
        ws.append([r.get(h, "N/A") for h in headers])

    # make careers link clickable
    link_col = headers.index("company_careers_link") + 1
    for i in range(2, ws.max_row + 1):
        cell = ws.cell(row=i, column=link_col)
        val = str(cell.value or "")
        if val.startswith("http"):
            cell.hyperlink = val
            cell.font = Font(color="0000FF", underline="single")


def create_excel(all_rows: List[Dict[str, str]], filename: str):
    wb = Workbook()

    headers = [
        "title",
        "company_name",
        "pay",
        "time_posted",
        "location",
        "source",
        "company_careers_link",
    ]

    # Sheet 1: All jobs
    ws1 = wb.active
    ws1.title = "All_Jobs"
    write_sheet(ws1, headers, all_rows)

    # Sheet 2: Indeed only
    indeed_rows = [r for r in all_rows if "indeed" in (r.get("source") or "").lower()]
    ws2 = wb.create_sheet("Indeed_Only")
    write_sheet(ws2, headers, indeed_rows)

    wb.save(filename)


def send_email_with_attachment(subject: str, body: str, attachment_path: str):
    msg = EmailMessage()
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER
    msg["Subject"] = subject
    msg.set_content(body)

    with open(attachment_path, "rb") as f:
        data = f.read()

    msg.add_attachment(
        data,
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=os.path.basename(attachment_path),
    )

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)


def main():
    validate_env()

    rows: List[Dict[str, str]] = []

    for q in SEARCH_QUERIES:
        jobs = serpapi_google_jobs(q, LOCATION, num=100)  # increase coverage
        for job in jobs:
            if looks_food_industry(job):
                rows.append(normalize_row(job))

    rows = dedupe(rows)

    # keep only last 7 days
    rows = [r for r in rows if is_within_last_7_days(r.get("time_posted", "N/A"))]

    # sort newest first (minutes small => newest)
    rows.sort(key=lambda r: posted_minutes(r.get("time_posted", "N/A")))

    today = datetime.now().strftime("%Y-%m-%d")
    excel_file = f"food_quality_jobs_{today}.xlsx"
    create_excel(rows, excel_file)

    subject = f"Daily Food Quality Jobs Report - {today}"
    body = (
        f"Hi,\n\n"
        f"Attached is your daily Food Industry Quality/FSQA jobs report (last 7 days).\n"
        f"Total jobs found: {len(rows)}\n\n"
        f"Excel sheets:\n"
        f"- All_Jobs (everything)\n"
        f"- Indeed_Only (only jobs where source shows Indeed)\n\n"
        f"Note: Source column typically shows: via Indeed / via LinkedIn / via Glassdoor / via ZipRecruiter.\n"
    )

    send_email_with_attachment(subject, body, excel_file)


if __name__ == "__main__":
    main()
