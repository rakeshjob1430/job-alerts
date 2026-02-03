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

# ================= ENV =================
SERPAPI_KEY = os.getenv("SERPAPI_KEY")

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
# ======================================

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

FOOD_HINTS = [
    "food", "food manufacturing", "food processing", "meat", "dairy",
    "bakery", "beverage", "plant", "production", "HACCP", "SQF",
    "FSQA", "GMP", "sanitation"
]


def validate_env():
    if not SERPAPI_KEY:
        raise ValueError("SERPAPI_KEY missing")
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        raise ValueError("Email secrets missing")


# ---------------- SerpAPI (retry-safe) ----------------
def serpapi_google_jobs(query: str, location: str, num: int = 50) -> List[Dict[str, Any]]:
    params = {
        "engine": "google_jobs",
        "q": query,
        "location": location,
        "api_key": SERPAPI_KEY,
        "num": num,
    }

    retry_statuses = {429, 502, 503, 504}
    for attempt in range(1, 6):
        try:
            r = requests.get("https://serpapi.com/search", params=params, timeout=30)
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

    for item in job.get("extensions", []) or []:
        if "$" in item:
            return item
    return "N/A"


def safe_time_posted(job: Dict[str, Any]) -> str:
    de = job.get("detected_extensions") or {}
    if isinstance(de, dict) and de.get("posted_at"):
        return str(de["posted_at"])

    for item in job.get("extensions", []) or []:
        if "ago" in item.lower() or "today" in item.lower():
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
    m = re.search(r"(\d+)\s+day", s)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s+week", s)
    if m:
        return int(m.group(1)) * 7
    return 999


def looks_food_industry(job: Dict[str, Any]) -> bool:
    text = " ".join([
        str(job.get("title", "")),
        str(job.get("company_name", "")),
        str(job.get("description", "")),
    ]).lower()
    return any(h in text for h in FOOD_HINTS)


def company_careers_link(company: str) -> str:
    if not company:
        return "N/A"
    q = f"{company} careers"
    return "https://www.google.com/search?q=" + urllib.parse.quote_plus(q)


def normalize_row(job: Dict[str, Any]) -> Dict[str, str]:
    return {
        "job_id": job.get("job_id", "N/A"),
        "title": job.get("title", "N/A"),
        "company_name": job.get("company_name", "N/A"),
        "pay": safe_pay(job),
        "time_posted": safe_time_posted(job),
        "location": job.get("location", "N/A"),
        "source": job.get("via", "Unknown"),
        "company_careers_link": company_careers_link(job.get("company_name", "")),
    }


def build_queries() -> List[str]:
    queries = []
    for role in ROLE_KEYWORDS:
        queries.append(f'"{role}" food')
        queries.append(f'"{role}" food manufacturing')
        queries.append(f'"{role}" HACCP')
        queries.append(f'"{role}" SQF')
    return queries


def dedupe_by_job_id(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
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
        "company_careers_link",
    ]
    ws.append(headers)

    for r in rows:
        ws.append([r[h] for h in headers])

    link_col = headers.index("company_careers_link") + 1
    for i in range(2, ws.max_row + 1):
        cell = ws.cell(row=i, column=link_col)
        if cell.value.startswith("http"):
            cell.hyperlink = cell.value
            cell.font = Font(color="0000FF", underline="single")

    wb.save(filename)


def send_email(excel_file: str, count: int):
    today = datetime.now().strftime("%Y-%m-%d")
    msg = EmailMessage()
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER
    msg["Subject"] = f"Daily Food Quality Jobs Report - {today}"

    msg.set_content(
        f"Attached is your Food Industry Quality/FSQA jobs report.\n"
        f"Jobs found (last 7 days): {count}\n\n"
        f"Use the company careers link to apply directly."
    )

    with open(excel_file, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=excel_file,
        )

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl.create_default_context()) as server:
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)


def main():
    validate_env()

    rows = []
    for q in build_queries():
        jobs = serpapi_google_jobs(q, LOCATION, num=50)
        for job in jobs:
            if looks_food_industry(job):
                rows.append(normalize_row(job))

    rows = dedupe_by_job_id(rows)
    rows = [r for r in rows if posted_days(r["time_posted"]) <= 7]
    rows.sort(key=lambda r: posted_days(r["time_posted"]))

    today = datetime.now().strftime("%Y-%m-%d")
    excel_file = f"food_quality_jobs_{today}.xlsx"
    create_excel(rows, excel_file)
    send_email(excel_file, len(rows))


if __name__ == "__main__":
    main()
