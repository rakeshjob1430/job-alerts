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

SERPAPI_KEY = os.getenv("SERPAPI_KEY")

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")

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


# ---------------- SerpAPI calls with pagination + retries ----------------
def serpapi_google_jobs(query: str, location: str, num: int = 100, pages: int = 3) -> List[Dict[str, Any]]:
    """
    Fetches multiple pages so we get more results (Indeed often appears later pages).
    pages=3 with num=100 can collect up to ~300 items (depends on availability).
    """
    results: List[Dict[str, Any]] = []
    start = 0

    retry_statuses = {429, 502, 503, 504}
    max_attempts = 5

    for _ in range(pages):
        params = {
            "engine": "google_jobs",
            "q": query,
            "location": location,
            "api_key": SERPAPI_KEY,
            "num": num,
            "start": start,
        }

        for attempt in range(1, max_attempts + 1):
            try:
                r = requests.get("https://serpapi.com/search", params=params, timeout=30)

                if r.status_code in retry_statuses:
                    time.sleep(2 ** attempt)
                    continue

                r.raise_for_status()
                data = r.json()
                page_jobs = data.get("jobs_results", []) or []
                results.extend(page_jobs)
                break
            except requests.RequestException:
                time.sleep(2 ** attempt)

        start += num

    return results


# ---------------- Helpers ----------------
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
        if isinstance(item, str) and ("ago" in item.lower() or "today" in item.lower() or "yesterday" in item.lower()):
            return item
    return "N/A"


def posted_days(time_posted: str) -> int:
    if not time_posted or time_posted == "N/A":
        return 999
    s = time_posted.lower()
    if "today" in s or "just posted" in s:
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


def indeed_search_link(title: str, location: str) -> str:
    """
    Always-works Indeed search link (no N/A).
    """
    q = f"{title} {location}"
    return "https://www.indeed.com/jobs?q=" + urllib.parse.quote_plus(q)


def normalize_row(job: Dict[str, Any]) -> Dict[str, str]:
    title = job.get("title", "N/A")
    company = job.get("company_name", "N/A")
    location = job.get("location", "N/A")
    source = job.get("via", "Unknown")

    return {
        "job_id": job.get("job_id", "N/A"),
        "title": title,
        "company_name": company,
        "pay": safe_pay(job),
        "time_posted": safe_time_posted(job),
        "location": location,
        "source": source,
        "company_careers_link": company_careers_link(company),
        "indeed_search_link": indeed_search_link(title, LOCATION),
    }


def build_queries() -> List[str]:
    queries = []
    for role in ROLE_KEYWORDS:
        queries.append(f'"{role}" food')
        queries.append(f'"{role}" food manufacturing')
        queries.append(f'"{role}" food processing')
        queries.append(f'"{role}" HACCP')
        queries.append(f'"{role}" SQF')
        queries.append(f'"{role}" FSQA')
    return queries


def dedupe(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for r in rows:
        key = r.get("job_id") or (r["title"] + "|" + r["company_name"] + "|" + r["location"])
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
        "indeed_search_link",
    ]
    ws.append(headers)

    for r in rows:
        ws.append([r.get(h, "N/A") for h in headers])

    # clickable links
    for link_field in ["company_careers_link", "indeed_search_link"]:
        col = headers.index(link_field) + 1
        for row_idx in range(2, ws.max_row + 1):
            cell = ws.cell(row=row_idx, column=col)
            val = str(cell.value or "")
            if val.startswith("http"):
                cell.hyperlink = val
                cell.font = Font(color="0000FF", underline="single")

    wb.save(filename)


def send_email(excel_file: str, count: int):
    today = datetime.now().strftime("%Y-%m-%d")

    msg = EmailMessage()
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER
    msg["Subject"] = f"Daily Food Quality Jobs Report - {today}"
    msg.set_content(
        f"Attached is your daily Food Quality/FSQA report (last 7 days).\n"
        f"Total jobs: {count}\n\n"
        f"Tip: If source is not Indeed, use the Indeed search link column."
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

    rows: List[Dict[str, str]] = []
    for q in build_queries():
        # Fetch more results/pages so we get Indeed results too
        jobs = serpapi_google_jobs(q, LOCATION, num=100, pages=3)
        for job in jobs:
            if looks_food_industry(job):
                rows.append(normalize_row(job))

    rows = dedupe(rows)

    # last 7 days only
    rows = [r for r in rows if posted_days(r.get("time_posted", "N/A")) <= 7]

    # sort newest first
    rows.sort(key=lambda r: posted_days(r.get("time_posted", "N/A")))

    today = datetime.now().strftime("%Y-%m-%d")
    excel_file = f"food_quality_jobs_{today}.xlsx"

    create_excel(rows, excel_file)
    send_email(excel_file, len(rows))


if __name__ == "__main__":
    main()
