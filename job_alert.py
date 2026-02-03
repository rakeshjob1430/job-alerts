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

def validate_env():
    if not SERPAPI_KEY:
        raise ValueError("SERPAPI_KEY missing")
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        raise ValueError("Email secrets missing")


def serpapi_google_jobs(query: str, location: str, num: int = 100) -> List[Dict[str, Any]]:
    params = {
        "engine": "google_jobs",
        "q": query,
        "location": location,
        "api_key": SERPAPI_KEY,
        "num": num,
    }

    retry_statuses = {429, 502, 503, 504}
    for attempt in range(1, 5):
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


def safe_pay(job: Dict[str, Any]) -> str:
    de = job.get("detected_extensions") or {}
    if isinstance(de, dict) and de.get("salary"):
        return str(de["salary"])
    for item in job.get("extensions", []) or []:
        if isinstance(item, str) and "$" in item:
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


def company_careers_link(company: str) -> str:
    if not company:
        return "N/A"
    q = f"{company} careers"
    return "https://www.google.com/search?q=" + urllib.parse.quote_plus(q)


def indeed_search_link(title: str) -> str:
    q = f"{title} {LOCATION}"
    return "https://www.indeed.com/jobs?q=" + urllib.parse.quote_plus(q)


def normalize_row(job: Dict[str, Any]) -> Dict[str, str]:
    title = job.get("title", "N/A")
    company = job.get("company_name", "N/A")
    return {
        "job_id": job.get("job_id", "N/A"),
        "title": title,
        "company_name": company,
        "pay": safe_pay(job),
        "time_posted": safe_time_posted(job),
        "location": job.get("location", "N/A"),
        "source": job.get("via", "Unknown"),
        "company_careers_link": company_careers_link(company),
        "indeed_search_link": indeed_search_link(title),
    }


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
        "title", "company_name", "pay", "time_posted",
        "location", "source", "company_careers_link", "indeed_search_link"
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
        f"Attached is your daily Food Quality/FSQA jobs report (last 7 days).\n"
        f"Total jobs: {count}\n\n"
        f"Use the company careers link and Indeed search link to apply quickly."
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

    # ONE strong query (fast!)
    big_query = (
        '("Quality Assurance Supervisor" OR "QA Supervisor" OR "Quality Supervisor" OR '
        '"Quality Assurance Manager" OR "QA Manager" OR "Quality Manager" OR '
        '"FSQA Supervisor" OR "FSQA Manager" OR "FSQ Supervisor" OR "FSQ Manager" OR '
        '"Food Safety Supervisor" OR "Food Safety Manager" OR "Quality Specialist") '
        'food manufacturing OR food processing OR HACCP OR SQF OR FSQA'
    )

    jobs = serpapi_google_jobs(big_query, LOCATION, num=100)
    rows = [normalize_row(j) for j in jobs]

    rows = dedupe(rows)

    # last 7 days
    rows = [r for r in rows if posted_days(r.get("time_posted", "N/A")) <= 7]

    # newest first
    rows.sort(key=lambda r: posted_days(r.get("time_posted", "N/A")))

    today = datetime.now().strftime("%Y-%m-%d")
    excel_file = f"food_quality_jobs_{today}.xlsx"
    create_excel(rows, excel_file)
    send_email(excel_file, len(rows))


if __name__ == "__main__":
    main()
