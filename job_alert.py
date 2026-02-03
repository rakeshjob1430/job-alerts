import os
import requests
import smtplib
import ssl
from email.message import EmailMessage
from datetime import datetime
from typing import List, Dict, Any
from openpyxl import Workbook

# ============== ENV (GitHub Secrets) ==============
SERPAPI_KEY = os.getenv("SERPAPI_KEY")

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")  # Gmail App Password
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
# ================================================

LOCATION = "United States"

# Food-industry quality roles (your request)
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
]

# Food context terms to bias results to food industry
FOOD_CONTEXT = [
    "food manufacturing",
    "food processing",
    "food industry",
    "FSQA",
    "HACCP",
    "SQF",
]


def validate_env():
    if not SERPAPI_KEY:
        raise ValueError("SERPAPI_KEY missing (GitHub Secret).")
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        raise ValueError("EMAIL_SENDER / EMAIL_PASSWORD / EMAIL_RECEIVER missing (GitHub Secrets).")


def serpapi_google_jobs(query: str, location: str) -> List[Dict[str, Any]]:
    """Fetch jobs from SerpAPI Google Jobs."""
    params = {
        "engine": "google_jobs",
        "q": query,
        "location": location,
        "api_key": SERPAPI_KEY,
    }
    r = requests.get("https://serpapi.com/search", params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("jobs_results", []) or []


def serpapi_google_jobs_listing(job_id: str) -> Dict[str, Any]:
    """Fetch detailed job listing info (often includes better apply links, posted time, salary)."""
    params = {
        "engine": "google_jobs_listing",
        "job_id": job_id,
        "api_key": SERPAPI_KEY,
    }
    r = requests.get("https://serpapi.com/search", params=params, timeout=30)
    r.raise_for_status()
    return r.json() or {}


def safe_apply_link_from_job(job: Dict[str, Any]) -> str:
    links = job.get("related_links") or []
    if isinstance(links, list) and len(links) > 0:
        return links[0].get("link") or "N/A"
    return "N/A"


def safe_apply_link_from_details(details: Dict[str, Any]) -> str:
    apply_options = details.get("apply_options") or []
    if isinstance(apply_options, list) and len(apply_options) > 0:
        return apply_options[0].get("link") or "N/A"
    return "N/A"


def safe_pay(job: Dict[str, Any]) -> str:
    de = job.get("detected_extensions") or {}
    if isinstance(de, dict):
        sal = de.get("salary")
        if sal:
            return str(sal)

    ext = job.get("extensions") or []
    if isinstance(ext, list):
        for item in ext:
            if isinstance(item, str) and ("$" in item or "hour" in item.lower() or "year" in item.lower() or "per" in item.lower()):
                return item
    return "N/A"


def safe_pay_from_details(details: Dict[str, Any]) -> str:
    de = details.get("detected_extensions") or {}
    if isinstance(de, dict):
        sal = de.get("salary")
        if sal:
            return str(sal)

    ext = details.get("extensions") or []
    if isinstance(ext, list):
        for item in ext:
            if isinstance(item, str) and ("$" in item or "hour" in item.lower() or "year" in item.lower() or "per" in item.lower()):
                return item
    return "N/A"


def safe_time_posted(job: Dict[str, Any]) -> str:
    de = job.get("detected_extensions") or {}
    if isinstance(de, dict):
        posted = de.get("posted_at")
        if posted:
            return str(posted)

    ext = job.get("extensions") or []
    if isinstance(ext, list):
        for item in ext:
            if isinstance(item, str) and ("ago" in item.lower() or "posted" in item.lower() or "today" in item.lower() or "yesterday" in item.lower()):
                return item
    return "N/A"


def safe_time_posted_from_details(details: Dict[str, Any]) -> str:
    de = details.get("detected_extensions") or {}
    if isinstance(de, dict):
        posted = de.get("posted_at")
        if posted:
            return str(posted)

    ext = details.get("extensions") or []
    if isinstance(ext, list):
        for item in ext:
            if isinstance(item, str) and ("ago" in item.lower() or "posted" in item.lower() or "today" in item.lower() or "yesterday" in item.lower()):
                return item
    return "N/A"


def normalize_row(job: Dict[str, Any]) -> Dict[str, str]:
    job_id = job.get("job_id")

    title = job.get("title") or "N/A"
    company = job.get("company_name") or "N/A"
    location = job.get("location") or "N/A"
    source = job.get("via") or "Unknown"

    pay = safe_pay(job)
    time_posted = safe_time_posted(job)
    apply_link = safe_apply_link_from_job(job)

    # If important fields are missing, fetch details to reduce N/A
    if job_id and (pay == "N/A" or time_posted == "N/A" or apply_link == "N/A"):
        details = serpapi_google_jobs_listing(job_id)

        if pay == "N/A":
            pay = safe_pay_from_details(details)

        if time_posted == "N/A":
            time_posted = safe_time_posted_from_details(details)

        if apply_link == "N/A":
            apply_link = safe_apply_link_from_details(details)

        # sometimes source appears here too
        if source == "Unknown":
            source = details.get("via") or source

    return {
        "title": title,
        "company_name": company,
        "pay": pay if pay else "N/A",
        "time_posted": time_posted if time_posted else "N/A",
        "location": location,
        "source": source,
        "apply_link": apply_link,
    }


def build_queries() -> List[str]:
    food_context = " OR ".join([f'"{x}"' for x in FOOD_CONTEXT])
    return [f'"{role}" ({food_context})' for role in ROLE_KEYWORDS]


def dedupe_by_link(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for row in rows:
        key = row.get("apply_link") or (row.get("title", "") + row.get("company_name", ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def create_excel(rows: List[Dict[str, str]], filename: str) -> str:
    wb = Workbook()
    ws = wb.active
    ws.title = "Jobs"

    headers = ["title", "company_name", "pay", "time_posted", "location", "source", "apply_link"]
    ws.append(headers)

    for r in rows:
        ws.append([r.get(h, "N/A") for h in headers])

    wb.save(filename)
    return filename


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

    queries = build_queries()
    all_rows: List[Dict[str, str]] = []

    for q in queries:
        jobs = serpapi_google_jobs(q, LOCATION)
        for job in jobs:
            all_rows.append(normalize_row(job))

    all_rows = dedupe_by_link(all_rows)

    today = datetime.now().strftime("%Y-%m-%d")
    excel_file = f"food_quality_jobs_{today}.xlsx"
    create_excel(all_rows, excel_file)

    subject = f"Daily Food Quality Jobs Report - {today}"
    body = f"""Hi,

Attached is your daily Food Industry Quality job report.
Total jobs found: {len(all_rows)}

Columns:
title, company name, pay, time posted, location, source, link to apply

Regards,
Job Bot
"""
    send_email_with_attachment(subject, body, excel_file)


if __name__ == "__main__":
    main()
