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

# Your requested quality roles + food industry focus
ROLE_KEYWORDS = [
    "Quality Assurance Supervisor",
    "QA Manager",
    "Quality Manager",
    "FSQ Manager",
    "FSQ Supervisor",
    "FSQ Specialist",
    "FSQA Manager",
    "FSQA Supervisor",
    "FSQA Specialist",
    "Quality Supervisor",
    "Quality Assurance Manager",
    "Food Safety Manager",
    "Food Safety Supervisor",
]

# Extra terms to keep results in the food industry
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


def safe_apply_link(job: Dict[str, Any]) -> str:
    links = job.get("related_links") or []
    if links and isinstance(links, list):
        return links[0].get("link") or "N/A"
    return "N/A"


def safe_pay(job: Dict[str, Any]) -> str:
    # SerpAPI sometimes puts salary info in detected_extensions or extensions
    de = job.get("detected_extensions") or {}
    if isinstance(de, dict):
        sal = de.get("salary")
        if sal:
            return str(sal)

    # Sometimes salary appears inside job["extensions"] list
    ext = job.get("extensions") or []
    if isinstance(ext, list):
        for item in ext:
            if isinstance(item, str) and ("$" in item or "per" in item.lower() or "hour" in item.lower()):
                return item
    return "N/A"


def safe_time_posted(job: Dict[str, Any]) -> str:
    de = job.get("detected_extensions") or {}
    if isinstance(de, dict):
        posted = de.get("posted_at")
        if posted:
            return str(posted)

    # Sometimes appears in extensions too
    ext = job.get("extensions") or []
    if isinstance(ext, list):
        # common values: "3 days ago", "Just posted"
        for item in ext:
            if isinstance(item, str) and ("ago" in item.lower() or "posted" in item.lower() or "today" in item.lower()):
                return item
    return "N/A"


def normalize_row(job: Dict[str, Any]) -> Dict[str, str]:
    return {
        "title": job.get("title") or "N/A",
        "company_name": job.get("company_name") or "N/A",
        "pay": safe_pay(job),
        "time_posted": safe_time_posted(job),
        "location": job.get("location") or "N/A",
        "source": job.get("via") or "Unknown",
        "apply_link": safe_apply_link(job),
    }


def build_queries() -> List[str]:
    # Search each role with food context
    queries = []
    food_context = " OR ".join([f'"{x}"' for x in FOOD_CONTEXT])
    for role in ROLE_KEYWORDS:
        # Example: ("QA Manager") AND ("food manufacturing" OR "FSQA" OR ...)
        q = f'"{role}" ({food_context})'
        queries.append(q)
    return queries


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

    # Your required column order
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

    # Excel MIME type
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

Attached is your daily job report (Food Industry - Quality roles).
Total jobs found: {len(all_rows)}

Columns: title, company name, pay, time posted, location, source, link to apply

Regards,
Job Bot
"""
    send_email_with_attachment(subject, body, excel_file)


if __name__ == "__main__":
    main()
