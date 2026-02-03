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
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")  # Gmail App Password
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
# ================================================

LOCATION = "United States"

# Your target roles
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

# Food-industry hints (used lightly)
FOOD_HINTS = [
    "food", "food manufacturing", "food processing", "HACCP",
    "FSQA", "GMP", "USDA", "FDA"
]

# Performance controls
NUM_PER_PAGE = 10
MAX_PAGES = 3                 # reduced from 6
MAX_RUNTIME_SECONDS = 240     # 4 minutes max
RETRY_STATUS = {429, 502, 503, 504}


def validate_env():
    if not SERPAPI_KEY:
        raise ValueError("SERPAPI_KEY missing (GitHub Secret).")
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        raise ValueError("EMAIL_SENDER / EMAIL_PASSWORD / EMAIL_RECEIVER missing (GitHub Secrets).")


def serpapi_google_jobs(query: str, location: str, start: int = 0, num: int = 10) -> List[Dict[str, Any]]:
    """
    Fetch jobs from SerpAPI Google Jobs with limited retries.
    """
    params = {
        "engine": "google_jobs",
        "q": query,
        "location": location,
        "api_key": SERPAPI_KEY,
        "num": num,
        "start": start,
    }

    # reduced retries to 3
    for attempt in range(1, 4):
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

    ext = job.get("extensions") or []
    if isinstance(ext, list):
        for item in ext:
            if isinstance(item, str) and ("$" in item or "hour" in item.lower() or "year" in item.lower()):
                return item
    return "N/A"


def safe_time_posted(job: Dict[str, Any]) -> str:
    de = job.get("detected_extensions") or {}
    if isinstance(de, dict) and de.get("posted_at"):
        return str(de["posted_at"])

    ext = job.get("extensions") or []
    if isinstance(ext, list):
        for item in ext:
            if isinstance(item, str) and ("ago" in item.lower() or "today" in item.lower() or "yesterday" in item.lower()):
                return item
    return "N/A"


def posted_days(time_posted: str) -> int:
    """
    Supports:
    - 'today', 'just posted'
    - '5 hours ago'
    - '3 days ago'
    - '1 week ago'
    """
    if not time_posted or time_posted == "N/A":
        return 999

    s = time_posted.strip().lower()
    if "just posted" in s or "today" in s:
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
    """
    Direct apply link if available, else empty string.
    """
    links = job.get("related_links") or []
    if isinstance(links, list) and links:
        link = links[0].get("link")
        if link and link.startswith("http"):
            return link
    return ""


def google_apply_search_link(title: str, company: str) -> str:
    """
    Always available fallback link (never N/A)
    """
    q = f'{title} {company} apply'
    return "https://www.google.com/search?q=" + urllib.parse.quote_plus(q)


def company_careers_search_link(company: str) -> str:
    """
    Always available fallback careers link (never N/A)
    """
    q = f"{company} careers"
    return "https://www.google.com/search?q=" + urllib.parse.quote_plus(q)


def looks_food_industry(job: Dict[str, Any]) -> bool:
    """
    Soft filter: only remove very irrelevant results.
    If food keywords are missing, we still keep it (to get more jobs).
    """
    text = " ".join([
        str(job.get("title") or ""),
        str(job.get("company_name") or ""),
        str(job.get("description") or ""),
    ]).lower()

    # If title contains QA/FSQA/Food Safety, keep it even if food hints missing
    title = str(job.get("title") or "").lower()
    must_keep_terms = ["qa", "quality", "fsqa", "food safety"]
    if any(t in title for t in must_keep_terms):
        return True

    return any(h.lower() in text for h in FOOD_HINTS)


def normalize_row(job: Dict[str, Any]) -> Dict[str, str]:
    title = job.get("title") or "N/A"
    company = job.get("company_name") or "N/A"

    apply_link = safe_apply_link(job)
    if not apply_link:
        apply_link = google_apply_search_link(title, company)

    return {
        "job_id": job.get("job_id") or f"{title}|{company}|{job.get('location','')}",
        "title": title,
        "company_name": company,
        "pay": safe_pay(job),
        "time_posted": safe_time_posted(job),
        "location": job.get("location") or "N/A",
        "source": job.get("via") or "Unknown",
        "apply_link": apply_link,
        "company_careers_link": company_careers_search_link(company),
    }


def build_queries() -> List[str]:
    """
    Reduced query set so it runs fast.
    Still strong coverage:
    - role + 'food'
    - role + 'HACCP'
    """
    queries = []
    for role in ROLE_KEYWORDS:
        queries.append(f'"{role}" food')
        queries.append(f'"{role}" HACCP')
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

    # Make links clickable
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
        f"Hi,\n\n"
        f"Attached is your Food Industry Quality/FSQA job report.\n"
        f"Jobs included (last 7 days, newest first): {total}\n\n"
        f"Links:\n"
        f"- apply_link: direct apply link when available; otherwise Google apply search link\n"
        f"- company_careers_link: Google careers search link\n"
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

    start_time = time.time()

    rows: List[Dict[str, str]] = []
    queries = build_queries()

    for q in queries:
        # Stop if we hit the runtime cap
        if time.time() - start_time > MAX_RUNTIME_SECONDS:
            break

        for page in range(MAX_PAGES):
            if time.time() - start_time > MAX_RUNTIME_SECONDS:
                break

            start = page * NUM_PER_PAGE
            jobs = serpapi_google_jobs(q, LOCATION, start=start, num=NUM_PER_PAGE)
            if not jobs:
                break

            for job in jobs:
                if looks_food_industry(job):
                    rows.append(normalize_row(job))

    rows = dedupe(rows)

    # last 7 days (includes hours ago)
    rows = [r for r in rows if posted_days(r.get("time_posted", "N/A")) <= 7]

    # newest first
    rows.sort(key=lambda r: posted_days(r.get("time_posted", "N/A")))

    today = datetime.now().strftime("%Y-%m-%d")
    excel_file = f"food_quality_jobs_{today}.xlsx"
    create_excel(rows, excel_file)
    send_email_with_attachment(excel_file, len(rows))


if __name__ == "__main__":
    main()
