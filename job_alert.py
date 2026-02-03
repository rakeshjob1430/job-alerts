import requests
import smtplib
import ssl
import json
from email.message import EmailMessage
from datetime import datetime

# ================= CONFIG =================
import os

SERPAPI_KEY = os.getenv("SERPAPI_KEY")

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")

SEARCH_QUERY = "Food Safety Supervisor"
LOCATION = "United States"

SEEN_JOBS_FILE = "seen_jobs.json"

# =========================================

def load_seen_jobs():
    try:
        with open(SEEN_JOBS_FILE, "r") as f:
            return set(json.load(f))
    except:
        return set()

def save_seen_jobs(jobs):
    with open(SEEN_JOBS_FILE, "w") as f:
        json.dump(list(jobs), f)

def send_email(job):
    msg = EmailMessage()
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER
    msg["Subject"] = f"🚨 NEW JOB: {job['title']}"

    body = f"""
New Job Posted!

Title: {job['title']}
Company: {job['company_name']}
Location: {job['location']}
Source: {job['via']}
Apply Link: {job['related_links'][0]['link']}

Posted at: {datetime.now()}
"""
    msg.set_content(body)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)

def check_jobs():
    seen_jobs = load_seen_jobs()

    params = {
        "engine": "google_jobs",
        "q": SEARCH_QUERY,
        "location": LOCATION,
        "api_key": SERPAPI_KEY
    }

    response = requests.get("https://serpapi.com/search", params=params)
    results = response.json()

    for job in results.get("jobs_results", []):
        job_id = job.get("job_id")

        if job_id not in seen_jobs:
            send_email(job)
            seen_jobs.add(job_id)

    save_seen_jobs(seen_jobs)

if __name__ == "__main__":
    check_jobs()


