import os
import pandas as pd
import urllib.parse
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from jobspy import scrape_jobs

# Configuration
QUERIES = [
    "Business Intelligence Analyst",
    "Economic Consulting Analyst",
    "AML Compliance Analyst",
    "Public Sector Analyst",
    "Operations Analyst"
]
SEEN_JOBS_FILE = "seen_jobs_master.csv"
DAILY_OUTPUT_FILE = "daily_leads.csv"

# SMTP Credentials from Environment Variables
SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com") # Defaults to Gmail
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASS = os.environ.get("SMTP_PASS")
RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL")

def generate_linkedin_url(company_name):
    """Generates a URL-encoded LinkedIn people search link."""
    if pd.isna(company_name) or not str(company_name).strip():
        return ""
    # We append "Recruiter" to the company name to narrow the search
    search_string = f"{company_name} Recruiter"
    encoded_string = urllib.parse.quote(search_string)
    return f"https://www.linkedin.com/search/results/people/?keywords={encoded_string}"

def main():
    print(f"Starting job scrape at {datetime.now()} UTC")
    
    # Load previously seen jobs for deduplication
    if os.path.exists(SEEN_JOBS_FILE):
        seen_df = pd.read_csv(SEEN_JOBS_FILE)
        seen_urls = set(seen_df['job_url'].dropna().tolist())
    else:
        seen_urls = set()
        
    all_new_jobs = []

    for query in QUERIES:
        print(f"Scraping for: {query}...")
        try:
            # Query multiple boards via jobspy
            jobs = scrape_jobs(
                site_name=["linkedin", "indeed", "glassdoor"],
                search_term=query,
                location="USA", # Modify this if targeting a specific city
                results_wanted=30, 
                hours_old=24,
                country_indeed="USA"
            )
            
            if not jobs.empty:
                # Deduplicate: Keep only jobs whose URL is NOT in seen_urls
                new_jobs = jobs[~jobs['job_url'].isin(seen_urls)].copy()
                all_new_jobs.append(new_jobs)
                print(f" -> Found {len(new_jobs)} net-new jobs for '{query}'.")
            else:
                print(f" -> No jobs found for '{query}' in the last 24h.")

        except Exception as e:
            # Catch rate limits or timeouts and continue to the next query
            print(f" -> ERROR scraping for '{query}': {e}")
            
    if not all_new_jobs:
        print("No net-new jobs found across any queries today. Exiting.")
        return

    # Combine all net-new jobs into a single DataFrame
    daily_leads = pd.concat(all_new_jobs, ignore_index=True)
    
    # Secondary deduplication: Remove duplicates gathered in today's run
    daily_leads.drop_duplicates(subset=['job_url'], inplace=True)
    
    # Data Augmentation: Add LinkedIn Recruiter search link
    daily_leads['LinkedIn_Recruiter_Search'] = daily_leads['company'].apply(generate_linkedin_url)
    
    # Save the daily payload to CSV
    daily_leads.to_csv(DAILY_OUTPUT_FILE, index=False)
    print(f"Saved {len(daily_leads)} total net-new leads to {DAILY_OUTPUT_FILE}.")
    
    # Update the persistent state tracker
    new_seen = daily_leads[['job_url']].copy()
    if os.path.exists(SEEN_JOBS_FILE):
        new_seen.to_csv(SEEN_JOBS_FILE, mode='a', header=False, index=False)
    else:
        new_seen.to_csv(SEEN_JOBS_FILE, index=False)
    
    # Dispatch Email
    if SMTP_USER and SMTP_PASS and RECIPIENT_EMAIL:
        send_email(DAILY_OUTPUT_FILE, len(daily_leads))
    else:
        print("WARNING: SMTP credentials not fully provided. Skipping email delivery.")

def send_email(file_path, job_count):
    print("Preparing to send email digest...")
    msg = MIMEMultipart()
    msg['From'] = SMTP_USER
    msg['To'] = RECIPIENT_EMAIL
    msg['Subject'] = f"Automated Lead Gen: {job_count} New Jobs Found"
    
    body = f"Attached is your daily digest of {job_count} net-new job leads spanning the last 24 hours.\n\nTime to network!"
    msg.attach(MIMEText(body, 'plain'))
    
    with open(file_path, 'rb') as f:
        attachment = MIMEApplication(f.read(), _subtype="csv")
        attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_path))
        msg.attach(attachment)
        
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls() # Secure the connection
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)
        server.quit()
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")

if __name__ == "__main__":
    main()
