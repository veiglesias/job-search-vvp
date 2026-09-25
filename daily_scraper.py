import os
import json
import pandas as pd
import urllib.parse
from datetime import datetime
import gspread
from jobspy import scrape_jobs
from triage import triage_jobs, BUCKET_LABELS

# Configuration
QUERIES = [
    "Business Intelligence Analyst",
    "Economic Consulting Analyst",
    "AML Compliance Analyst",
    "Public Sector Analyst",
    "Operations Analyst",
    "Data Analyst",
    "Forecasting Analyst",
    "Machine Learning Analyst",
    "Risk Analyst",
    "Regulatory Analyst",
    "Fraud Analyst",
    "Strategy Analyst",
    "Process Improvement Analyst",
    "Policy Analyst"
]
SEEN_JOBS_FILE = "seen_jobs_master.csv"
HITLIST_LIMIT = 100
EXPORT_DIR = "exports"
MAX_TRIAGE = int(os.environ.get("MAX_TRIAGE", "300"))  # cost guardrail
# LinkedIn descriptions need one extra request per job; turn off if LinkedIn starts blocking runs
LINKEDIN_DESCRIPTIONS = os.environ.get("LINKEDIN_DESCRIPTIONS", "true").lower() == "true"

def generate_linkedin_url(company_name):
    if pd.isna(company_name) or not str(company_name).strip():
        return ""
    search_string = f"{company_name} Recruiter"
    return f"https://www.linkedin.com/search/results/people/?keywords={urllib.parse.quote(search_string)}"

def generate_message(row):
    title = row.get('title', 'this role')
    company = row.get('company', 'your company')
    if pd.isna(title) or pd.isna(company):
        return ""

    return (f"Hi [Recruiter Name], I just submitted my application for the {title} role at {company}. "
            f"Given my M.S. in Business Analytics and background in forecasting and compliance, I believe I'd be a strong fit for your organization—"
            f"whether in this specific position or other data/operations roles your team is currently recruiting for. "
            f"I know you are busy, but I'd love to connect and introduce myself!")

def format_pay(row):
    lo, hi = row.get('min_amount'), row.get('max_amount')
    if pd.isna(lo) and pd.isna(hi):
        return ""
    fmt = lambda v: f"${v:,.0f}" if pd.notna(v) else "?"
    interval = row.get('interval')
    suffix = f"/{interval}" if pd.notna(interval) and interval else ""
    return f"{fmt(lo)}-{fmt(hi)}{suffix}" if pd.notna(lo) and pd.notna(hi) and lo != hi else f"{fmt(lo if pd.notna(lo) else hi)}{suffix}"

def classify_resume(title):
    title_lower = str(title).lower()
    
    # Data / Tech Bucket
    if any(word in title_lower for word in ['data', 'intelligence', 'analytics engineer', 'scientist', 'machine learning', 'bi']):
        return "Data PDF"
    
    # Compliance / Risk Bucket
    elif any(word in title_lower for word in ['compliance', 'aml', 'risk', 'fraud', 'regulatory', 'trust', 'crimes']):
        return "Compliance PDF"
    
    # Operations / Strategy Bucket
    elif any(word in title_lower for word in ['operations', 'consulting', 'strategy', 'business analyst', 'project']):
        return "Operations PDF"
    
    # Default Fallback
    else:
        return "Master / Evaluate"

def main():
    print(f"Starting job scrape at {datetime.now()} UTC")
    
    # Load Google Sheets Connection
    creds_json = os.environ.get("GCP_CREDENTIALS")
    sheet_id = os.environ.get("SHEET_ID")
    
    if not creds_json or not sheet_id:
        print("ERROR: Missing GCP_CREDENTIALS or SHEET_ID environment variables.")
        return

    creds_dict = json.loads(creds_json)
    gc = gspread.service_account_from_dict(creds_dict)
    sh = gc.open_by_key(sheet_id)
    ws_hitlist = sh.worksheet("Today's Hitlist")
    ws_vault = sh.worksheet("The Vault")

    # Load Deduplication State
    if os.path.exists(SEEN_JOBS_FILE):
        seen_df = pd.read_csv(SEEN_JOBS_FILE)
        seen_urls = set(seen_df['job_url'].dropna().tolist())
    else:
        seen_urls = set()
        
    all_new_jobs = []

    # Scrape
    for query in QUERIES:
        print(f"Scraping for: {query}...")
        try:
            jobs = scrape_jobs(
                site_name=["linkedin", "indeed", "glassdoor"],
                search_term=query,
                location="USA",
                results_wanted=25, # Pull ~125 total across 5 queries to have a healthy vault
                hours_old=24,
                country_indeed="USA",
                linkedin_fetch_description=LINKEDIN_DESCRIPTIONS
            )
            if not jobs.empty:
                new_jobs = jobs[~jobs['job_url'].isin(seen_urls)].copy()
                all_new_jobs.append(new_jobs)
        except Exception as e:
            print(f" -> ERROR scraping for '{query}': {e}")
            
    if not all_new_jobs:
        print("No net-new jobs found today. Exiting.")
        return

    daily_leads = pd.concat(all_new_jobs, ignore_index=True)
    daily_leads.drop_duplicates(subset=['job_url'], inplace=True)
    
    # Data Augmentation
    today_str = datetime.now().strftime('%Y-%m-%d')
    daily_leads['Date Added'] = today_str
    daily_leads['Recruiter Link'] = daily_leads['company'].apply(generate_linkedin_url)
    daily_leads['Outreach Template'] = daily_leads.apply(generate_message, axis=1)
    daily_leads['Resume Version'] = daily_leads['title'].apply(classify_resume)
    daily_leads['Status'] = 'New Lead'
    
    daily_leads['Pay'] = daily_leads.apply(format_pay, axis=1)
    for col in ['location', 'description', 'site']:
        if col not in daily_leads.columns:
            daily_leads[col] = ""

    # Filter out non-related, not qualified roles
    # We use \b to ensure we match whole words (so we don't accidentally ban "internal" when looking for "intern")
    forbidden_words = r'\b(?:senior|sr\.|sr|intern|internship|principal|lead|manager|director|vice-president|president|staff)\b'
    daily_leads = daily_leads[~daily_leads['title'].str.contains(forbidden_words, case=False, na=False, regex=True)]

    # Shuffle so ties (and untriaged rows) still get a mix of all queries
    daily_leads = daily_leads.sample(frac=1).reset_index(drop=True)

    # Claude triage: bucket, fit score, verdict
    daily_leads['Fit Score'] = ""
    daily_leads['Verdict'] = ""
    daily_leads['Why'] = ""
    daily_leads['Main Gap'] = ""
    to_score = daily_leads.head(MAX_TRIAGE)
    job_dicts = [
        {"title": r['title'], "company": r['company'], "location": r['location'],
         "pay": r['Pay'], "description": r['description']}
        for _, r in to_score.iterrows()
    ]
    results = triage_jobs(job_dicts) if job_dicts else None
    if results:
        for i, res in enumerate(results):
            if res.get('bucket') in BUCKET_LABELS:
                daily_leads.at[i, 'Resume Version'] = BUCKET_LABELS[res['bucket']]
            daily_leads.at[i, 'Fit Score'] = res.get('score', 0)
            daily_leads.at[i, 'Verdict'] = res.get('verdict', '')
            daily_leads.at[i, 'Why'] = res.get('reason', '')
            daily_leads.at[i, 'Main Gap'] = res.get('gap', '')
        # Best fits first, so the hitlist is the top of the day rather than a random draw
        daily_leads['_sort'] = pd.to_numeric(daily_leads['Fit Score'], errors='coerce').fillna(-1)
        daily_leads = daily_leads.sort_values('_sort', ascending=False, kind='stable').drop(columns='_sort').reset_index(drop=True)

    # Full daily export (with description text) for review or pasting into Claude
    os.makedirs(EXPORT_DIR, exist_ok=True)
    export = daily_leads[['Date Added', 'Fit Score', 'Verdict', 'Resume Version', 'title', 'company',
                          'location', 'Pay', 'Why', 'Main Gap', 'site', 'job_url', 'description']].copy()
    export['description'] = export['description'].fillna("").astype(str).str.slice(0, 2000)
    export_path = os.path.join(EXPORT_DIR, f"leads_{today_str}.csv")
    export.to_csv(export_path, index=False)
    print(f"Wrote {len(export)} rows to {export_path}")

    # Sheet columns: the original 8 stay in place (A-H); new ones are appended (I-O).
    # The description column lets Claude score rows straight from the sheet via the Google Sheets connector.
    daily_leads['Description'] = daily_leads['description'].fillna("").astype(str).str.slice(0, 1500)
    columns_to_keep = ['Date Added', 'company', 'title', 'Resume Version', 'job_url', 'Recruiter Link',
                       'Outreach Template', 'Status', 'Fit Score', 'Verdict', 'Why', 'Main Gap', 'location', 'Pay',
                       'Description']
    sheet_rows = daily_leads[columns_to_keep].fillna("").astype(str)

    hitlist_df = sheet_rows.head(HITLIST_LIMIT)
    vault_df = sheet_rows.iloc[HITLIST_LIMIT:]

    # Push to Google Sheets (Append to prevent overwriting)
    if not hitlist_df.empty:
        ws_hitlist.append_rows(hitlist_df.values.tolist())
        print(f"Appended {len(hitlist_df)} jobs to Today's Hitlist.")
        
    if not vault_df.empty:
        ws_vault.append_rows(vault_df.values.tolist())
        print(f"Appended {len(vault_df)} jobs to The Vault.")

    # Update local CSV state for tomorrow
    new_seen = daily_leads[['job_url']].copy()
    if os.path.exists(SEEN_JOBS_FILE):
        new_seen.to_csv(SEEN_JOBS_FILE, mode='a', header=False, index=False)
    else:
        new_seen.to_csv(SEEN_JOBS_FILE, index=False)
        
    print("Pipeline complete!")

if __name__ == "__main__":
    main()
