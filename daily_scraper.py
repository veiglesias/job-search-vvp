"""
Daily job scraper -> Google Sheet CRM ("Today's Hitlist" + "The Vault").

Pipeline
  1. Search LinkedIn (primary) and Indeed (secondary).
     Tier 1 cities run every day; Tier 2 regions alternate between two groups
     with a 48-hour window, so every region is covered without doubling requests.
  2. Drop obvious non-fits (titles, active clearance, 4+ years required, LinkedIn seniority, ...).
  3. Remove duplicates: same company + cleaned-up title, within today's run and against past runs.
  4. Rank without an API key (résumé keyword match + entry-level signals + location tier).
  5. Top HITLIST_LIMIT rows -> Today's Hitlist, the rest -> The Vault.
     Everything that was filtered out goes to exports/filtered_<date>.csv with the reason.

Environment variables
  GCP_CREDENTIALS, SHEET_ID   required for sheet access
  DRY_RUN=true                search, filter and rank, but don't write to the sheet or the seen-jobs file
  TEST_SCOPE=tier1            only search the Tier 1 cities (fast test)
  TIER2_GROUP=A|B|both|none   override the day's Tier 2 rotation (default: alternates A/B by date)
  HITLIST_LIMIT=50            rows sent to Today's Hitlist
  BOOLEAN_QUERIES=true        one OR-joined search per query group (false = one search per term)
"""
import os
import json
import random
import re
import time
import urllib.parse
from collections import Counter
from datetime import datetime, date

import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
def _env_bool(name, default):
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes", "y")

DRY_RUN = _env_bool("DRY_RUN", False)
TEST_SCOPE = os.environ.get("TEST_SCOPE", "all").strip().lower()
HITLIST_LIMIT = int(os.environ.get("HITLIST_LIMIT", "50"))
BOOLEAN_QUERIES = _env_bool("BOOLEAN_QUERIES", True)
LINKEDIN_DESCRIPTIONS = _env_bool("LINKEDIN_DESCRIPTIONS", True)

SEEN_JOBS_FILE = "seen_jobs_master.csv"
EXPORT_DIR = "exports"
SHEET_DESC_CHARS = 4000        # description kept in the sheet (Sheets allows 50,000 per cell)
EXPORT_DESC_CHARS = 20000      # description kept in the daily export CSV

# Results requested per search call. LinkedIn fetches one extra page per job for the
# description, so it gets a smaller number and an overall budget.
RESULTS = {
    ("linkedin", 1): int(os.environ.get("LI_RESULTS_T1", "15")),
    ("linkedin", 2): int(os.environ.get("LI_RESULTS_T2", "8")),
    ("indeed", 1): int(os.environ.get("INDEED_RESULTS_T1", "40")),
    ("indeed", 2): int(os.environ.get("INDEED_RESULTS_T2", "25")),
}
LINKEDIN_JOB_BUDGET = int(os.environ.get("LI_MAX_JOBS", "450"))  # stop LinkedIn calls past this many jobs
LINKEDIN_EMPTY_LIMIT = 6        # this many empty LinkedIn results in a row -> assume blocked, stop LinkedIn
HOURS_OLD = {1: 24, 2: 48}      # Tier 2 runs every other day, so it looks back 48 hours

SITES = ["linkedin", "indeed"]  # Glassdoor dropped: it returned nothing in 400+ rows

# Search terms, grouped. With BOOLEAN_QUERIES each group is one OR-joined search.
QUERY_GROUPS = {
    "data": ["data analyst", "business intelligence analyst", "associate data scientist",
             "junior data scientist"],
    "forecasting": ["demand planning analyst", "demand planner", "forecasting analyst",
                    "revenue management analyst"],
    "compliance": ["AML analyst", "financial crimes analyst", "fraud analyst", "compliance analyst",
                   "third party risk analyst", "risk analyst"],
    "ops_strategy": ["operations analyst", "business operations analyst", "strategy analyst",
                     "economic consulting analyst", "rotational analyst program"],
    "policy": ["policy analyst", "legislative analyst"],
}
TIER1_ONLY_GROUPS = {"policy"}  # policy roles only where they fit (mainly DC)

# Locations. "location" can be one string for both sites or a per-site dict.
TIER1_AREAS = [
    {"name": "NYC / Jersey City / Hoboken", "location": "New York, NY", "distance": 25},
    {"name": "Chicago", "location": "Chicago, IL", "distance": 25},
    {"name": "Boston", "location": "Boston, MA", "distance": 25},
    {"name": "Washington DC", "location": "Washington, DC", "distance": 25},
]
TIER2_GROUPS = {
    "A": [  # Northeast, Mid-Atlantic, Illinois
        {"name": "New York State", "location": {"linkedin": "New York, United States", "indeed": "NY"}, "distance": 100},
        {"name": "New Jersey", "location": {"linkedin": "New Jersey, United States", "indeed": "NJ"}, "distance": 100},
        {"name": "Connecticut", "location": {"linkedin": "Connecticut, United States", "indeed": "CT"}, "distance": 100},
        {"name": "Massachusetts", "location": {"linkedin": "Massachusetts, United States", "indeed": "MA"}, "distance": 100},
        {"name": "Philadelphia", "location": "Philadelphia, PA", "distance": 25},
        {"name": "Pennsylvania", "location": {"linkedin": "Pennsylvania, United States", "indeed": "PA"}, "distance": 100},
        {"name": "Maryland", "location": {"linkedin": "Maryland, United States", "indeed": "MD"}, "distance": 100},
        {"name": "Virginia", "location": {"linkedin": "Virginia, United States", "indeed": "VA"}, "distance": 100},
        {"name": "Illinois", "location": {"linkedin": "Illinois, United States", "indeed": "IL"}, "distance": 100},
    ],
    "B": [  # Texas metros, Miami, Carolinas, Georgia, Colorado, remote
        {"name": "Austin", "location": "Austin, TX", "distance": 50},
        {"name": "San Antonio", "location": "San Antonio, TX", "distance": 50},
        {"name": "Houston", "location": "Houston, TX", "distance": 50},
        {"name": "Miami", "location": "Miami, FL", "distance": 50},
        {"name": "North Carolina", "location": {"linkedin": "North Carolina, United States", "indeed": "NC"}, "distance": 100},
        {"name": "South Carolina", "location": {"linkedin": "South Carolina, United States", "indeed": "SC"}, "distance": 100},
        {"name": "Georgia", "location": {"linkedin": "Georgia, United States", "indeed": "GA"}, "distance": 100},
        {"name": "Colorado", "location": {"linkedin": "Colorado, United States", "indeed": "CO"}, "distance": 100},
        {"name": "Remote (US)", "location": {"linkedin": "United States", "indeed": "Remote"}, "distance": 50, "remote": True},
    ],
}

# Jobs located in these states are kept; anything else is dropped unless it's remote.
TARGET_STATES = {"IL", "NY", "NJ", "CT", "MA", "PA", "DC", "MD", "VA", "TX", "FL", "NC", "SC", "GA", "CO"}

# ---------------------------------------------------------------------------
# Small helpers carried over from the original script
# ---------------------------------------------------------------------------
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

def clean_description(text):
    """Turn jobspy's markdown into plain, readable text."""
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    t = str(text)
    t = re.sub(r"\\([\\`*_{}\[\]()#+\-.!|>~])", r"\1", t)       # markdown escapes: data\-driven -> data-driven
    t = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", t)                # [link text](url) -> link text
    t = re.sub(r"^[ \t]*[*+\-•][ \t]+", "- ", t, flags=re.M)      # any bullet level -> "- "
    t = re.sub(r"(\*\*|__|\*|`)", "", t)                          # bold / italics / code marks
    t = re.sub(r"^\s*#+\s*", "", t, flags=re.M)                   # headings
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n\s*\n+", "\n", t)
    return t.strip()

def _is_blank(v):
    return v is None or (isinstance(v, float) and pd.isna(v)) or not str(v).strip() or str(v).strip().lower() == "nan"

# ---------------------------------------------------------------------------
# Résumé selector (unchanged): scores the posting against each résumé bucket.
# The winning score also feeds the ranking as a relevance signal.
# ---------------------------------------------------------------------------
BUCKET_TERMS = {
    "Data PDF": [
        "data analyst", "data analytics", "data scientist", "data science", "data engineer",
        "business intelligence", "bi", "power bi", "tableau", "looker", "dashboard", "dashboards",
        "sql", "python", "r programming", "pandas", "etl", "data pipeline", "data pipelines",
        "data model", "data modeling", "data models", "machine learning", "predictive",
        "forecasting", "forecast", "time series", "statistical", "statistics", "regression",
        "visualization", "visualizations", "analytics", "kpi", "kpis", "snowflake", "dbt",
        "a/b testing", "quantitative", "modeling", "key performance indicators", "metrics",
        "reporting", "insights", "data-driven", "data sources", "data quality", "excel",
        "large data sets", "large datasets", "data visualization",
    ],
    "Compliance PDF": [
        "compliance", "aml", "anti-money laundering", "bsa", "kyc", "know your customer",
        "cdd", "edd", "due diligence", "sanctions", "ofac", "fraud", "financial crimes",
        "financial crime", "transaction monitoring", "suspicious activity", "sar", "sars",
        "risk", "risk management", "regulatory", "regulation", "regulations", "audit",
        "auditing", "internal controls", "controls", "sox", "data governance", "governance",
        "policy", "policies", "privacy", "investigations", "investigation", "trust and safety",
        "examination", "examiner",
    ],
    "Operations PDF": [
        "operations", "operational", "business operations", "process improvement",
        "business process", "workflow", "workflows", "supply chain", "logistics", "procurement",
        "vendor", "vendors", "project management", "project coordination", "program",
        "stakeholder", "stakeholders", "strategy", "strategic", "consulting", "business analyst",
        "requirements", "lean", "six sigma", "capacity", "scheduling", "inventory",
        "onboarding", "implementation", "cross-functional", "efficiency", "operating model",
    ],
}
TITLE_WEIGHT = 5
GENERIC_TITLE_TERMS = {"business analyst", "strategy", "strategic", "program", "requirements", "analytics", "modeling"}
DESC_CAP = 3
MIN_SIGNAL = 3
CLOSE_MARGIN = 0.15

_BUCKET_PATTERNS = {
    b: [(t, re.compile(r"(?<![\w/])" + re.escape(t) + r"(?![\w/])", re.I)) for t in sorted(terms, key=len, reverse=True)]
    for b, terms in BUCKET_TERMS.items()
}

def _bucket_score(patterns, title, desc):
    score, used_spans_t, used_spans_d = 0, [], []
    for term, pat in patterns:
        title_weight = 1 if term in GENERIC_TITLE_TERMS else TITLE_WEIGHT
        for text, spans, weight, cap in ((title, used_spans_t, title_weight, 1), (desc, used_spans_d, 1, DESC_CAP)):
            hits = 0
            for m in pat.finditer(text):
                if any(a <= m.start() < b for a, b in spans):
                    continue
                spans.append(m.span())
                hits += 1
            score += weight * min(hits, cap)
    return score

def classify_resume(title, description="", return_scores=False):
    title = str(title or "")
    desc = str(description or "")
    claimed = {"data governance": "Compliance PDF", "risk analytics": "Compliance PDF",
               "fraud analytics": "Compliance PDF", "operations analytics": "Operations PDF"}
    scores = {b: _bucket_score(p, title, desc) for b, p in _BUCKET_PATTERNS.items()}
    for phrase, owner in claimed.items():
        n = len(re.findall(re.escape(phrase), title, re.I)) * TITLE_WEIGHT + min(len(re.findall(re.escape(phrase), desc, re.I)), DESC_CAP)
        if n:
            scores[owner] += n
    if return_scores:
        return scores
    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    (best, s1), (second, s2) = ranked[0], ranked[1]
    if s1 < MIN_SIGNAL:
        return "Master / Evaluate"
    if s2 >= s1 * (1 - CLOSE_MARGIN):
        return f"{best} (or {second.replace(' PDF', '')})"
    return best

# ---------------------------------------------------------------------------
# Filters: obvious non-fits
# ---------------------------------------------------------------------------
# Seniority words (from the original script). "Level II" / "Analyst 2" are kept on purpose.
SENIORITY_RE = re.compile(
    r"\b(?:senior|sr\.?|intern|internship|principal|lead|manager|director|vice[- ]president|president|staff|iii|iv)\b"
    r"|\b(?:analyst|associate|specialist|level)\s*[34]\b", re.I)
# Jobs that aren't in your field at all, even though the title says "analyst" or similar.
OFF_FIELD_TITLE_RE = re.compile(
    r"\b(?:behaviou?r|bcba|bcaba|rbt|technician|technologist|nurse|nursing|lpn|doula|therapist|chemist|welder|"
    r"architect|developer|help ?desk|paralegal|driver|journeyman|supervisor|social worker|slpa|co-?op|"
    r"account executive|sales representative)\b", re.I)
EXEC_TITLE_RE = re.compile(r"\b(?:avp|vp|svp|evp|officer)\b|(?<!business )\bpartner\b", re.I)
MID_TITLE_RE = re.compile(r"\bmid\b(?![- ]market)|\bmid[- ]level\b", re.I)
ENGINEER_RE = re.compile(r"\bengineer(?:ing)?\b", re.I)
DATA_ENGINEER_RE = re.compile(r"\b(?:data|bi|analytics|business intelligence|intelligence|reporting)\s+engineer", re.I)

LINKEDIN_EXCLUDED_LEVELS = {"mid-senior level", "director", "executive", "internship"}

YEARS_RE = re.compile(
    r"(?<![\d.$])(\d{1,2})\s*(?:\+|plus)?\s*(?:(?:-|–|to)\s*\d{1,2}\s*)?\+?\s*(?:years?|yrs?)(?:'s)?\s+"
    r"(?:of\s+)?(?:[\w/&,.()-]+\s+){0,6}?(?:experience|exp\b)", re.I)
PREFERRED_RE = re.compile(r"prefer|nice to have|a plus|desired|ideally|bonus|advantage", re.I)
CLEARANCE_RE = re.compile(
    r"ts\s*/\s*sci|top\s+secret|secret\s+(?:security\s+)?clearance|security\s+clearance|polygraph|clearance\s+required", re.I)
RESIDENTS_ONLY_RE = re.compile(r"\bresidents?\s+only\b", re.I)

def required_years(desc):
    """(minimum years stated as required, minimum years stated as preferred). None if not stated."""
    req, pref = [], []
    for m in YEARS_RE.finditer(desc):
        n = int(m.group(1))
        if n > 12:                      # "100 years of experience" is company boilerplate, not a requirement
            continue
        window = desc[max(0, m.start() - 120): m.end() + 60]
        (pref if PREFERRED_RE.search(window) else req).append(n)
    return (min(req) if req else None), (min(pref) if pref else None)

def needs_active_clearance(desc):
    for m in CLEARANCE_RE.finditer(desc):
        before = desc[max(0, m.start() - 100): m.start()].lower()
        after = desc[m.end(): m.end() + 40].lower()
        window = before + m.group(0).lower() + after
        if re.search(r"\bnone|not required|n/a|\bno clearance", after[:30]) or re.search(r"\bnone|not required", before[-30:]):
            continue                                        # "Clearance Required: None", "Type: None/Not Required"
        if re.search(r"mbi|public trust|background|suitability", after):
            continue                                        # background checks aren't clearances
        if re.search(r"desired|preferred|a plus", before[-40:] + after):
            continue                                        # nice-to-have, not a gate
        if re.search(r"obtain|eligib|ability to|able to|willing", window) and not re.search(r"\bactive\b|\bcurrent\b", window):
            continue                                        # "must be able to obtain": fine, you're a citizen
        if re.search(r"\bactive\b|\bcurrent\b|required|must (?:have|hold|possess)|\bhold\b|ts\s*/\s*sci|polygraph", window):
            return True
    return False

def filter_reason(row):
    """Why this posting is an obvious non-fit, or None to keep it. Citizenship requirements are NOT filtered."""
    title = str(row.get("title") or "")
    desc = str(row.get("description") or "")
    if _is_blank(row.get("company")):
        return "no company name"
    if SENIORITY_RE.search(title):
        return "seniority in title"
    if OFF_FIELD_TITLE_RE.search(title):
        return "off-field title"
    if re.search(r"\bclearance\b|\bts\s*/\s*sci\b", title, re.I):
        return "active clearance required"                  # "Data Analyst with Security Clearance"
    if EXEC_TITLE_RE.search(title) or MID_TITLE_RE.search(title):
        return "seniority in title"
    if ENGINEER_RE.search(title) and not DATA_ENGINEER_RE.search(title):
        return "engineering role"
    level = str(row.get("job_level") or "").strip().lower()
    if level in LINKEDIN_EXCLUDED_LEVELS:
        return f"LinkedIn level: {level}"
    if needs_active_clearance(desc):
        return "active clearance required"
    req, _ = required_years(desc)
    if req is not None and req >= 4:
        return f"{req}+ years required"
    if RESIDENTS_ONLY_RE.search(desc):
        return "state residents only"
    if not row.get("in_target_area", True):
        return "outside target areas"
    return None

# ---------------------------------------------------------------------------
# Location tiers
# ---------------------------------------------------------------------------
STATE_NAMES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA", "colorado": "CO",
    "connecticut": "CT", "delaware": "DE", "district of columbia": "DC", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD", "massachusetts": "MA",
    "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
}
STATE_CODES = set(STATE_NAMES.values())
TIER1_CITY_RE = re.compile(
    r"\b(?:new york|manhattan|brooklyn|queens|bronx|long island city|jersey city|hoboken|chicago|boston|cambridge)\b", re.I)

def parse_state(location):
    loc = str(location or "")
    for code in re.findall(r"(?:^|,)\s*([A-Z]{2})\b", loc):
        if code in STATE_CODES or code == "DC":
            return code
    low = loc.lower()
    if "district of columbia" in low or re.search(r"\bwashington,?\s*d\.?c\b", low):
        return "DC"
    for name in sorted(STATE_NAMES, key=len, reverse=True):   # "west virginia" before "virginia"
        if re.search(r"\b" + re.escape(name) + r"\b", low):
            return STATE_NAMES[name]
    return None

def location_tier(location, is_remote, search_tier):
    """1 = focus city, 1.5 = within a focus city's search radius, 2 = wider target area, 3 = remote, 4 = unknown."""
    state = parse_state(location)
    if state == "DC" or (TIER1_CITY_RE.search(str(location or "")) and state in {"NY", "NJ", "IL", "MA"}):
        return 1
    if is_remote and state is None:
        return 3
    if search_tier == 1:
        return 1.5
    if state in TARGET_STATES:
        return 2
    if is_remote:
        return 3
    return 4

# ---------------------------------------------------------------------------
# Ranking (no API key)
# ---------------------------------------------------------------------------
# "Associate" only counts when it leads the role ("Associate Data Scientist"), not bank-style
# "Credit Risk, Associate", where Associate is a mid-level rank above Analyst.
ENTRY_TITLE_RE = re.compile(
    r"\b(?:junior|jr\.?|entry[- ]level|early career|early talent|rotational|new grad|graduate|program|"
    r"analyst (?:i|1)|associate (?:consultant|analyst|data)|associate (?!director|vice|manager|principal)[a-z]+(?: [a-z]+)*? (?:analyst|scientist|consultant|specialist))\b"
    r"|^associate (?!director|vice|manager|principal)[a-z]", re.I)
ENTRY_DESC_RE = re.compile(r"recent grad|new grad|entry[- ]level|early[- ]career|0\s*(?:-|–|to)\s*[12]\s*years?", re.I)
AGGREGATOR_RE = re.compile(
    r"talenthop|jobright|remotehunter|swooped|wiraa|vetjobs|jobot|secondwind|meeboss|talentally|"
    r"placement group|funtorecruit|netrolynx|jobs via|dice\b|cybercoders", re.I)
LOCATION_POINTS = {1: 15, 1.5: 12, 2: 8, 3: 4, 4: 0}
LEVEL_POINTS = {"entry level": 10, "associate": 6}

def rank_row(row):
    title = str(row.get("title") or "")
    desc = str(row.get("description") or "")
    relevance = min(max(classify_resume(title, desc, return_scores=True).values()), 40)
    score = relevance
    if ENTRY_TITLE_RE.search(title):
        score += 12
    elif ENTRY_DESC_RE.search(desc):
        score += 5
    score += LEVEL_POINTS.get(str(row.get("job_level") or "").strip().lower(), 0)
    score += LOCATION_POINTS.get(row.get("loc_tier", 4), 0)
    if row.get("site") == "linkedin":
        score += 3
    req, pref = required_years(desc)
    if req is not None and req >= 2:
        score -= 8
    elif pref is not None and pref >= 3:
        score -= 4
    if AGGREGATOR_RE.search(str(row.get("company") or "")):
        score -= 10
    return int(round(score))

# ---------------------------------------------------------------------------
# Key requirements: the lines that matter for fit, pulled out so they never get cut off
# ---------------------------------------------------------------------------
REQ_LINE_RE = re.compile(
    r"\d+\+?\s*(?:-|–|to)?\s*\d*\+?\s*(?:years?|yrs?)|bachelor|master'?s|degree|clearance|public trust|"
    r"polygraph|licen[cs]e|certif|citizen|on-?site|hybrid|in[- ]office|days (?:a|per) week|travel", re.I)

def key_requirements(desc, max_chars=700):
    parts = re.split(r"(?<=[.;!?])\s+|\n", str(desc or ""))
    picked, seen = [], set()
    for p in parts:
        p = p.strip(" -•\t")
        if 12 <= len(p) <= 260 and REQ_LINE_RE.search(p):
            k = p.lower()
            if k not in seen:
                seen.add(k)
                picked.append(p)
    out = " | ".join(picked)
    return out[:max_chars].rstrip() + ("…" if len(out) > max_chars else "")

# ---------------------------------------------------------------------------
# Duplicates: company + cleaned-up title
# ---------------------------------------------------------------------------
COMPANY_SUFFIX_RE = re.compile(r"\b(?:inc|llc|l\.l\.c|ltd|corp|corporation|co|company|group|plc|lp|llp|pllc|na|the)\b\.?", re.I)
TITLE_NOISE_RE = re.compile(r"\b(?:remote|hybrid|on-?site|work (?:from|at) home|wfh|full[- ]time|part[- ]time|contract)\b", re.I)

def norm_company(c):
    c = re.sub(r"\(.*?\)", " ", str(c or "").lower())
    c = COMPANY_SUFFIX_RE.sub(" ", c)
    return re.sub(r"[^a-z0-9]+", " ", c).strip()

def norm_title(t):
    t = str(t or "").lower()
    t = re.sub(r"\(.*?\)|\[.*?\]", " ", t)                    # "(Remote)", "(Hybrid)"
    t = re.sub(r"\b[a-z]*\d{4,}[a-z0-9-]*\b", " ", t)          # requisition numbers like JR2026517219, 540174
    segments = re.split(r"\s+[-–|]\s+", t)
    keep = [s for s in segments if not re.fullmatch(r"[\w .']+,\s*[a-z]{2}", s.strip())      # "Tampa, FL"
            and not TITLE_NOISE_RE.fullmatch(s.strip())]
    t = " ".join(keep) if keep else t
    t = TITLE_NOISE_RE.sub(" ", t)
    return re.sub(r"[^a-z0-9]+", " ", t).strip()

def dedupe_key(company, title):
    return f"{norm_company(company)}|{norm_title(title)}"

# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------
def build_queries():
    """[(group, search_term)] — one OR-joined term per group, or one per term."""
    out = []
    for group, terms in QUERY_GROUPS.items():
        if BOOLEAN_QUERIES:
            out.append((group, " OR ".join(f'"{t}"' for t in terms)))
        else:
            out.extend((group, t) for t in terms)
    return out

def tier2_group_for_today():
    override = os.environ.get("TIER2_GROUP", "").strip().upper()
    if override in ("A", "B"):
        return [override]
    if override == "BOTH":
        return ["A", "B"]
    if override == "NONE":
        return []
    return ["A" if date.today().toordinal() % 2 == 0 else "B"]

def build_search_plan():
    """Ordered list of search calls. Tier 1 first, so the LinkedIn budget always covers it."""
    queries = build_queries()
    plan = []
    for area in TIER1_AREAS:
        for group, term in queries:
            plan.append({"tier": 1, "area": area, "group": group, "term": term})
    if TEST_SCOPE != "tier1":
        tier2 = []
        for g in tier2_group_for_today():
            for area in TIER2_GROUPS[g]:
                for group, term in queries:
                    if group not in TIER1_ONLY_GROUPS:
                        tier2.append({"tier": 2, "area": area, "group": group, "term": term})
        random.Random(date.today().toordinal()).shuffle(tier2)  # if the budget runs out, a different slice is cut each day
        plan.extend(tier2)
    return plan

def run_searches(plan):
    from jobspy import scrape_jobs
    frames, stats = [], Counter()
    li_jobs, li_empty_streak, li_blocked = 0, 0, False
    for call in plan:
        area = call["area"]
        for site in SITES:
            if site == "linkedin" and (li_blocked or li_jobs >= LINKEDIN_JOB_BUDGET):
                stats["linkedin calls skipped (budget/blocked)"] += 1
                continue
            loc = area["location"][site] if isinstance(area["location"], dict) else area["location"]
            kwargs = dict(site_name=[site], search_term=call["term"], location=loc, distance=area["distance"],
                          results_wanted=RESULTS[(site, call["tier"])], hours_old=HOURS_OLD[call["tier"]],
                          country_indeed="USA", verbose=0)
            if site == "linkedin":
                kwargs["linkedin_fetch_description"] = LINKEDIN_DESCRIPTIONS
                if area.get("remote"):
                    kwargs["is_remote"] = True   # LinkedIn allows remote + hours_old together; Indeed doesn't
            try:
                jobs = scrape_jobs(**kwargs)
            except Exception as e:
                print(f" -> ERROR {site} | {area['name']} | {call['group']}: {e}")
                jobs = pd.DataFrame()
                if site == "linkedin" and "429" in str(e):
                    li_blocked = True
            n = 0 if jobs is None else len(jobs)
            stats[f"{site} jobs"] += n
            stats[f"{site} calls"] += 1
            if site == "linkedin":
                li_jobs += n
                li_empty_streak = li_empty_streak + 1 if n == 0 else 0
                if li_empty_streak >= LINKEDIN_EMPTY_LIMIT:
                    print(" -> LinkedIn returned nothing several times in a row; assuming it's blocking this run.")
                    li_blocked = True
                time.sleep(random.uniform(1.5, 3.5))
            if n:
                jobs = jobs.copy()
                jobs["search_tier"] = call["tier"]
                jobs["search_area"] = area["name"]
                jobs["search_group"] = call["group"]
                jobs["site"] = site
                frames.append(jobs)
            print(f"{site:8} | T{call['tier']} {area['name']:28} | {call['group']:12} | {n} jobs")
    raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return raw, stats

# ---------------------------------------------------------------------------
# Processing: filter -> rank -> dedupe (pure pandas, no network; easy to test)
# ---------------------------------------------------------------------------
def process(raw, seen_urls=frozenset(), seen_keys=frozenset()):
    """Returns (kept, filtered). kept is ranked best-first; filtered has a 'Filter Reason' column."""
    df = raw.copy()
    for col in ["title", "company", "location", "description", "job_url", "site", "job_level", "is_remote",
                "search_tier", "search_area", "search_group", "min_amount", "max_amount", "interval"]:
        if col not in df.columns:
            df[col] = None
    df = df[~df["job_url"].isin(seen_urls)].drop_duplicates(subset=["job_url"]).copy()
    df["description"] = df["description"].apply(clean_description)
    df["search_tier"] = pd.to_numeric(df["search_tier"], errors="coerce").fillna(2).astype(int)
    df["is_remote_flag"] = df.apply(
        lambda r: bool(r["is_remote"]) if isinstance(r["is_remote"], bool)
        else bool(re.search(r"\bremote\b", f"{r['location']} {r['title']}", re.I)), axis=1)
    df["state"] = df["location"].apply(parse_state)
    df["in_target_area"] = df.apply(
        lambda r: r["is_remote_flag"] or r["state"] is None or r["state"] in TARGET_STATES
        or bool(TIER1_CITY_RE.search(str(r["title"]))), axis=1)   # "Las Vegas or Jersey City" in the title
    df["loc_tier"] = df.apply(lambda r: location_tier(r["location"], r["is_remote_flag"], r["search_tier"]), axis=1)
    df["dedupe_key"] = df.apply(lambda r: dedupe_key(r["company"], r["title"]), axis=1)

    df["Filter Reason"] = df.apply(lambda r: filter_reason(r.to_dict()), axis=1)
    filtered = df[df["Filter Reason"].notna()].copy()
    kept = df[df["Filter Reason"].isna()].copy()

    kept["Rank"] = kept.apply(lambda r: rank_row(r.to_dict()), axis=1)
    kept = kept.sort_values(["Rank", "loc_tier"], ascending=[False, True], kind="stable")

    # Same job already sent on an earlier day
    repeat = kept["dedupe_key"].isin(seen_keys)
    rep = kept[repeat].copy()
    rep["Filter Reason"] = "duplicate of an earlier day"
    kept = kept[~repeat]

    # Same job several times today (other board / other city): keep the best-ranked, note the other places
    also = kept.groupby("dedupe_key")["location"].apply(lambda s: [str(x) for x in s.iloc[1:] if not _is_blank(x)])
    dup_mask = kept.duplicated(subset=["dedupe_key"], keep="first")
    dups = kept[dup_mask].copy()
    dups["Filter Reason"] = "duplicate within today's run"
    kept = kept[~dup_mask].copy()
    def with_also(r):
        others = [o for o in dict.fromkeys(also.get(r["dedupe_key"], [])) if o != str(r["location"])]
        base = "" if _is_blank(r["location"]) else str(r["location"])
        return base + (f" (also in: {'; '.join(others[:3])})" if others else "")
    kept["location"] = kept.apply(with_also, axis=1)

    filtered = pd.concat([filtered, rep, dups], ignore_index=True)
    return kept.reset_index(drop=True), filtered.reset_index(drop=True)

# ---------------------------------------------------------------------------
# Sheet + state
# ---------------------------------------------------------------------------
SHEET_COLUMNS = ['Date Added', 'company', 'title', 'Resume Version', 'job_url', 'Recruiter Link',
                 'Outreach Template', 'Status', 'Fit Score', 'Verdict', 'Why', 'Main Gap', 'location', 'Pay',
                 'Description', 'Key Requirements', 'Rank', 'Source']
NEW_HEADERS = ["Key Requirements", "Rank", "Source"]   # columns P, Q, R

def ensure_headers(ws):
    header = ws.row_values(1)
    if len(header) < 18 or header[15:18] != NEW_HEADERS:
        ws.update(range_name="P1:R1", values=[NEW_HEADERS])

def load_seen():
    if not os.path.exists(SEEN_JOBS_FILE):
        return pd.DataFrame(columns=["job_url", "dedupe_key"])
    seen = pd.read_csv(SEEN_JOBS_FILE, dtype=str)
    if "dedupe_key" not in seen.columns:
        seen["dedupe_key"] = ""
    return seen[["job_url", "dedupe_key"]].fillna("")

def sheet_keys(*worksheets):
    """Company+title keys already in the sheet, so reposts with new URLs are caught from day one."""
    keys = set()
    for ws in worksheets:
        companies, titles = ws.col_values(2)[1:], ws.col_values(3)[1:]
        keys.update(dedupe_key(c, t) for c, t in zip(companies, titles) if c or t)
    return keys

def main():
    print(f"Starting job scrape at {datetime.now()} UTC | dry run: {DRY_RUN} | scope: {TEST_SCOPE}")
    today_str = datetime.now().strftime('%Y-%m-%d')

    ws_hitlist = ws_vault = None
    creds_json, sheet_id = os.environ.get("GCP_CREDENTIALS"), os.environ.get("SHEET_ID")
    if creds_json and sheet_id:
        import gspread
        gc = gspread.service_account_from_dict(json.loads(creds_json))
        sh = gc.open_by_key(sheet_id)
        ws_hitlist, ws_vault = sh.worksheet("Today's Hitlist"), sh.worksheet("The Vault")
    elif not DRY_RUN:
        print("ERROR: Missing GCP_CREDENTIALS or SHEET_ID environment variables.")
        return

    seen = load_seen()
    seen_urls = set(seen["job_url"])
    seen_keys = set(k for k in seen["dedupe_key"] if k)
    if ws_hitlist is not None:
        seen_keys |= sheet_keys(ws_hitlist, ws_vault)

    plan = build_search_plan()
    print(f"Search plan: {len(plan)} searches x {len(SITES)} sites | Tier 2 group(s) today: {tier2_group_for_today() if TEST_SCOPE != 'tier1' else 'skipped'}")
    raw, stats = run_searches(plan)
    if raw.empty:
        print("No jobs returned today. Exiting.")
        return

    kept, filtered = process(raw, seen_urls, seen_keys)

    # Sheet fields
    kept["Date Added"] = today_str
    kept["Recruiter Link"] = kept["company"].apply(generate_linkedin_url)
    kept["Outreach Template"] = kept.apply(generate_message, axis=1)
    kept["Resume Version"] = kept.apply(lambda r: classify_resume(r["title"], r["description"]), axis=1)
    kept["Status"] = "New Lead"
    kept["Pay"] = kept.apply(format_pay, axis=1)
    for col in ["Fit Score", "Verdict", "Why", "Main Gap"]:
        kept[col] = ""
    kept["Key Requirements"] = kept["description"].apply(key_requirements)
    kept["Source"] = kept.apply(lambda r: f"{r['site']} · {r['search_group']} · {r['search_area']}", axis=1)
    kept["Description"] = kept["description"].fillna("").astype(str).str.slice(0, SHEET_DESC_CHARS)

    # Optional: Claude triage of the hitlist if an API key is ever added (skipped otherwise)
    if os.environ.get("ANTHROPIC_API_KEY"):
        try:
            from triage import triage_jobs, BUCKET_LABELS
            top = kept.head(HITLIST_LIMIT)
            results = triage_jobs([{"title": r["title"], "company": r["company"], "location": r["location"],
                                    "pay": r["Pay"], "description": r["description"]} for _, r in top.iterrows()])
            for i, res in enumerate(results or []):
                if res.get("bucket") in BUCKET_LABELS:
                    kept.at[i, "Resume Version"] = BUCKET_LABELS[res["bucket"]]
                kept.at[i, "Fit Score"] = res.get("score", "")
                kept.at[i, "Verdict"] = res.get("verdict", "")
                kept.at[i, "Why"] = res.get("reason", "")
                kept.at[i, "Main Gap"] = res.get("gap", "")
        except Exception as e:
            print(f"Triage skipped: {e}")

    # Exports: full ranked list + everything filtered out (with the reason), for weekly review
    os.makedirs(EXPORT_DIR, exist_ok=True)
    prefix = "dryrun_" if DRY_RUN else ""
    export = kept[["Date Added", "Rank", "Resume Version", "title", "company", "location", "Pay", "site",
                   "job_level", "search_area", "search_group", "Key Requirements", "job_url", "description"]].copy()
    export["description"] = export["description"].fillna("").astype(str).str.slice(0, EXPORT_DESC_CHARS)
    export.to_csv(os.path.join(EXPORT_DIR, f"{prefix}leads_{today_str}.csv"), index=False)
    filtered[["Filter Reason", "title", "company", "location", "site", "job_level", "search_area",
              "search_group", "job_url"]].to_csv(os.path.join(EXPORT_DIR, f"{prefix}filtered_{today_str}.csv"), index=False)

    # Summary
    print("\n=== Summary ===")
    for k, v in sorted(stats.items()):
        print(f"{k}: {v}")
    print(f"raw jobs: {len(raw)} | kept: {len(kept)} | filtered/duplicates: {len(filtered)}")
    for reason, n in Counter(filtered["Filter Reason"]).most_common():
        print(f"  {n:4}  {reason}")
    print("\nTop of today's hitlist:")
    for _, r in kept.head(HITLIST_LIMIT).iterrows():
        print(f"{r['Rank']:4} | {str(r['title'])[:55]:55} | {str(r['company'])[:28]:28} | {str(r['location'])[:30]:30} | {r['site']}")

    if DRY_RUN:
        print("\nDry run: nothing written to the sheet or the seen-jobs file.")
        return

    sheet_rows = kept[SHEET_COLUMNS].fillna("").astype(str)
    hitlist_df, vault_df = sheet_rows.head(HITLIST_LIMIT), sheet_rows.iloc[HITLIST_LIMIT:]
    ensure_headers(ws_hitlist)
    ensure_headers(ws_vault)
    if not hitlist_df.empty:
        ws_hitlist.append_rows(hitlist_df.values.tolist())
        print(f"Appended {len(hitlist_df)} jobs to Today's Hitlist.")
    if not vault_df.empty:
        ws_vault.append_rows(vault_df.values.tolist())
        print(f"Appended {len(vault_df)} jobs to The Vault.")

    # Remember every URL and company+title we processed today (kept or filtered), so none come back
    processed = pd.concat([kept[["job_url", "dedupe_key"]], filtered[["job_url", "dedupe_key"]]], ignore_index=True)
    pd.concat([seen, processed.fillna("")], ignore_index=True).drop_duplicates().to_csv(SEEN_JOBS_FILE, index=False)
    print("Pipeline complete!")

if __name__ == "__main__":
    main()
