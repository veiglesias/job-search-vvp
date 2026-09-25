"""Score scraped job postings against the three résumé buckets using Claude.

Each posting gets: bucket (Operations / Data / Compliance), a 1-10 fit score,
a verdict (Apply / Stretch / Skip), a one-line reason, and the main gap.

Résumé text is read from the RESUME_O, RESUME_D and RESUME_C environment
variables (set them as GitHub secrets so they never live in this public repo),
falling back to resumes/resume_{O,D,C}.txt for local runs.
"""
import os
import concurrent.futures as cf

import anthropic

MODEL = os.environ.get("TRIAGE_MODEL", "claude-haiku-4-5-20251001")
MAX_WORKERS = int(os.environ.get("TRIAGE_WORKERS", "4"))
DESCRIPTION_CHARS = 2000

BUCKET_LABELS = {"O": "Operations PDF", "D": "Data PDF", "C": "Compliance PDF"}

RUBRIC = """You are a candid career advisor triaging job postings for one candidate.
The candidate is an early-career applicant (M.S. Business Analytics, May 2026) who
applies at volume, so honest scores matter more than encouragement: an inflated
score wastes an application.

The candidate has three versions of the same résumé, each framing the same
experience for a different track. Pick the ONE version that best matches the
posting and score fit against that version.

Score five dimensions 1-10, then combine with these weights:
- Hard requirements (30%): required degrees, licenses/certifications, years of
  experience, required tools. Preferred/"nice to have" items are not hard requirements.
- Relevant experience (30%): has the candidate done the core responsibilities?
- Seniority match (15%): entry-level / 0-3 years fits; roles demanding 5+ years score low.
- Domain / industry (15%): familiarity with the industry or function.
- Logistics (10%): score 7 by default; lower it only for things the posting states
  that commonly block applicants (security clearance required, polygraph, a required
  professional license, citizenship requirement). Never assume the candidate's
  citizenship or clearance status; name the requirement in the gap instead.

Caps (apply after the weighted average):
- Missing a truly required license, certification or clearance: max 4.
- Missing 2+ hard requirements: max 5.
- A years-of-experience ask within ~1-2 years of the candidate's is a soft gap, not a miss.
Give credit for transferable skills (e.g. forecasting in one industry applies in another)
while scoring domain lower.

Verdict: 7-10 = "Apply", 5-6 = "Stretch", 1-4 = "Skip".
If the description is missing or very short, score from the title and company only,
cap the score at 6, and start the reason with "Title only:".

Keep "reason" and "gap" under 20 words each, concrete and tied to the posting.
Always answer by calling the record_triage tool."""

TOOL = {
    "name": "record_triage",
    "description": "Record the triage result for one job posting.",
    "input_schema": {
        "type": "object",
        "properties": {
            "bucket": {"type": "string", "enum": ["O", "D", "C"],
                       "description": "Best-matching résumé: O=Operations, D=Data, C=Compliance"},
            "score": {"type": "integer", "minimum": 1, "maximum": 10},
            "verdict": {"type": "string", "enum": ["Apply", "Stretch", "Skip"]},
            "reason": {"type": "string", "description": "Main reason for the score"},
            "gap": {"type": "string", "description": "Biggest gap or risk, or 'None'"},
        },
        "required": ["bucket", "score", "verdict", "reason", "gap"],
    },
}


def load_resumes():
    resumes = {}
    for b in "ODC":
        text = os.environ.get(f"RESUME_{b}")
        path = os.path.join("resumes", f"resume_{b}.txt")
        if not text and os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                text = f.read()
        if text:
            resumes[b] = text.strip()
    return resumes


def build_system(resumes):
    blocks = [RUBRIC]
    names = {"O": "OPERATIONS", "D": "DATA", "C": "COMPLIANCE"}
    for b, text in resumes.items():
        blocks.append(f"<resume version=\"{b}\" track=\"{names[b]}\">\n{text}\n</resume>")
    # Cache the static part (rubric + résumés) so each posting only pays for its own text.
    return [{"type": "text", "text": "\n\n".join(blocks), "cache_control": {"type": "ephemeral"}}]


def _clean(value):
    return "" if value is None or str(value).lower() == "nan" else str(value).strip()


def score_one(client, system, job):
    description = _clean(job.get("description"))[:DESCRIPTION_CHARS]
    posting = (
        f"Title: {_clean(job.get('title'))}\n"
        f"Company: {_clean(job.get('company'))}\n"
        f"Location: {_clean(job.get('location'))}\n"
        f"Pay: {_clean(job.get('pay')) or 'not listed'}\n\n"
        f"Description:\n{description or '(no description available)'}"
    )
    msg = client.messages.create(
        model=MODEL,
        max_tokens=400,
        system=system,
        tools=[TOOL],
        tool_choice={"type": "tool", "name": "record_triage"},
        messages=[{"role": "user", "content": posting}],
    )
    for block in msg.content:
        if block.type == "tool_use":
            return block.input
    raise ValueError("No triage result returned")


def triage_jobs(jobs):
    """Take a list of job dicts; return a list of result dicts in the same order.

    Returns None (and the pipeline falls back to its old behaviour) when the API
    key or the résumés are missing.
    """
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("Triage skipped: ANTHROPIC_API_KEY not set.")
        return None
    resumes = load_resumes()
    if not resumes:
        print("Triage skipped: no résumé text found (RESUME_O/D/C secrets or resumes/*.txt).")
        return None

    client = anthropic.Anthropic(max_retries=4, timeout=60)
    system = build_system(resumes)
    results = [None] * len(jobs)
    print(f"Triaging {len(jobs)} postings with {MODEL}...")

    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(score_one, client, system, job): i for i, job in enumerate(jobs)}
        for done, fut in enumerate(cf.as_completed(futures), 1):
            i = futures[fut]
            try:
                results[i] = fut.result()
            except Exception as e:  # one bad posting shouldn't sink the run
                print(f" -> triage failed for '{jobs[i].get('title')}': {e}")
                results[i] = {"bucket": "", "score": 0, "verdict": "Error",
                              "reason": f"Triage failed: {type(e).__name__}", "gap": ""}
            if done % 25 == 0:
                print(f"   {done}/{len(jobs)} scored")
    return results
