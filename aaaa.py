import os
import json
import fitz  # PyMuPDF
import pymupdf4llm
import requests
import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# =====================================================
# SSL FIX (CORPORATE / SELF-SIGNED CERTS)
# =====================================================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =====================================================
# WINDOWS PDF PATH
# =====================================================
PDF_PATH = r"C:\Users\US67251\OneDrive - Premera Blue Cross\Desktop\Premera\p966_claim\hari_new\BC - Determine If BlueCard Claim2 - P966.pdf"

# =====================================================
# HARDCODED CREDENTIALS (AS REQUESTED)
# =====================================================
USERNAME = "hareesha.thippaih@premera.com"
PASSWORD = "Narasamma@65"

PORTAL_DOMAIN = "premera.zavanta.com"


# -----------------------------------------------------
# PDF TEXT + LINK EXTRACTION (CORRECT pymupdf4llm USAGE)
# -----------------------------------------------------
def extract_pdf_content(pdf_path):
    doc = fitz.open(pdf_path)

    # Convert entire PDF to markdown ONCE
    markdown_pages = pymupdf4llm.to_markdown(pdf_path)

    pdf_data = {
        "file_name": os.path.basename(pdf_path),
        "pages": []
    }

    for page_index, page in enumerate(doc):
        links = []
        for link in page.get_links():
            if link.get("uri"):
                links.append({
                    "text": page.get_textbox(link["from"]),
                    "url": link["uri"]
                })

        pdf_data["pages"].append({
            "page_number": page_index + 1,
            "text": markdown_pages[page_index],
            "links": links
        })

    return pdf_data


# -----------------------------------------------------
# LOGIN TO ZAVANTA (SSL FIXED)
# -----------------------------------------------------
def create_portal_session():
    session = requests.Session()

    login_url = "https://premera.zavanta.com/login"

    payload = {
        "username": USERNAME,
        "password": PASSWORD
    }

    response = session.post(
        login_url,
        data=payload,
        timeout=30,
        verify=False
    )
    response.raise_for_status()
    return session


# -----------------------------------------------------
# RECURSIVE HTML CRAWLER (FIXES CHILD TEXT ISSUE)
# -----------------------------------------------------
def crawl_zavanta_pages(session, start_url, visited, max_depth=2, depth=0, parent_url=None):
    if depth > max_depth:
        return []

    if start_url in visited:
        return []

    visited.add(start_url)

    try:
        response = session.get(start_url, timeout=30, verify=False)
        response.raise_for_status()
    except Exception:
        return []

    soup = BeautifulSoup(response.text, "lxml")
    page_text = soup.get_text(separator=" ", strip=True)

    results = [{
        "url": start_url,
        "parent_url": parent_url,
        "text": page_text
    }]

    for a in soup.find_all("a", href=True):
        next_url = urljoin(start_url, a["href"])
        if PORTAL_DOMAIN in urlparse(next_url).netloc:
            results.extend(
                crawl_zavanta_pages(
                    session=session,
                    start_url=next_url,
                    visited=visited,
                    max_depth=max_depth,
                    depth=depth + 1,
                    parent_url=start_url
                )
            )

    return results


# -----------------------------------------------------
# MAIN PIPELINE
# -----------------------------------------------------
def run_pipeline():
    final_output = {}

    # Step 1: PDF extraction
    pdf_data = extract_pdf_content(PDF_PATH)
    final_output["pdf"] = pdf_data

    # Step 2: Collect Zavanta links from PDF
    portal_links = {
        link["url"]
        for page in pdf_data["pages"]
        for link in page["links"]
        if PORTAL_DOMAIN in link["url"]
    }

    # Step 3: Recursive portal + child HTML extraction
    session = create_portal_session()
    visited = set()
    portal_results = []

    for url in portal_links:
        extracted_pages = crawl_zavanta_pages(
            session=session,
            start_url=url,
            visited=visited,
            max_depth=2   # Increase to 3 if deeper nesting exists
        )
        portal_results.extend(extracted_pages)

    final_output["portal_html"] = portal_results
    return final_output


# -----------------------------------------------------
# RUN
# -----------------------------------------------------
if __name__ == "__main__":
    result = run_pipeline()

    with open("extracted_output.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print("✅ SUCCESS: extracted_output.json generated")
