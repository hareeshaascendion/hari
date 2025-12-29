import os
import json
import fitz  # PyMuPDF
import pymupdf4llm
import requests
import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# =====================================================
# DISABLE SSL WARNINGS (CORPORATE CERTS)
# =====================================================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =====================================================
# WINDOWS PDF PATH
# =====================================================
PDF_PATH = r"C:\Users\US67251\OneDrive - Premera Blue Cross\Desktop\Premera\p966_claim\hari_new\BC - Determine If BlueCard Claim2 - P966.pdf"

# =====================================================
# HARDCODED CREDENTIALS
# =====================================================
USERNAME = "hareesha.thippaih@premera.com"
PASSWORD = "Narasamma@65"

PORTAL_DOMAIN = "premera.zavanta.com"


# -----------------------------------------------------
# PDF TEXT + LINK EXTRACTION (FIXED)
# -----------------------------------------------------
def extract_pdf_content(pdf_path):
    doc = fitz.open(pdf_path)

    # Correct pymupdf4llm usage
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

    # 🔴 SSL FIX IS HERE
    response = session.post(
        login_url,
        data=payload,
        timeout=30,
        verify=False  # ✅ FIX
    )
    response.raise_for_status()

    return session


# -----------------------------------------------------
# HTML + CHILD LINK EXTRACTION (SSL FIXED)
# -----------------------------------------------------
def extract_html_text(session, url, parent_url=None, visited=None):
    if visited is None:
        visited = set()

    if url in visited:
        return None

    visited.add(url)

    response = session.get(
        url,
        timeout=30,
        verify=False  # ✅ FIX
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")
    page_text = soup.get_text(separator=" ", strip=True)

    child_urls = set()
    for a in soup.find_all("a", href=True):
        full_url = urljoin(url, a["href"])
        if PORTAL_DOMAIN in urlparse(full_url).netloc:
            child_urls.add(full_url)

    return {
        "url": url,
        "parent_url": parent_url,
        "text": page_text,
        "child_links": list(child_urls)
    }


# -----------------------------------------------------
# MAIN PIPELINE
# -----------------------------------------------------
def run_pipeline():
    final_output = {}

    # Step 1: PDF
    pdf_data = extract_pdf_content(PDF_PATH)
    final_output["pdf"] = pdf_data

    # Step 2: Collect Zavanta links
    portal_links = {
        link["url"]
        for page in pdf_data["pages"]
        for link in page["links"]
        if PORTAL_DOMAIN in link["url"]
    }

    # Step 3: Portal + child pages
    session = create_portal_session()
    visited = set()
    portal_results = []

    for url in portal_links:
        page_data = extract_html_text(
            session=session,
            url=url,
            parent_url="PDF",
            visited=visited
        )

        if not page_data:
            continue

        child_pages = []
        for child_url in page_data["child_links"]:
            child_data = extract_html_text(
                session=session,
                url=child_url,
                parent_url=url,
                visited=visited
            )
            if child_data:
                child_pages.append(child_data)

        page_data["child_links"] = child_pages
        portal_results.append(page_data)

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
