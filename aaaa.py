import os
import json
import fitz  # PyMuPDF
import pymupdf4llm
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

PDF_PATH = "/mnt/data/BC_Determine_If_BlueCard_Claim_P966.pdf"
PORTAL_DOMAIN = "premera.zavanta.com"

USERNAME = os.getenv("ZAVANTA_USERNAME")
PASSWORD = os.getenv("ZAVANTA_PASSWORD")

# -------------------------
# PDF Extraction
# -------------------------
def extract_pdf_content(pdf_path):
    doc = fitz.open(pdf_path)
    pdf_data = {"file_name": os.path.basename(pdf_path), "pages": []}

    for page_num, page in enumerate(doc, start=1):
        text = pymupdf4llm.to_markdown(page)
        links = []

        for link in page.get_links():
            uri = link.get("uri")
            if uri:
                links.append({
                    "text": page.get_textbox(link["from"]),
                    "url": uri
                })

        pdf_data["pages"].append({
            "page_number": page_num,
            "text": text,
            "links": links
        })

    return pdf_data


# -------------------------
# Portal Login Session
# -------------------------
def create_portal_session():
    session = requests.Session()

    login_url = "https://premera.zavanta.com/login"

    payload = {
        "username": USERNAME,
        "password": PASSWORD
    }

    response = session.post(login_url, data=payload)
    response.raise_for_status()

    return session


# -------------------------
# HTML Extraction
# -------------------------
def extract_html_text(session, url, parent_url=None, visited=None):
    if visited is None:
        visited = set()

    if url in visited:
        return None

    visited.add(url)

    resp = session.get(url)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    page_text = soup.get_text(separator=" ", strip=True)

    child_links = []
    for a in soup.find_all("a", href=True):
        full_url = urljoin(url, a["href"])
        if PORTAL_DOMAIN in urlparse(full_url).netloc:
            child_links.append(full_url)

    return {
        "url": url,
        "parent_url": parent_url,
        "text": page_text,
        "child_links": child_links
    }


# -------------------------
# Main Pipeline
# -------------------------
def run_pipeline():
    output = {}

    # Step 1: PDF
    pdf_data = extract_pdf_content(PDF_PATH)
    output["pdf"] = pdf_data

    # Step 2: Collect Portal Links
    portal_links = set()
    for page in pdf_data["pages"]:
        for link in page["links"]:
            if PORTAL_DOMAIN in link["url"]:
                portal_links.add(link["url"])

    # Step 3: Portal HTML Extraction
    session = create_portal_session()
    portal_results = []
    visited = set()

    for url in portal_links:
        page_data = extract_html_text(session, url, parent_url="PDF", visited=visited)
        if not page_data:
            continue

        child_data = []
        for child_url in page_data["child_links"]:
            child_page = extract_html_text(
                session, child_url, parent_url=url, visited=visited
            )
            if child_page:
                child_data.append(child_page)

        page_data["child_links"] = child_data
        portal_results.append(page_data)

    output["portal_html"] = portal_results

    return output


# -------------------------
# Execute
# -------------------------
if __name__ == "__main__":
    final_json = run_pipeline()
    with open("extracted_output.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, indent=2, ensure_ascii=False)

    print("✅ Extraction completed → extracted_output.json")
