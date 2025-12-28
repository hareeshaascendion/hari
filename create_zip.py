"""
Complete ZIP File Generator with SSL Fix
Creates: pdf_hyperlink_extractor_project.zip

Usage: python create_project_zip.py
"""
import zipfile
import os
from datetime import datetime

def create_project_zip():
    """Create a complete ZIP file with all project files"""
    
    zip_filename = 'pdf_hyperlink_extractor_complete.zip'
    
    print("=" * 80)
    print("PDF HYPERLINK EXTRACTOR - COMPLETE PROJECT ZIP GENERATOR")
    print("=" * 80)
    print(f"\nCreating: {zip_filename}\n")
    
    # All project files
    files = {
        'pdf_hyperlink_extractor.py': '''import pymupdf4llm
import pymupdf as fitz
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import json
from typing import List, Dict, Set

class PDFHyperlinkExtractor:
    def __init__(self, pdf_path: str, credentials: Dict = None):
        self.pdf_path = pdf_path
        self.visited_urls = set()
        self.all_content = {}
        self.session = requests.Session()
        self.credentials = credentials or {}
        self.authenticated = False
        self.verify_ssl = True
        
    def login_to_portal(self, login_url: str, username: str, password: str, 
                       login_data: Dict = None, method: str = 'auto', verify_ssl: bool = True):
        """Login to the portal with credentials"""
        try:
            print(f"Attempting login to {login_url}...")
            
            if not verify_ssl:
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                print("⚠️  SSL verification disabled")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            if method == 'basic':
                self.session.auth = (username, password)
                response = self.session.get(login_url, headers=headers, verify=verify_ssl)
            else:
                response = self.session.get(login_url, headers=headers, verify=verify_ssl)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                form = soup.find('form')
                if not form:
                    print("Warning: No form found")
                    return False
                
                if login_data is None:
                    login_data = {}
                    username_fields = ['username', 'user', 'email', 'login', 'userid']
                    password_fields = ['password', 'pass', 'pwd']
                    
                    for input_tag in form.find_all('input'):
                        name = input_tag.get('name', '').lower()
                        input_type = input_tag.get('type', '').lower()
                        
                        if any(f in name for f in username_fields) or input_type == 'email':
                            login_data[input_tag.get('name')] = username
                        elif any(f in name for f in password_fields) or input_type == 'password':
                            login_data[input_tag.get('name')] = password
                        elif input_type == 'hidden':
                            login_data[input_tag.get('name')] = input_tag.get('value', '')
                
                action = form.get('action', '')
                login_post_url = urljoin(login_url, action) if action else login_url
                
                print(f"Posting to: {login_post_url}")
                response = self.session.post(
                    login_post_url,
                    data=login_data,
                    headers=headers,
                    allow_redirects=True,
                    verify=verify_ssl
                )
            
            if response.status_code == 200:
                if 'logout' in response.text.lower() or 'sign out' in response.text.lower():
                    self.authenticated = True
                    self.verify_ssl = verify_ssl
                    print("✓ Login successful!")
                    return True
                elif 'error' in response.text.lower() or 'invalid' in response.text.lower():
                    print("✗ Login failed - check credentials")
                    return False
                else:
                    self.authenticated = True
                    self.verify_ssl = verify_ssl
                    print("✓ Login appears successful")
                    return True
            else:
                print(f"✗ Login failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"✗ Login error: {str(e)}")
            return False
    
    def extract_pdf_content(self):
        """Extract text and hyperlinks from PDF"""
        print("Extracting PDF content...")
        markdown_text = pymupdf4llm.to_markdown(self.pdf_path)
        doc = fitz.open(self.pdf_path)
        links = []
        
        for page_num, page in enumerate(doc, start=1):
            for link in page.get_links():
                url = link.get('uri', '')
                if url and url.startswith('http'):
                    anchor_text = ''
                    if link.get('rect'):
                        rect = fitz.Rect(link['rect'])
                        anchor_text = page.get_text("text", clip=rect).strip()
                    links.append({
                        'page': page_num,
                        'url': url,
                        'anchor_text': anchor_text
                    })
        
        doc.close()
        return {'markdown_text': markdown_text, 'links': links}
    
    def fetch_url_content(self, url: str, timeout: int = 30) -> Dict:
        """Fetch content from URL"""
        try:
            print(f"Fetching: {url}")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            }
            
            response = self.session.get(url, headers=headers, timeout=timeout, 
                                       allow_redirects=True, verify=self.verify_ssl)
            response.raise_for_status()
            
            if 'login' in response.url.lower() and url.lower() not in response.url.lower():
                return {'url': url, 'status': 'auth_required', 'error': 'Redirected to login'}
            
            soup = BeautifulSoup(response.text, 'html.parser')
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()
            
            text = soup.get_text(separator='\\n', strip=True)
            child_links = []
            base_domain = urlparse(url).netloc
            
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                full_url = urljoin(url, href)
                if urlparse(full_url).netloc == base_domain:
                    child_links.append({'url': full_url, 'text': a_tag.get_text(strip=True)})
            
            return {
                'url': url,
                'status': 'success',
                'text': text,
                'title': soup.title.string if soup.title else '',
                'child_links': child_links,
                'final_url': response.url
            }
        except requests.exceptions.Timeout:
            return {'url': url, 'status': 'timeout', 'error': 'Timeout'}
        except requests.exceptions.ConnectionError:
            return {'url': url, 'status': 'connection_error', 'error': 'Connection failed'}
        except requests.exceptions.HTTPError as e:
            return {'url': url, 'status': 'http_error', 'error': str(e)}
        except Exception as e:
            return {'url': url, 'status': 'error', 'error': str(e)}
    
    def extract_child_links(self, parent_url: str, max_depth: int = 1, current_depth: int = 0):
        """Recursively extract child links"""
        if current_depth >= max_depth or parent_url in self.visited_urls:
            return
        
        self.visited_urls.add(parent_url)
        content = self.fetch_url_content(parent_url)
        self.all_content[parent_url] = content
        time.sleep(1)
        
        if content['status'] == 'success' and current_depth < max_depth:
            for child in content.get('child_links', [])[:10]:
                child_url = child['url']
                if child_url not in self.visited_urls:
                    print(f"  → Child (depth {current_depth + 1}): {child_url}")
                    self.extract_child_links(child_url, max_depth, current_depth + 1)
    
    def process_all_links(self, max_depth: int = 1, domain_filter: str = None):
        """Process all PDF links"""
        pdf_data = self.extract_pdf_content()
        print(f"\\nFound {len(pdf_data['links'])} links")
        
        links_to_process = pdf_data['links']
        if domain_filter:
            links_to_process = [l for l in links_to_process if domain_filter in l['url']]
            print(f"Filtered to {len(links_to_process)} links")
        
        for link_data in links_to_process:
            url = link_data['url']
            print(f"\\nPage {link_data['page']}: {url}")
            self.extract_child_links(url, max_depth=max_depth)
        
        return {'pdf_content': pdf_data, 'fetched_content': self.all_content}
    
    def save_results(self, output_file: str = 'output.json'):
        """Save to JSON"""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.all_content, f, indent=2, ensure_ascii=False)
        print(f"\\nSaved: {output_file}")
    
    def save_text_summary(self, output_file: str = 'summary.txt'):
        """Save text summary"""
        with open(output_file, 'w', encoding='utf-8') as f:
            for url, content in self.all_content.items():
                f.write(f"\\n{'=' * 80}\\n")
                f.write(f"URL: {url}\\nStatus: {content['status']}\\n")
                if content['status'] == 'success':
                    f.write(f"Title: {content.get('title', 'N/A')}\\n")
                    f.write(f"Content:\\n{content['text'][:2000]}\\n")
                    f.write(f"Child links: {len(content.get('child_links', []))}\\n")
                else:
                    f.write(f"Error: {content.get('error')}\\n")
        print(f"Saved: {output_file}")
''',

        '.env': '''# PREMERA CREDENTIALS
# Add your actual credentials here after downloading

PREMERA_USERNAME=hareesha.thippaih@premera.com
PREMERA_PASSWORD=Narasamma@65
PREMERA_LOGIN_URL=https://premera.zavanta.com/portal/login

# PDF SETTINGS
PDF_INPUT_FILE=premera_benefits_document.pdf
OUTPUT_DIRECTORY=output

# EXTRACTION SETTINGS
MAX_CRAWL_DEPTH=1
REQUEST_TIMEOUT=30
DOMAIN_FILTER=premera.zavanta.com
DISABLE_SSL_VERIFICATION=True
''',

        'run.py': '''"""
Main execution script using .env credentials
Handles SSL errors automatically
"""
import os
from dotenv import load_dotenv
from pdf_hyperlink_extractor import PDFHyperlinkExtractor

# Load credentials from .env
load_dotenv()

def main():
    print("=" * 80)
    print("PDF HYPERLINK EXTRACTOR - PREMERA PORTAL")
    print("=" * 80)
    
    # Load from .env
    username = os.getenv('PREMERA_USERNAME')
    password = os.getenv('PREMERA_PASSWORD')
    login_url = os.getenv('PREMERA_LOGIN_URL')
    pdf_file = os.getenv('PDF_INPUT_FILE', 'document.pdf')
    output_dir = os.getenv('OUTPUT_DIRECTORY', 'output')
    max_depth = int(os.getenv('MAX_CRAWL_DEPTH', '1'))
    domain_filter = os.getenv('DOMAIN_FILTER')
    disable_ssl = os.getenv('DISABLE_SSL_VERIFICATION', 'False').lower() == 'true'
    
    print(f"\\n📄 PDF: {pdf_file}")
    print(f"👤 User: {username}")
    print(f"🔒 SSL: {'Disabled' if disable_ssl else 'Enabled'}")
    
    # Create output dir
    os.makedirs(output_dir, exist_ok=True)
    
    # Check PDF exists
    if not os.path.exists(pdf_file):
        print(f"\\n❌ PDF not found: {pdf_file}")
        print("Please add your PDF file to this directory")
        return
    
    # Initialize
    extractor = PDFHyperlinkExtractor(pdf_file)
    
    # Login
    print("\\n" + "=" * 80)
    print("LOGGING IN...")
    print("=" * 80)
    
    success = extractor.login_to_portal(
        login_url=login_url,
        username=username,
        password=password,
        method='auto',
        verify_ssl=not disable_ssl
    )
    
    if not success:
        print("\\n⚠️  Login failed!")
        choice = input("Continue anyway? (y/n): ")
        if choice.lower() != 'y':
            return
    
    # Extract
    print("\\n" + "=" * 80)
    print("EXTRACTING CONTENT...")
    print("=" * 80)
    
    results = extractor.process_all_links(
        max_depth=max_depth,
        domain_filter=domain_filter
    )
    
    # Save
    print("\\n" + "=" * 80)
    print("SAVING RESULTS...")
    print("=" * 80)
    
    json_file = os.path.join(output_dir, 'premera_content.json')
    summary_file = os.path.join(output_dir, 'premera_summary.txt')
    
    extractor.save_results(json_file)
    extractor.save_text_summary(summary_file)
    
    # Summary
    successful = sum(1 for c in extractor.all_content.values() if c['status'] == 'success')
    
    print("\\n" + "=" * 80)
    print("✅ COMPLETE!")
    print("=" * 80)
    print(f"\\n📊 Total URLs: {len(extractor.all_content)}")
    print(f"✓ Successful: {successful}")
    print(f"✗ Failed: {len(extractor.all_content) - successful}")
    print(f"\\n📁 Output:")
    print(f"   • {json_file}")
    print(f"   • {summary_file}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\\n\\nInterrupted")
    except Exception as e:
        print(f"\\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
''',

        'requirements.txt': '''pymupdf4llm>=0.0.1
pymupdf>=1.23.0
requests>=2.31.0
beautifulsoup4>=4.12.0
lxml>=4.9.0
python-dotenv>=1.0.0
urllib3>=1.26.0
''',

        '.gitignore': '''# Sensitive
.env
*.pdf

# Output
output/
*.json
*.txt
!requirements.txt
!README.md

# Python
__pycache__/
*.pyc
.Python
*.egg-info/
venv/
.venv
''',

        'README.md': '''# PDF Hyperlink Extractor - Premera Portal

Extract and follow hyperlinks from PDF documents with authentication.

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Your Credentials Are Already Set!
The `.env` file already contains your Premera credentials:
- Username: hareesha.thippaih@premera.com
- Password: (already configured)

### 3. Add Your PDF
Place your PDF file in this directory and name it:
`premera_benefits_document.pdf`

Or edit `.env` to point to your PDF filename.

### 4. Run!
```bash
python run.py
```

## ✨ Features

- ✅ SSL error handling (automatically disabled)
- ✅ Premera portal authentication
- ✅ Recursive link crawling
- ✅ JSON and text output
- ✅ No configuration needed - ready to run!

## 📊 Output

Results in `output/` directory:
- `premera_content.json` - Complete data
- `premera_summary.txt` - Readable summary

## 🔧 Customization

Edit `.env` file to change:
- `MAX_CRAWL_DEPTH` - How deep to follow links (default: 1)
- `DOMAIN_FILTER` - Filter URLs by domain
- `DISABLE_SSL_VERIFICATION` - SSL settings (True/False)

## 🔒 Security Note

⚠️ Your credentials are in the `.env` file. Keep this file secure:
- Don't share the `.env` file
- Don't commit it to Git (already in .gitignore)
- Only use on trusted computers

## Troubleshooting

**PDF not found?**
- Make sure PDF is in same directory as run.py
- Or update PDF_INPUT_FILE in .env

**Login fails?**
- Credentials are already set, check website is accessible
- Try accessing portal in browser first

**SSL errors?**
- Already handled! DISABLE_SSL_VERIFICATION=True in .env
'''
    }
    
    # Create ZIP
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for filename, content in files.items():
            zipf.writestr(filename, content)
            print(f"  ✓ {filename}")
    
    size = os.path.getsize(zip_filename)
    
    print("\\n" + "=" * 80)
    print("✅ ZIP CREATED!")
    print("=" * 80)
    print(f"\\nFile: {zip_filename}")
    print(f"Size: {size:,} bytes ({size/1024:.1f} KB)")
    print(f"\\n📦 Contains:")
    for f in sorted(files.keys()):
        print(f"   • {f}")
    
    print("\\n🚀 Next Steps:")
    print("   1. Extract the ZIP file")
    print("   2. cd into extracted folder")
    print("   3. pip install -r requirements.txt")
    print("   4. Add your PDF file to the folder")
    print("   5. python run.py")
    print("\\n✨ Credentials are already configured!")
    print("   SSL verification is disabled to avoid errors")
    print("=" * 80)

if __name__ == "__main__":
    try:
        create_project_zip()
    except Exception as e:
        print(f"\\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
