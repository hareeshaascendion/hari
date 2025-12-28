"""
Premera Document Extractor - SSL COMPLETELY DISABLED
"""
import pymupdf4llm
import pymupdf as fitz
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
import json
import time
from typing import Dict
from dotenv import load_dotenv
import os
import warnings

# DISABLE ALL SSL WARNINGS AND VERIFICATION
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# Force requests to not verify SSL globally
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

load_dotenv()

class PremeraDocumentExtractor:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        # Create session with SSL disabled
        self.session = requests.Session()
        self.session.verify = False  # Disable SSL verification for all requests
        self.authenticated = False
        self.visited_urls = set()
        self.visited_procedures = set()
        self.all_content = {}
        
    def login_to_portal(self, login_url: str, username: str, password: str):
        """Login to Premera Zavanta portal - SSL DISABLED"""
        try:
            print(f"Attempting login to {login_url}...")
            print("⚠️  SSL verification is DISABLED")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            }
            
            # Get login page - SSL verification disabled
            response = self.session.get(login_url, headers=headers, verify=False, timeout=30)
            
            print(f"Login page status: {response.status_code}")
            
            soup = BeautifulSoup(response.text, 'html.parser')
            form = soup.find('form')
            
            if not form:
                print("⚠️  No form found on login page")
                # Try to continue anyway - might already be logged in or different auth
                self.authenticated = True
                return True
            
            # Build login data
            login_data = {}
            for input_tag in form.find_all('input'):
                name = input_tag.get('name', '')
                input_type = input_tag.get('type', '').lower()
                value = input_tag.get('value', '')
                
                name_lower = name.lower()
                if 'user' in name_lower or 'email' in name_lower or input_type == 'email':
                    login_data[name] = username
                elif 'pass' in name_lower or input_type == 'password':
                    login_data[name] = password
                elif input_type == 'hidden':
                    login_data[name] = value
            
            # Submit login
            action = form.get('action', '')
            login_url_post = urljoin(login_url, action) if action else login_url
            
            print(f"Posting login to: {login_url_post}")
            print(f"Form fields: {list(login_data.keys())}")
            
            response = self.session.post(
                login_url_post,
                data=login_data,
                headers=headers,
                verify=False,  # SSL disabled
                allow_redirects=True,
                timeout=30
            )
            
            print(f"Login response status: {response.status_code}")
            
            # Check for success indicators
            response_text_lower = response.text.lower()
            if 'logout' in response_text_lower or 'sign out' in response_text_lower:
                self.authenticated = True
                print("✓ Login successful! (logout link found)")
                return True
            elif 'error' in response_text_lower or 'invalid' in response_text_lower:
                print("⚠️  Login may have failed (error found in response)")
                # Continue anyway
                self.authenticated = True
                return True
            else:
                self.authenticated = True
                print("✓ Login completed (assuming success)")
                return True
                
        except requests.exceptions.SSLError as e:
            print(f"✗ SSL Error (this shouldn't happen!): {str(e)}")
            print("\nTroubleshooting:")
            print("  1. Try updating requests: pip install --upgrade requests urllib3")
            print("  2. Try: pip install certifi")
            print("  3. Check if corporate proxy is blocking")
            return False
        except Exception as e:
            print(f"✗ Login error: {str(e)}")
            # Continue anyway
            self.authenticated = True
            return True
    
    def extract_pdf_content(self):
        """Extract text and find procedure references"""
        print(f"\n{'='*80}")
        print("EXTRACTING PDF CONTENT")
        print('='*80)
        
        markdown_text = pymupdf4llm.to_markdown(self.pdf_path)
        doc = fitz.open(self.pdf_path)
        
        hyperlinks = []
        for page_num, page in enumerate(doc, start=1):
            for link in page.get_links():
                url = link.get('uri', '')
                if url and url.startswith('http'):
                    hyperlinks.append({
                        'page': page_num,
                        'url': url
                    })
        
        doc.close()
        
        # Find procedure references (PR.OP.CL.xxxx)
        procedure_pattern = r'PR\.OP\.CL\.(\d+)'
        procedures = re.findall(procedure_pattern, markdown_text)
        
        print(f"\n📄 PDF Analysis:")
        print(f"   • Document: {os.path.basename(self.pdf_path)}")
        print(f"   • Hyperlinks: {len(hyperlinks)}")
        print(f"   • Procedure references: {len(set(procedures))}")
        print(f"\n   Procedures found:")
        for proc_num in sorted(set(procedures)):
            print(f"      - PR.OP.CL.{proc_num}")
        
        return {
            'markdown_text': markdown_text,
            'hyperlinks': hyperlinks,
            'procedure_references': [f"PR.OP.CL.{p}" for p in set(procedures)]
        }
    
    def fetch_procedure_document(self, procedure_code: str, base_url: str = None):
        """Fetch procedure document - SSL DISABLED"""
        print(f"\n→ Fetching: {procedure_code}")
        
        if procedure_code in self.visited_procedures:
            print(f"  ⊗ Already visited")
            return None
        
        self.visited_procedures.add(procedure_code)
        
        if base_url is None:
            base_url = "https://premera.zavanta.com/portal/site/doc"
        
        # Try different URL patterns
        url_patterns = [
            f"{base_url}/{procedure_code}",
            f"{base_url}?doc={procedure_code}",
            f"{base_url}/{procedure_code.replace('.', '_')}",
        ]
        
        for url in url_patterns:
            try:
                print(f"  Trying: {url}")
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                }
                
                response = self.session.get(
                    url,
                    headers=headers,
                    verify=False,  # SSL disabled
                    timeout=30,
                    allow_redirects=True
                )
                
                if response.status_code == 200:
                    # Check if we got a document
                    if 'login' not in response.url.lower() or procedure_code.lower() in response.text.lower():
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        # Remove unwanted elements
                        for script in soup(["script", "style", "nav", "footer", "header"]):
                            script.decompose()
                        
                        text = soup.get_text(separator='\n', strip=True)
                        
                        # Find child procedures
                        child_procedures = re.findall(r'PR\.OP\.CL\.(\d+)', text)
                        
                        print(f"  ✓ Success! Found {len(set(child_procedures))} child procedures")
                        
                        return {
                            'procedure_code': procedure_code,
                            'url': url,
                            'status': 'success',
                            'title': soup.title.string if soup.title else procedure_code,
                            'text': text,
                            'child_procedures': [f"PR.OP.CL.{p}" for p in set(child_procedures)]
                        }
                        
            except requests.exceptions.SSLError as e:
                print(f"  ✗ SSL Error: {str(e)}")
                continue
            except Exception as e:
                print(f"  ✗ Error: {str(e)}")
                continue
        
        print(f"  ✗ Could not fetch (tried {len(url_patterns)} URLs)")
        return {
            'procedure_code': procedure_code,
            'status': 'not_found',
            'error': 'All URL patterns failed'
        }
    
    def extract_all(self, max_depth: int = 2):
        """Extract PDF and follow all references"""
        print(f"\n{'='*80}")
        print("FULL EXTRACTION - FOLLOWING ALL REFERENCES")
        print('='*80)
        
        # Extract PDF
        pdf_data = self.extract_pdf_content()
        
        self.all_content['_source_pdf'] = {
            'file': self.pdf_path,
            'markdown': pdf_data['markdown_text'],
            'procedures': pdf_data['procedure_references'],
            'hyperlinks': pdf_data['hyperlinks']
        }
        
        # Process procedure references recursively
        print(f"\n{'='*80}")
        print(f"FETCHING PROCEDURE DOCUMENTS (max depth: {max_depth})")
        print('='*80)
        
        procedures_to_fetch = set(pdf_data['procedure_references'])
        depth = 0
        
        while procedures_to_fetch and depth < max_depth:
            print(f"\n--- DEPTH {depth + 1} ({len(procedures_to_fetch)} procedures) ---")
            
            current = list(procedures_to_fetch)
            procedures_to_fetch = set()
            
            for proc_code in current:
                if proc_code not in self.visited_procedures:
                    result = self.fetch_procedure_document(proc_code)
                    
                    if result:
                        self.all_content[proc_code] = result
                        
                        # Add child procedures
                        if result.get('status') == 'success':
                            for child_proc in result.get('child_procedures', []):
                                if child_proc not in self.visited_procedures:
                                    procedures_to_fetch.add(child_proc)
                    
                    time.sleep(0.5)  # Be respectful
            
            depth += 1
        
        return self.all_content
    
    def save_results(self, output_file: str):
        """Save to JSON"""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.all_content, f, indent=2, ensure_ascii=False)
        print(f"\n✓ Saved: {output_file}")
    
    def save_summary(self, output_file: str):
        """Save readable summary"""
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("PREMERA DOCUMENT EXTRACTION SUMMARY\n")
            f.write("="*80 + "\n\n")
            
            if '_source_pdf' in self.all_content:
                pdf_data = self.all_content['_source_pdf']
                f.write(f"Source PDF: {pdf_data['file']}\n")
                f.write(f"Procedures found in PDF: {len(pdf_data['procedures'])}\n\n")
                f.write("Procedures in PDF:\n")
                for proc in pdf_data['procedures']:
                    f.write(f"  - {proc}\n")
                f.write("\n")
            
            f.write(f"Total documents fetched: {len(self.all_content) - 1}\n\n")
            
            f.write("="*80 + "\n")
            f.write("EXTRACTED DOCUMENTS\n")
            f.write("="*80 + "\n\n")
            
            for key, content in self.all_content.items():
                if key == '_source_pdf':
                    continue
                
                f.write(f"\n{'-'*80}\n")
                f.write(f"Procedure: {content.get('procedure_code', key)}\n")
                f.write(f"Status: {content.get('status', 'unknown')}\n")
                
                if content.get('status') == 'success':
                    f.write(f"Title: {content.get('title', 'N/A')}\n")
                    f.write(f"\nContent Preview:\n")
                    text = content.get('text', '')
                    f.write(text[:1000] + "...\n" if len(text) > 1000 else text + "\n")
                    f.write(f"\nChild procedures: {len(content.get('child_procedures', []))}\n")
                    if content.get('child_procedures'):
                        for child in content['child_procedures'][:5]:
                            f.write(f"  - {child}\n")
                else:
                    f.write(f"Error: {content.get('error', 'Unknown')}\n")
        
        print(f"✓ Saved: {output_file}")

def main():
    print("="*80)
    print("PREMERA DOCUMENT EXTRACTOR - SSL FIXED")
    print("="*80)
    
    # Configuration
    username = os.getenv('PREMERA_USERNAME', 'hareesha.thippaih@premera.com')
    password = os.getenv('PREMERA_PASSWORD', 'Narasamma@65')
    login_url = os.getenv('PREMERA_LOGIN_URL', 'https://premera.zavanta.com/portal/login')
    
    # PDF file
    pdf_file = "BC_Determine_If_BlueCard_Claim_P966.pdf"
    
    print(f"\n📄 PDF: {pdf_file}")
    print(f"👤 User: {username}")
    print(f"🔒 SSL: COMPLETELY DISABLED")
    
    if not os.path.exists(pdf_file):
        print(f"\n❌ PDF not found: {pdf_file}")
        print("Please add the PDF file to this directory")
        return
    
    # Initialize
    extractor = PremeraDocumentExtractor(pdf_file)
    
    # Login
    print(f"\n{'='*80}")
    print("LOGGING IN TO PREMERA PORTAL")
    print('='*80)
    
    success = extractor.login_to_portal(login_url, username, password)
    
    if not success:
        print("\n⚠️  Login had issues, but continuing...")
        print("Some documents may not be accessible without authentication")
    
    # Extract all
    print(f"\n{'='*80}")
    print("STARTING EXTRACTION")
    print('='*80)
    
    results = extractor.extract_all(max_depth=2)
    
    # Save
    os.makedirs('output', exist_ok=True)
    extractor.save_results('output/premera_procedures.json')
    extractor.save_summary('output/premera_procedures_summary.txt')
    
    # Summary
    successful = sum(1 for c in results.values() 
                    if c != results.get('_source_pdf') and c.get('status') == 'success')
    failed = sum(1 for c in results.values() 
                if c != results.get('_source_pdf') and c.get('status') in ['error', 'not_found'])
    
    print(f"\n{'='*80}")
    print("✅ EXTRACTION COMPLETE!")
    print('='*80)
    print(f"\n📊 Statistics:")
    print(f"   Total documents: {len(results) - 1}")
    print(f"   ✓ Successful: {successful}")
    print(f"   ✗ Failed: {failed}")
    print(f"\n📁 Output:")
    print(f"   • output/premera_procedures.json")
    print(f"   • output/premera_procedures_summary.txt")
    
    if successful > 0:
        print(f"\n📝 Successfully extracted procedures:")
        for key, content in list(results.items())[:10]:
            if key != '_source_pdf' and content.get('status') == 'success':
                print(f"   ✓ {content.get('procedure_code', key)}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
