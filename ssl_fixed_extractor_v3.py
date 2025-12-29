"""
Premera Document Extractor - SSL FIXED + FOLLOWS ACTUAL HYPERLINKS
Extracts the 37 hyperlinks from your PDF and follows them!
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

# DISABLE ALL SSL WARNINGS
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore', message='Unverified HTTPS request')
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

load_dotenv()

class PremeraDocumentExtractor:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.session = requests.Session()
        self.session.verify = False
        self.authenticated = False
        self.visited_urls = set()
        self.all_content = {}
        
    def login_to_portal(self, login_url: str, username: str, password: str):
        """Login to Premera Zavanta portal"""
        try:
            print(f"Attempting login to {login_url}...")
            print("⚠️  SSL verification is DISABLED")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            }
            
            response = self.session.get(login_url, headers=headers, verify=False, timeout=30)
            print(f"Login page status: {response.status_code}")
            
            soup = BeautifulSoup(response.text, 'html.parser')
            form = soup.find('form')
            
            if not form:
                print("⚠️  No form found - might use different auth or already logged in")
                self.authenticated = True
                return True
            
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
            
            action = form.get('action', '')
            login_url_post = urljoin(login_url, action) if action else login_url
            
            print(f"Posting login to: {login_url_post}")
            
            response = self.session.post(
                login_url_post,
                data=login_data,
                headers=headers,
                verify=False,
                allow_redirects=True,
                timeout=30
            )
            
            print(f"Login response: {response.status_code}")
            
            response_lower = response.text.lower()
            if 'logout' in response_lower or 'sign out' in response_lower:
                print("✓ Login successful!")
            else:
                print("✓ Login completed")
            
            self.authenticated = True
            return True
                
        except Exception as e:
            print(f"✗ Login error: {str(e)}")
            self.authenticated = True
            return True
    
    def extract_pdf_content(self):
        """Extract text and ALL hyperlinks from PDF"""
        print(f"\n{'='*80}")
        print("EXTRACTING PDF CONTENT AND HYPERLINKS")
        print('='*80)
        
        # Get markdown text
        markdown_text = pymupdf4llm.to_markdown(self.pdf_path)
        
        # Extract ALL hyperlinks with details
        doc = fitz.open(self.pdf_path)
        hyperlinks = []
        
        for page_num, page in enumerate(doc, start=1):
            for link in page.get_links():
                url = link.get('uri', '')
                if url and url.startswith('http'):
                    # Get anchor text (the visible text that was clicked)
                    anchor_text = ''
                    if link.get('rect'):
                        rect = fitz.Rect(link['rect'])
                        anchor_text = page.get_text("text", clip=rect).strip()
                    
                    hyperlinks.append({
                        'page': page_num,
                        'url': url,
                        'anchor_text': anchor_text,
                        'domain': urlparse(url).netloc
                    })
        
        doc.close()
        
        # Try multiple patterns for procedure references in text
        procedure_patterns = [
            r'PR\.OP\.CL\.(\d+)',  # Standard
            r'PR\s*\.\s*OP\s*\.\s*CL\s*\.\s*(\d+)',  # With spaces
            r'PR\.OP\.CL\s+(\d+)',  # Space before number
        ]
        
        all_procedures = []
        for pattern in procedure_patterns:
            procedures = re.findall(pattern, markdown_text)
            all_procedures.extend(procedures)
        
        print(f"\n📄 PDF Analysis:")
        print(f"   • Document: {os.path.basename(self.pdf_path)}")
        print(f"   • Total hyperlinks found: {len(hyperlinks)}")
        print(f"   • Procedure refs in text: {len(set(all_procedures))}")
        
        # Categorize hyperlinks by domain
        zavanta_links = [h for h in hyperlinks if 'zavanta' in h['domain'].lower() or 'premera' in h['domain'].lower()]
        other_links = [h for h in hyperlinks if h not in zavanta_links]
        
        print(f"\n   Hyperlink breakdown:")
        print(f"      • Zavanta/Premera links: {len(zavanta_links)}")
        print(f"      • Other domains: {len(other_links)}")
        
        print(f"\n   Sample hyperlinks:")
        for i, link in enumerate(hyperlinks[:5], 1):
            print(f"      {i}. {link['url']}")
            if link['anchor_text']:
                print(f"         Text: {link['anchor_text'][:60]}")
        
        if len(hyperlinks) > 5:
            print(f"      ... and {len(hyperlinks) - 5} more")
        
        return {
            'markdown_text': markdown_text,
            'all_hyperlinks': hyperlinks,
            'zavanta_links': zavanta_links,
            'other_links': other_links,
            'procedure_references': [f"PR.OP.CL.{p}" for p in set(all_procedures)]
        }
    
    def fetch_url(self, url: str, source_info: Dict = None):
        """Fetch content from a URL"""
        if url in self.visited_urls:
            print(f"  ⊗ Already visited")
            return None
        
        self.visited_urls.add(url)
        
        try:
            print(f"\n→ Fetching: {url}")
            if source_info and source_info.get('anchor_text'):
                print(f"  Link text: {source_info['anchor_text'][:60]}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            }
            
            response = self.session.get(
                url,
                headers=headers,
                verify=False,
                timeout=30,
                allow_redirects=True
            )
            
            if response.status_code != 200:
                print(f"  ✗ Status {response.status_code}")
                return {
                    'url': url,
                    'status': 'error',
                    'error': f'HTTP {response.status_code}'
                }
            
            # Check if redirected to login
            if 'login' in response.url.lower() and 'login' not in url.lower():
                print(f"  ⚠️  Redirected to login - auth may be required")
                return {
                    'url': url,
                    'status': 'auth_required',
                    'error': 'Redirected to login page'
                }
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Remove unwanted elements
            for element in soup(["script", "style", "nav", "footer", "header"]):
                element.decompose()
            
            text = soup.get_text(separator='\n', strip=True)
            
            # Find child links
            child_links = []
            base_domain = urlparse(url).netloc
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                full_url = urljoin(url, href)
                if base_domain in urlparse(full_url).netloc and full_url not in self.visited_urls:
                    child_links.append({
                        'url': full_url,
                        'text': a_tag.get_text(strip=True)[:100]
                    })
            
            # Find procedure references in fetched content
            procedure_refs = re.findall(r'PR\.OP\.CL\.(\d+)', text)
            
            print(f"  ✓ Success! Text: {len(text)} chars, Child links: {len(child_links)}, Procedures: {len(set(procedure_refs))}")
            
            return {
                'url': url,
                'status': 'success',
                'title': soup.title.string if soup.title else '',
                'text': text,
                'text_length': len(text),
                'child_links': child_links[:10],  # Limit to 10
                'procedure_references': [f"PR.OP.CL.{p}" for p in set(procedure_refs)],
                'final_url': response.url,
                'source_anchor': source_info.get('anchor_text', '') if source_info else ''
            }
            
        except requests.exceptions.Timeout:
            print(f"  ✗ Timeout")
            return {'url': url, 'status': 'timeout', 'error': 'Request timed out'}
        except Exception as e:
            print(f"  ✗ Error: {str(e)}")
            return {'url': url, 'status': 'error', 'error': str(e)}
    
    def extract_all(self, max_depth: int = 1, max_links: int = 20):
        """Extract PDF and follow hyperlinks"""
        print(f"\n{'='*80}")
        print("FULL EXTRACTION - FOLLOWING PDF HYPERLINKS")
        print(f"Max depth: {max_depth}, Max links per level: {max_links}")
        print('='*80)
        
        # Extract PDF
        pdf_data = self.extract_pdf_content()
        
        # Store PDF content with FULL TEXT
        self.all_content['_source_pdf'] = {
            'file': self.pdf_path,
            'full_text': pdf_data['markdown_text'],  # Keep full text
            'text_length': len(pdf_data['markdown_text']),
            'total_hyperlinks': len(pdf_data['all_hyperlinks']),
            'zavanta_links': len(pdf_data['zavanta_links']),
            'hyperlink_details': pdf_data['all_hyperlinks'],  # Keep all hyperlink info
            'procedure_references': pdf_data['procedure_references']
        }
        
        # Show PDF text preview
        print(f"\n📄 PDF Text Preview (first 1000 chars):")
        print("-" * 80)
        print(pdf_data['markdown_text'][:1000])
        print("-" * 80)
        print(f"... ({len(pdf_data['markdown_text'])} total characters)")
        print()
        
        # Follow Zavanta/Premera hyperlinks first (most relevant)
        print(f"\n{'='*80}")
        print(f"FOLLOWING ZAVANTA/PREMERA LINKS ({len(pdf_data['zavanta_links'])} found)")
        print('='*80)
        
        links_to_process = pdf_data['zavanta_links'][:max_links]
        
        for i, link_info in enumerate(links_to_process, 1):
            print(f"\n[{i}/{len(links_to_process)}]")
            result = self.fetch_url(link_info['url'], link_info)
            
            if result:
                key = f"link_{i}_{link_info['url'].split('/')[-1][:30]}"
                self.all_content[key] = result
            
            time.sleep(0.5)  # Be respectful
        
        # Optionally follow child links (depth 2)
        if max_depth > 1:
            print(f"\n{'='*80}")
            print(f"FOLLOWING CHILD LINKS (DEPTH 2)")
            print('='*80)
            
            child_urls = []
            for content in self.all_content.values():
                if isinstance(content, dict) and content.get('child_links'):
                    child_urls.extend([c['url'] for c in content['child_links'][:3]])
            
            child_urls = list(set(child_urls))[:10]  # Max 10 child links
            
            for i, url in enumerate(child_urls, 1):
                if url not in self.visited_urls:
                    print(f"\n[Child {i}/{len(child_urls)}]")
                    result = self.fetch_url(url)
                    if result:
                        key = f"child_{i}_{url.split('/')[-1][:30]}"
                        self.all_content[key] = result
                    time.sleep(0.5)
        
        return self.all_content
    
    def save_results(self, output_file: str):
        """Save to JSON"""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.all_content, f, indent=2, ensure_ascii=False)
        print(f"\n✓ Saved: {output_file}")
    
    def save_pdf_text(self, output_file: str):
        """Save just the PDF text to a separate file"""
        if '_source_pdf' not in self.all_content:
            return
        
        pdf_data = self.all_content['_source_pdf']
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write(f"PDF TEXT EXTRACTION: {pdf_data['file']}\n")
            f.write("="*80 + "\n\n")
            f.write(pdf_data.get('full_text', 'No text extracted'))
        
        print(f"✓ Saved PDF text: {output_file}")
    
    def save_linked_documents_text(self, output_file: str):
        """Save text from all linked documents to a separate file"""
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("TEXT FROM ALL HYPERLINKED DOCUMENTS\n")
            f.write("="*80 + "\n\n")
            
            doc_count = 0
            for key, content in self.all_content.items():
                if key == '_source_pdf':
                    continue
                
                if content.get('status') == 'success':
                    doc_count += 1
                    f.write(f"\n{'='*80}\n")
                    f.write(f"DOCUMENT #{doc_count}\n")
                    f.write('='*80 + "\n\n")
                    f.write(f"URL: {content.get('url', 'N/A')}\n")
                    f.write(f"Title: {content.get('title', 'N/A')}\n")
                    if content.get('source_anchor'):
                        f.write(f"Link Text: {content['source_anchor']}\n")
                    f.write(f"Content Length: {content.get('text_length', 0)} characters\n")
                    f.write(f"\n{'-'*80}\n")
                    f.write("FULL TEXT CONTENT:\n")
                    f.write('-'*80 + "\n\n")
                    f.write(content.get('text', 'No text extracted'))
                    f.write(f"\n\n{'-'*80}\n")
                    f.write("END OF DOCUMENT\n")
                    f.write('-'*80 + "\n\n")
            
            if doc_count == 0:
                f.write("No documents were successfully fetched.\n")
            else:
                f.write(f"\n{'='*80}\n")
                f.write(f"TOTAL: {doc_count} documents extracted\n")
                f.write('='*80 + "\n")
        
        print(f"✓ Saved linked documents text: {output_file}")
    
    def save_summary(self, output_file: str):
        """Save readable summary"""
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("PREMERA DOCUMENT EXTRACTION SUMMARY\n")
            f.write("="*80 + "\n\n")
            
            if '_source_pdf' in self.all_content:
                pdf_data = self.all_content['_source_pdf']
                f.write(f"Source PDF: {pdf_data['file']}\n")
                f.write(f"Text length: {pdf_data.get('text_length', 0)} characters\n")
                f.write(f"Total hyperlinks in PDF: {pdf_data.get('total_hyperlinks', 0)}\n")
                f.write(f"Zavanta links: {pdf_data.get('zavanta_links', 0)}\n")
                f.write(f"Procedure references in text: {len(pdf_data.get('procedure_references', []))}\n\n")
                
                # Write FULL PDF TEXT
                f.write("="*80 + "\n")
                f.write("SOURCE PDF - FULL TEXT CONTENT\n")
                f.write("="*80 + "\n\n")
                f.write(pdf_data.get('full_text', 'No text extracted'))
                f.write("\n\n")
                
                # Write hyperlinks list
                f.write("="*80 + "\n")
                f.write("HYPERLINKS FOUND IN PDF\n")
                f.write("="*80 + "\n\n")
                for i, link in enumerate(pdf_data.get('hyperlink_details', []), 1):
                    f.write(f"{i}. Page {link['page']}: {link['url']}\n")
                    if link.get('anchor_text'):
                        f.write(f"   Text: {link['anchor_text']}\n")
                    f.write("\n")
            
            f.write(f"\nDocuments fetched from hyperlinks: {len([k for k in self.all_content.keys() if k != '_source_pdf'])}\n\n")
            
            f.write("="*80 + "\n")
            f.write("FETCHED DOCUMENTS FROM HYPERLINKS\n")
            f.write("="*80 + "\n\n")
            
            for key, content in self.all_content.items():
                if key == '_source_pdf':
                    continue
                
                f.write(f"\n{'-'*80}\n")
                f.write(f"Document Key: {key}\n")
                f.write(f"URL: {content.get('url', 'N/A')}\n")
                f.write(f"Status: {content.get('status', 'unknown')}\n")
                
                if content.get('status') == 'success':
                    f.write(f"Title: {content.get('title', 'N/A')}\n")
                    if content.get('source_anchor'):
                        f.write(f"Link text from PDF: {content['source_anchor']}\n")
                    f.write(f"Content length: {content.get('text_length', 0)} characters\n")
                    f.write(f"\nFull Content:\n")
                    f.write("-"*80 + "\n")
                    text = content.get('text', '')
                    f.write(text)  # Write FULL text, not truncated
                    f.write("\n" + "-"*80 + "\n")
                    f.write(f"\nChild links found: {len(content.get('child_links', []))}\n")
                    if content.get('child_links'):
                        f.write("Child links:\n")
                        for child in content.get('child_links', []):
                            f.write(f"  - {child['url']}\n")
                            if child.get('text'):
                                f.write(f"    Text: {child['text']}\n")
                    f.write(f"\nProcedure references: {len(content.get('procedure_references', []))}\n")
                    if content.get('procedure_references'):
                        f.write(f"Procedures: {', '.join(content['procedure_references'])}\n")
                else:
                    f.write(f"Error: {content.get('error', 'Unknown')}\n")
        
        print(f"✓ Saved: {output_file}")

def main():
    print("="*80)
    print("PREMERA HYPERLINK EXTRACTOR - FOLLOWS ACTUAL PDF LINKS")
    print("="*80)
    
    # Configuration
    username = os.getenv('PREMERA_USERNAME', 'hareesha.thippaih@premera.com')
    password = os.getenv('PREMERA_PASSWORD', 'Narasamma@65')
    login_url = os.getenv('PREMERA_LOGIN_URL', 'https://premera.zavanta.com/portal/login')
    
    # PDF file
    pdf_file = input("Enter PDF filename (or press Enter for 'BC - Determine If BlueCard Claim2 - P966.pdf'): ").strip()
    if not pdf_file:
        pdf_file = "BC - Determine If BlueCard Claim2 - P966.pdf"
    
    print(f"\n📄 PDF: {pdf_file}")
    print(f"👤 User: {username}")
    print(f"🔒 SSL: COMPLETELY DISABLED")
    
    if not os.path.exists(pdf_file):
        print(f"\n❌ PDF not found: {pdf_file}")
        return
    
    # Initialize
    extractor = PremeraDocumentExtractor(pdf_file)
    
    # Login
    print(f"\n{'='*80}")
    print("LOGGING IN")
    print('='*80)
    
    extractor.login_to_portal(login_url, username, password)
    
    # Extract
    results = extractor.extract_all(max_depth=1, max_links=20)
    
    # Save all outputs
    os.makedirs('output', exist_ok=True)
    extractor.save_pdf_text('output/1_pdf_source_text.txt')
    extractor.save_linked_documents_text('output/2_linked_documents_text.txt')
    extractor.save_results('output/3_complete_data.json')
    extractor.save_summary('output/4_full_summary.txt')
    
    # Summary
    successful = sum(1 for c in results.values() 
                    if c != results.get('_source_pdf') and c.get('status') == 'success')
    failed = sum(1 for c in results.values() 
                if c != results.get('_source_pdf') and c.get('status') in ['error', 'timeout', 'auth_required'])
    
    print(f"\n{'='*80}")
    print("✅ EXTRACTION COMPLETE!")
    print('='*80)
    print(f"\n📊 Statistics:")
    print(f"   PDF text: {results['_source_pdf'].get('text_length', 0)} characters")
    print(f"   Hyperlinks in PDF: {results['_source_pdf'].get('total_hyperlinks', 0)}")
    print(f"   Documents fetched: {len(results) - 1}")
    print(f"   ✓ Successful: {successful}")
    print(f"   ✗ Failed: {failed}")
    print(f"\n📁 Output Files (numbered for easy access):")
    print(f"   1️⃣  output/1_pdf_source_text.txt")
    print(f"      → Original PDF text only")
    print(f"\n   2️⃣  output/2_linked_documents_text.txt  ⭐")
    print(f"      → Full text from all {successful} hyperlinked documents")
    print(f"      → Easy to read, one document after another")
    print(f"\n   3️⃣  output/3_complete_data.json")
    print(f"      → All data in structured JSON format")
    print(f"\n   4️⃣  output/4_full_summary.txt")
    print(f"      → Complete summary with everything")
    
    if successful > 0:
        print(f"\n✨ TEXT FROM HYPERLINKS IS IN FILE #2!")
        print(f"   Open: output/2_linked_documents_text.txt")
        print(f"   Contains full text from {successful} documents")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
