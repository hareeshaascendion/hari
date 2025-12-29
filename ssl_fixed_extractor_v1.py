"""
Premera Document Extractor - FOLLOWS ALL LINKS RECURSIVELY
Extracts text from PDF hyperlinks AND their child links
"""
import pymupdf4llm
import pymupdf as fitz
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
import json
import time
from typing import Dict, Set
from dotenv import load_dotenv
import os
import warnings

# DISABLE ALL SSL
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
        self.visited_urls: Set[str] = set()
        self.all_content = {}
        self.extraction_order = []
        
    def login_to_portal(self, login_url: str, username: str, password: str):
        """Login to Premera Zavanta portal"""
        try:
            print(f"Logging in to {login_url}...")
            print("⚠️  SSL verification is DISABLED")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            }
            
            response = self.session.get(login_url, headers=headers, verify=False, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')
            form = soup.find('form')
            
            if not form:
                print("✓ No login form - continuing")
                self.authenticated = True
                return True
            
            login_data = {}
            for input_tag in form.find_all('input'):
                name = input_tag.get('name', '')
                input_type = input_tag.get('type', '').lower()
                value = input_tag.get('value', '')
                
                if 'user' in name.lower() or 'email' in name.lower() or input_type == 'email':
                    login_data[name] = username
                elif 'pass' in name.lower() or input_type == 'password':
                    login_data[name] = password
                elif input_type == 'hidden':
                    login_data[name] = value
            
            action = form.get('action', '')
            login_url_post = urljoin(login_url, action) if action else login_url
            
            response = self.session.post(
                login_url_post,
                data=login_data,
                headers=headers,
                verify=False,
                allow_redirects=True,
                timeout=30
            )
            
            print(f"✓ Login completed (status: {response.status_code})")
            self.authenticated = True
            return True
                
        except Exception as e:
            print(f"✗ Login error: {str(e)}")
            self.authenticated = True
            return True
    
    def extract_pdf_content(self):
        """Extract text and hyperlinks from PDF"""
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
                    anchor_text = ''
                    if link.get('rect'):
                        rect = fitz.Rect(link['rect'])
                        anchor_text = page.get_text("text", clip=rect).strip()
                    
                    hyperlinks.append({
                        'page': page_num,
                        'url': url,
                        'anchor_text': anchor_text
                    })
        
        doc.close()
        
        print(f"\n✓ PDF extracted:")
        print(f"   • Text length: {len(markdown_text)} characters")
        print(f"   • Hyperlinks found: {len(hyperlinks)}")
        
        return {
            'markdown_text': markdown_text,
            'hyperlinks': hyperlinks
        }
    
    def fetch_url(self, url: str, depth: int = 0, source: str = "PDF"):
        """Fetch content from URL and extract text"""
        if url in self.visited_urls:
            return None
        
        self.visited_urls.add(url)
        
        try:
            indent = "  " * depth
            print(f"\n{indent}[Depth {depth}] Fetching: {url}")
            print(f"{indent}Source: {source}")
            
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
                print(f"{indent}✗ Failed (HTTP {response.status_code})")
                return None
            
            # Check for login redirect
            if 'login' in response.url.lower() and 'login' not in url.lower():
                print(f"{indent}⚠️  Redirected to login")
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Remove unwanted elements
            for element in soup(["script", "style", "nav", "footer", "header"]):
                element.decompose()
            
            # Extract text
            text = soup.get_text(separator='\n', strip=True)
            
            # Find child links
            child_links = []
            base_domain = urlparse(url).netloc
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                full_url = urljoin(url, href)
                
                # Only same domain and not already visited
                if base_domain in urlparse(full_url).netloc and full_url not in self.visited_urls:
                    child_links.append({
                        'url': full_url,
                        'text': a_tag.get_text(strip=True)[:100]
                    })
            
            # Remove duplicates
            seen = set()
            unique_child_links = []
            for link in child_links:
                if link['url'] not in seen:
                    seen.add(link['url'])
                    unique_child_links.append(link)
            
            print(f"{indent}✓ Success!")
            print(f"{indent}   • Text: {len(text)} characters")
            print(f"{indent}   • Child links: {len(unique_child_links)}")
            
            result = {
                'url': url,
                'depth': depth,
                'source': source,
                'status': 'success',
                'title': soup.title.string if soup.title else '',
                'text': text,
                'text_length': len(text),
                'child_links': unique_child_links,
                'final_url': response.url
            }
            
            # Add to extraction order
            self.extraction_order.append(url)
            
            return result
            
        except requests.exceptions.Timeout:
            print(f"{indent}✗ Timeout")
            return None
        except Exception as e:
            print(f"{indent}✗ Error: {str(e)}")
            return None
    
    def extract_all(self, max_depth: int = 2, max_links_per_page: int = 10):
        """Extract PDF and follow ALL links recursively"""
        print(f"\n{'='*80}")
        print("RECURSIVE EXTRACTION - FOLLOWING ALL LINKS")
        print(f"Max depth: {max_depth}, Max child links per page: {max_links_per_page}")
        print('='*80)
        
        # Extract PDF
        pdf_data = self.extract_pdf_content()
        
        # Store PDF content
        self.all_content['_source_pdf'] = {
            'file': self.pdf_path,
            'full_text': pdf_data['markdown_text'],
            'text_length': len(pdf_data['markdown_text']),
            'total_hyperlinks': len(pdf_data['hyperlinks'])
        }
        
        # Queue for BFS (breadth-first search)
        to_visit = []
        for link in pdf_data['hyperlinks']:
            to_visit.append({
                'url': link['url'],
                'depth': 0,
                'source': f"PDF page {link['page']}"
            })
        
        print(f"\n{'='*80}")
        print(f"STARTING RECURSIVE EXTRACTION")
        print(f"Initial links to visit: {len(to_visit)}")
        print('='*80)
        
        processed = 0
        
        while to_visit:
            item = to_visit.pop(0)
            url = item['url']
            depth = item['depth']
            source = item['source']
            
            # Skip if already visited or too deep
            if url in self.visited_urls or depth > max_depth:
                continue
            
            # Fetch content
            result = self.fetch_url(url, depth, source)
            
            if result:
                processed += 1
                key = f"doc_{processed:03d}_depth{depth}"
                self.all_content[key] = result
                
                # Add child links to queue if not at max depth
                if depth < max_depth and result.get('child_links'):
                    child_links_to_add = result['child_links'][:max_links_per_page]
                    print(f"   → Adding {len(child_links_to_add)} child links to queue")
                    
                    for child in child_links_to_add:
                        to_visit.append({
                            'url': child['url'],
                            'depth': depth + 1,
                            'source': f"Child of: {url[:50]}..."
                        })
            
            time.sleep(0.5)  # Be respectful
        
        print(f"\n{'='*80}")
        print(f"EXTRACTION COMPLETE - Processed {processed} documents")
        print('='*80)
        
        return self.all_content
    
    def save_pdf_text(self, output_file: str):
        """Save PDF text"""
        if '_source_pdf' not in self.all_content:
            return
        
        pdf_data = self.all_content['_source_pdf']
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write(f"SOURCE PDF TEXT: {pdf_data['file']}\n")
            f.write("="*80 + "\n\n")
            f.write(pdf_data.get('full_text', ''))
        
        print(f"✓ Saved: {output_file}")
    
    def save_all_extracted_text(self, output_file: str):
        """Save text from ALL extracted documents"""
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("ALL EXTRACTED TEXT FROM HYPERLINKS AND CHILD LINKS\n")
            f.write("="*80 + "\n\n")
            
            # Group by depth
            by_depth = {}
            for key, content in self.all_content.items():
                if key == '_source_pdf' or content.get('status') != 'success':
                    continue
                depth = content.get('depth', 0)
                if depth not in by_depth:
                    by_depth[depth] = []
                by_depth[depth].append((key, content))
            
            # Write by depth
            for depth in sorted(by_depth.keys()):
                docs = by_depth[depth]
                f.write(f"\n{'='*80}\n")
                f.write(f"DEPTH {depth} - {len(docs)} DOCUMENTS\n")
                f.write('='*80 + "\n\n")
                
                for i, (key, content) in enumerate(docs, 1):
                    f.write(f"\n{'-'*80}\n")
                    f.write(f"DOCUMENT #{i} (Depth {depth})\n")
                    f.write(f"Key: {key}\n")
                    f.write('-'*80 + "\n\n")
                    f.write(f"URL: {content.get('url', 'N/A')}\n")
                    f.write(f"Source: {content.get('source', 'N/A')}\n")
                    f.write(f"Title: {content.get('title', 'N/A')}\n")
                    f.write(f"Text Length: {content.get('text_length', 0)} characters\n")
                    f.write(f"\n{'='*80}\n")
                    f.write("FULL TEXT:\n")
                    f.write('='*80 + "\n\n")
                    f.write(content.get('text', ''))
                    f.write(f"\n\n{'='*80}\n")
                    f.write("END OF DOCUMENT\n")
                    f.write('='*80 + "\n\n")
            
            # Summary
            total = sum(len(docs) for docs in by_depth.values())
            f.write(f"\n{'='*80}\n")
            f.write(f"EXTRACTION SUMMARY\n")
            f.write('='*80 + "\n")
            f.write(f"Total documents extracted: {total}\n")
            for depth in sorted(by_depth.keys()):
                f.write(f"  Depth {depth}: {len(by_depth[depth])} documents\n")
        
        print(f"✓ Saved: {output_file}")
    
    def save_results(self, output_file: str):
        """Save JSON"""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.all_content, f, indent=2, ensure_ascii=False)
        print(f"✓ Saved: {output_file}")
    
    def save_extraction_map(self, output_file: str):
        """Save a visual map of what was extracted"""
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("EXTRACTION MAP - WHAT WAS FETCHED\n")
            f.write("="*80 + "\n\n")
            
            # Group by depth
            by_depth = {}
            for key, content in self.all_content.items():
                if key == '_source_pdf':
                    continue
                depth = content.get('depth', 0)
                if depth not in by_depth:
                    by_depth[depth] = []
                by_depth[depth].append(content)
            
            for depth in sorted(by_depth.keys()):
                docs = by_depth[depth]
                f.write(f"\n{'='*80}\n")
                f.write(f"DEPTH {depth} ({len(docs)} documents)\n")
                f.write('='*80 + "\n\n")
                
                for i, content in enumerate(docs, 1):
                    status = content.get('status', 'unknown')
                    icon = "✓" if status == 'success' else "✗"
                    f.write(f"{icon} {i}. {content.get('url', 'N/A')}\n")
                    f.write(f"   Source: {content.get('source', 'N/A')}\n")
                    if status == 'success':
                        f.write(f"   Text: {content.get('text_length', 0)} chars\n")
                        f.write(f"   Child links: {len(content.get('child_links', []))}\n")
                    f.write("\n")
        
        print(f"✓ Saved: {output_file}")

def main():
    print("="*80)
    print("PREMERA RECURSIVE EXTRACTOR")
    print("Follows PDF links AND their child links")
    print("="*80)
    
    # Configuration
    username = os.getenv('PREMERA_USERNAME', 'hareesha.thippaih@premera.com')
    password = os.getenv('PREMERA_PASSWORD', 'Narasamma@65')
    login_url = os.getenv('PREMERA_LOGIN_URL', 'https://premera.zavanta.com/portal/login')
    
    # PDF file
    pdf_file = input("\nEnter PDF filename (press Enter for default): ").strip()
    if not pdf_file:
        pdf_file = "BC - Determine If BlueCard Claim2 - P966.pdf"
    
    if not os.path.exists(pdf_file):
        print(f"\n❌ PDF not found: {pdf_file}")
        return
    
    # Settings
    print(f"\n⚙️  Extraction Settings:")
    max_depth = input("Max depth (press Enter for 2): ").strip()
    max_depth = int(max_depth) if max_depth else 2
    
    max_links = input("Max child links per page (press Enter for 10): ").strip()
    max_links = int(max_links) if max_links else 10
    
    print(f"\n📄 PDF: {pdf_file}")
    print(f"👤 User: {username}")
    print(f"🔒 SSL: DISABLED")
    print(f"📊 Max depth: {max_depth}")
    print(f"🔗 Max child links/page: {max_links}")
    
    # Initialize
    extractor = PremeraDocumentExtractor(pdf_file)
    
    # Login
    print(f"\n{'='*80}")
    print("LOGGING IN")
    print('='*80)
    extractor.login_to_portal(login_url, username, password)
    
    # Extract
    results = extractor.extract_all(max_depth=max_depth, max_links_per_page=max_links)
    
    # Save
    os.makedirs('output', exist_ok=True)
    extractor.save_pdf_text('output/1_pdf_text.txt')
    extractor.save_all_extracted_text('output/2_all_extracted_text.txt')
    extractor.save_extraction_map('output/3_extraction_map.txt')
    extractor.save_results('output/4_complete_data.json')
    
    # Summary
    successful = sum(1 for c in results.values() 
                    if c != results.get('_source_pdf') and c.get('status') == 'success')
    
    print(f"\n{'='*80}")
    print("✅ COMPLETE!")
    print('='*80)
    print(f"\n📊 Extracted:")
    print(f"   • PDF text: {results['_source_pdf'].get('text_length', 0)} chars")
    print(f"   • Documents: {successful}")
    print(f"   • Visited URLs: {len(extractor.visited_urls)}")
    
    print(f"\n📁 Output Files:")
    print(f"   1️⃣  output/1_pdf_text.txt")
    print(f"   2️⃣  output/2_all_extracted_text.txt  ⭐ ALL TEXT HERE!")
    print(f"   3️⃣  output/3_extraction_map.txt")
    print(f"   4️⃣  output/4_complete_data.json")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
