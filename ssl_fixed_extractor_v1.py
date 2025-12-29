"""
Premera FULL Extractor - Extracts ALL Links and Child Links
Fixed to follow EVERY link recursively
"""
import pymupdf4llm
import pymupdf as fitz
from selenium import webdriver
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json
import time
import os
import re
from dotenv import load_dotenv

load_dotenv()

class PremeraFullExtractor:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.visited_urls = set()
        self.all_content = {}
        self.driver = None
        self.total_extracted = 0
        self.failed_urls = []
        
    def setup_browser(self, headless: bool = True):
        """Setup Edge browser"""
        print("Setting up Edge browser...")
        
        options = EdgeOptions()
        if headless:
            options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-ssl-errors')
        options.add_argument('--disable-web-security')
        options.add_argument('--allow-running-insecure-content')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        self.driver = webdriver.Edge(options=options)
        self.driver.set_page_load_timeout(120)
        print("✓ Browser ready")
    
    def login_to_portal(self, login_url: str, username: str, password: str):
        """Login to portal"""
        try:
            print(f"\nNavigating to: {login_url}")
            self.driver.get(login_url)
            time.sleep(5)
            
            print(f"Current URL: {self.driver.current_url}")
            print(f"Page title: {self.driver.title}")
            
            # Look for login fields
            username_selectors = [
                'input[name*="user"]', 'input[name*="User"]',
                'input[name*="email"]', 'input[name*="Email"]',
                'input[type="email"]', '#username', '#email',
                'input[id*="user"]', 'input[id*="User"]',
                'input[id*="email"]', 'input[id*="Email"]',
                'input[placeholder*="user"]', 'input[placeholder*="email"]'
            ]
            
            password_selectors = [
                'input[name*="pass"]', 'input[name*="Pass"]',
                'input[type="password"]', '#password',
                'input[id*="pass"]', 'input[id*="Pass"]'
            ]
            
            username_field = None
            for sel in username_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, sel)
                    for el in elements:
                        if el.is_displayed():
                            username_field = el
                            print(f"Found username field: {sel}")
                            break
                    if username_field:
                        break
                except:
                    continue
            
            password_field = None
            for sel in password_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, sel)
                    for el in elements:
                        if el.is_displayed():
                            password_field = el
                            print(f"Found password field: {sel}")
                            break
                    if password_field:
                        break
                except:
                    continue
            
            if username_field and password_field:
                print("Entering credentials...")
                username_field.clear()
                username_field.send_keys(username)
                time.sleep(1)
                
                password_field.clear()
                password_field.send_keys(password)
                time.sleep(1)
                
                # Find submit button
                submit_selectors = [
                    'button[type="submit"]', 'input[type="submit"]',
                    'button[id*="login"]', 'button[id*="Login"]',
                    'input[value*="Login"]', 'input[value*="Sign"]',
                    'button[class*="login"]', 'button[class*="submit"]',
                    'button', 'input[type="button"]'
                ]
                
                for sel in submit_selectors:
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, sel)
                        for btn in elements:
                            if btn.is_displayed() and btn.is_enabled():
                                print(f"Clicking submit: {sel}")
                                btn.click()
                                time.sleep(5)
                                break
                        break
                    except:
                        continue
                
                print(f"After login URL: {self.driver.current_url}")
                print("✓ Login attempted")
            else:
                print("⚠️ No login form found - might be SSO or already logged in")
            
            return True
            
        except Exception as e:
            print(f"⚠️ Login note: {e}")
            return True
    
    def extract_pdf_content(self):
        """Extract PDF text and ALL hyperlinks"""
        print(f"\n{'='*80}")
        print("EXTRACTING PDF")
        print('='*80)
        
        markdown_text = pymupdf4llm.to_markdown(self.pdf_path)
        doc = fitz.open(self.pdf_path)
        
        hyperlinks = []
        seen_urls = set()
        
        for page_num, page in enumerate(doc, start=1):
            for link in page.get_links():
                url = link.get('uri', '')
                if url and url.startswith('http') and url not in seen_urls:
                    seen_urls.add(url)
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
        
        print(f"✓ PDF text: {len(markdown_text):,} characters")
        print(f"✓ Unique hyperlinks: {len(hyperlinks)}")
        
        print(f"\nAll hyperlinks in PDF:")
        for i, link in enumerate(hyperlinks, 1):
            print(f"   {i}. Page {link['page']}: {link['url'][:80]}")
        
        return {'markdown_text': markdown_text, 'hyperlinks': hyperlinks}
    
    def extract_text_from_page(self):
        """Extract ALL text from current page"""
        try:
            # Wait for page load
            time.sleep(3)
            
            # Scroll to load all content
            try:
                last_height = self.driver.execute_script("return document.body.scrollHeight")
                while True:
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1)
                    new_height = self.driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(1)
            except:
                pass
            
            # Get rendered HTML
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Remove unwanted tags
            for tag in soup.find_all(['script', 'style', 'noscript', 'svg', 'path', 'meta', 'link']):
                tag.decompose()
            
            # Try to find main content
            text = ""
            content_found = False
            
            # Method 1: Common content containers
            for selector in ['main', 'article', '#content', '.content', 
                           '#main-content', '.main-content', '.document-content',
                           '.page-content', '.body-content', '[role="main"]',
                           '.procedure', '.policy', '.zavanta']:
                try:
                    content = soup.select_one(selector)
                    if content:
                        for unwanted in content.find_all(['header', 'footer', 'nav', 'aside']):
                            unwanted.decompose()
                        text = content.get_text(separator='\n', strip=True)
                        if len(text) > 200:
                            content_found = True
                            break
                except:
                    continue
            
            # Method 2: Body without header/footer
            if not content_found or len(text) < 200:
                body = soup.find('body')
                if body:
                    for unwanted in body.find_all(['header', 'footer', 'nav', 'aside', 'script', 'style']):
                        unwanted.decompose()
                    text = body.get_text(separator='\n', strip=True)
            
            # Method 3: All divs
            if len(text) < 200:
                all_text = []
                for div in soup.find_all(['div', 'p', 'section', 'article', 'td', 'li']):
                    div_text = div.get_text(strip=True)
                    if len(div_text) > 20:
                        all_text.append(div_text)
                text = '\n'.join(all_text)
            
            # Clean up
            text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
            text = text.strip()
            
            return text, page_source
            
        except Exception as e:
            print(f"   Text extraction error: {e}")
            return "", ""
    
    def get_all_links_from_page(self, base_url: str, page_source: str):
        """Extract ALL links from page"""
        links = []
        seen = set()
        
        soup = BeautifulSoup(page_source, 'html.parser')
        base_domain = urlparse(base_url).netloc
        
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            
            # Skip empty, javascript, anchor links
            if not href or href.startswith('#') or href.startswith('javascript:'):
                continue
            
            # Build full URL
            full_url = urljoin(base_url, href)
            
            # Parse URL
            parsed = urlparse(full_url)
            
            # Only same domain
            if base_domain not in parsed.netloc:
                continue
            
            # Clean URL (remove fragments)
            clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if parsed.query:
                clean_url += f"?{parsed.query}"
            
            # Skip if already seen or visited
            if clean_url in seen or clean_url in self.visited_urls:
                continue
            
            seen.add(clean_url)
            link_text = a_tag.get_text(strip=True)[:100]
            
            links.append({
                'url': clean_url,
                'text': link_text
            })
        
        return links
    
    def fetch_url(self, url: str, depth: int = 0, source: str = ""):
        """Fetch and extract FULL content from URL"""
        # Skip if already visited
        if url in self.visited_urls:
            return None
        
        self.visited_urls.add(url)
        
        indent = "  " * depth
        self.total_extracted += 1
        doc_num = self.total_extracted
        
        print(f"\n{indent}[{doc_num}] Depth {depth}: {url[:70]}...")
        
        try:
            self.driver.get(url)
            
            # Wait for page
            try:
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except:
                pass
            
            # Extract text
            text, page_source = self.extract_text_from_page()
            
            # Get child links
            child_links = self.get_all_links_from_page(url, page_source)
            
            print(f"{indent}   ✓ Text: {len(text):,} chars | Child links: {len(child_links)}")
            
            if len(text) > 0:
                preview = text[:100].replace('\n', ' ')
                print(f"{indent}   Preview: {preview}...")
            
            result = {
                'doc_number': doc_num,
                'url': url,
                'depth': depth,
                'source': source,
                'status': 'success',
                'title': self.driver.title,
                'text': text,
                'text_length': len(text),
                'child_links': child_links,
                'child_links_count': len(child_links)
            }
            
            return result
            
        except Exception as e:
            print(f"{indent}   ✗ Error: {str(e)}")
            self.failed_urls.append({'url': url, 'error': str(e)})
            return None
    
    def extract_all(self, max_depth: int = 3, follow_all_children: bool = True):
        """Extract EVERYTHING - all links and all child links"""
        print(f"\n{'='*80}")
        print("FULL RECURSIVE EXTRACTION")
        print(f"Max depth: {max_depth}")
        print(f"Follow ALL child links: {follow_all_children}")
        print('='*80)
        
        # Extract PDF
        pdf_data = self.extract_pdf_content()
        
        self.all_content['_source_pdf'] = {
            'file': self.pdf_path,
            'full_text': pdf_data['markdown_text'],
            'text_length': len(pdf_data['markdown_text']),
            'hyperlinks_count': len(pdf_data['hyperlinks'])
        }
        
        # Build initial queue from PDF links
        queue = []
        for link in pdf_data['hyperlinks']:
            queue.append({
                'url': link['url'],
                'depth': 0,
                'source': f"PDF page {link['page']}"
            })
        
        print(f"\n{'='*80}")
        print(f"STARTING EXTRACTION - {len(queue)} initial links")
        print('='*80)
        
        while queue:
            # Show queue status
            print(f"\n--- Queue: {len(queue)} remaining ---")
            
            item = queue.pop(0)
            url = item['url']
            depth = item['depth']
            source = item['source']
            
            # Skip if too deep
            if depth > max_depth:
                print(f"   Skipping (too deep): {url[:50]}")
                continue
            
            # Fetch the page
            result = self.fetch_url(url, depth, source)
            
            if result:
                # Save result
                key = f"doc_{result['doc_number']:04d}"
                self.all_content[key] = result
                
                # Add ALL child links to queue
                if follow_all_children and result.get('child_links'):
                    child_links = result['child_links']
                    new_children = 0
                    
                    for child in child_links:
                        child_url = child['url']
                        if child_url not in self.visited_urls:
                            queue.append({
                                'url': child_url,
                                'depth': depth + 1,
                                'source': f"Child of doc_{result['doc_number']:04d}"
                            })
                            new_children += 1
                    
                    if new_children > 0:
                        print(f"   → Added {new_children} child links to queue")
            
            # Small delay
            time.sleep(0.3)
        
        print(f"\n{'='*80}")
        print(f"EXTRACTION COMPLETE")
        print(f"Total documents: {self.total_extracted}")
        print(f"Failed URLs: {len(self.failed_urls)}")
        print('='*80)
        
        return self.all_content
    
    def save_all_text(self, output_file: str):
        """Save ALL extracted text"""
        total_chars = 0
        doc_count = 0
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("ALL EXTRACTED TEXT - COMPLETE\n")
            f.write("="*80 + "\n\n")
            
            # PDF text
            if '_source_pdf' in self.all_content:
                pdf = self.all_content['_source_pdf']
                f.write("="*80 + "\n")
                f.write("SOURCE PDF\n")
                f.write("="*80 + "\n")
                f.write(f"File: {pdf['file']}\n")
                f.write(f"Text: {pdf['text_length']:,} characters\n")
                f.write("-"*80 + "\n\n")
                f.write(pdf.get('full_text', ''))
                f.write("\n\n")
                total_chars += pdf.get('text_length', 0)
            
            # All documents sorted by doc number
            docs = [(k, v) for k, v in self.all_content.items() 
                   if k != '_source_pdf' and v.get('status') == 'success']
            docs.sort(key=lambda x: x[1].get('doc_number', 0))
            
            for key, content in docs:
                doc_count += 1
                text = content.get('text', '')
                total_chars += len(text)
                
                f.write("\n" + "="*80 + "\n")
                f.write(f"DOCUMENT #{content.get('doc_number', doc_count)}\n")
                f.write("="*80 + "\n")
                f.write(f"URL: {content.get('url', 'N/A')}\n")
                f.write(f"Title: {content.get('title', 'N/A')}\n")
                f.write(f"Depth: {content.get('depth', 0)}\n")
                f.write(f"Source: {content.get('source', 'N/A')}\n")
                f.write(f"Text: {len(text):,} characters\n")
                f.write(f"Child links found: {content.get('child_links_count', 0)}\n")
                f.write("-"*80 + "\n\n")
                
                # FULL TEXT - NO TRUNCATION
                f.write(text)
                
                f.write("\n\n")
            
            # Summary
            f.write("\n" + "="*80 + "\n")
            f.write("EXTRACTION SUMMARY\n")
            f.write("="*80 + "\n")
            f.write(f"Total documents: {doc_count}\n")
            f.write(f"Total characters: {total_chars:,}\n")
            f.write(f"Failed URLs: {len(self.failed_urls)}\n")
            
            if self.failed_urls:
                f.write("\nFailed URLs:\n")
                for fail in self.failed_urls:
                    f.write(f"   - {fail['url']}: {fail['error']}\n")
        
        print(f"\n✓ Saved: {output_file}")
        print(f"   Documents: {doc_count}")
        print(f"   Total text: {total_chars:,} characters")
    
    def save_json(self, output_file: str):
        """Save JSON"""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.all_content, f, indent=2, ensure_ascii=False)
        print(f"✓ Saved: {output_file}")
    
    def close(self):
        """Close browser"""
        if self.driver:
            try:
                self.driver.quit()
                print("✓ Browser closed")
            except:
                pass

def main():
    print("="*80)
    print("PREMERA FULL EXTRACTOR")
    print("Extracts ALL links and ALL child links recursively")
    print("="*80)
    
    username = os.getenv('PREMERA_USERNAME', 'hareesha.thippaih@premera.com')
    password = os.getenv('PREMERA_PASSWORD', 'Narasamma@65')
    login_url = os.getenv('PREMERA_LOGIN_URL', 'https://premera.zavanta.com/portal/login')
    
    pdf_file = input("\nPDF filename (Enter for default): ").strip()
    if not pdf_file:
        pdf_file = "BC - Determine If BlueCard Claim2 - P966.pdf"
    
    if not os.path.exists(pdf_file):
        print(f"\n❌ Not found: {pdf_file}")
        return
    
    print(f"\n📄 PDF: {pdf_file}")
    print(f"👤 User: {username}")
    
    extractor = PremeraFullExtractor(pdf_file)
    
    try:
        # Browser setup
        headless = input("\nRun hidden? (Y/n): ").strip().lower() != 'n'
        extractor.setup_browser(headless=headless)
        
        # Login
        extractor.login_to_portal(login_url, username, password)
        
        # Settings
        print("\n⚙️ Extraction Settings:")
        
        max_depth = input("Max depth - how deep to follow links (Enter for 3): ").strip()
        max_depth = int(max_depth) if max_depth else 3
        
        follow_all = input("Follow ALL child links? (Y/n): ").strip().lower() != 'n'
        
        print(f"\n   Max depth: {max_depth}")
        print(f"   Follow all children: {follow_all}")
        
        # Extract
        results = extractor.extract_all(max_depth=max_depth, follow_all_children=follow_all)
        
        # Save
        os.makedirs('output', exist_ok=True)
        extractor.save_all_text('output/ALL_EXTRACTED_TEXT.txt')
        extractor.save_json('output/complete_data.json')
        
        # Summary
        successful = sum(1 for k, c in results.items() 
                        if k != '_source_pdf' and isinstance(c, dict) and c.get('status') == 'success')
        total_text = sum(c.get('text_length', 0) for c in results.values() 
                        if isinstance(c, dict) and c.get('text_length'))
        
        print(f"\n{'='*80}")
        print("✅ COMPLETE!")
        print('='*80)
        print(f"\n📊 Results:")
        print(f"   • Documents extracted: {successful}")
        print(f"   • Total text: {total_text:,} characters")
        print(f"   • URLs visited: {len(extractor.visited_urls)}")
        print(f"   • Failed: {len(extractor.failed_urls)}")
        print(f"\n📁 Output:")
        print(f"   ⭐ output/ALL_EXTRACTED_TEXT.txt")
        print(f"   📊 output/complete_data.json")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        extractor.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted")
