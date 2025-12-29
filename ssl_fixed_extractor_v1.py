"""
Premera FULL Extractor - FIXED Session Handling
- Auto-restarts browser if it crashes
- Skips external/non-Zavanta URLs
- Better error handling
"""
import pymupdf4llm
import pymupdf as fitz
from selenium import webdriver
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import InvalidSessionIdException, WebDriverException, TimeoutException
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
        self.skipped_urls = []
        self.requests_count = 0
        self.max_requests_before_restart = 50  # Restart browser every 50 requests
        self.headless = True
        
        # Allowed domains - only extract from these
        self.allowed_domains = [
            'premera.zavanta.com',
            'zavanta.com'
        ]
        
        # Skip these domains
        self.skip_domains = [
            'sharepoint.com',
            'microsoft.com',
            'office.com',
            'google.com',
            'youtube.com'
        ]
    
    def is_allowed_url(self, url: str) -> bool:
        """Check if URL should be processed"""
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        # Check if should skip
        for skip in self.skip_domains:
            if skip in domain:
                return False
        
        # Check if allowed
        for allowed in self.allowed_domains:
            if allowed in domain:
                return True
        
        return False
    
    def setup_browser(self, headless: bool = True):
        """Setup Edge browser"""
        self.headless = headless
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
        options.add_argument('--disable-features=VizDisplayCompositor')
        options.add_argument('--disable-extensions')
        options.add_argument('--no-first-run')
        options.add_argument('--disable-default-apps')
        # Reduce memory usage
        options.add_argument('--disable-background-networking')
        options.add_argument('--disable-sync')
        options.add_argument('--disable-translate')
        options.add_argument('--single-process')
        
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        try:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
            
            self.driver = webdriver.Edge(options=options)
            self.driver.set_page_load_timeout(60)
            self.requests_count = 0
            print("✓ Browser ready")
            return True
        except Exception as e:
            print(f"✗ Browser setup failed: {e}")
            return False
    
    def restart_browser(self):
        """Restart browser to clear memory and fix session issues"""
        print("\n🔄 Restarting browser...")
        try:
            if self.driver:
                self.driver.quit()
        except:
            pass
        
        time.sleep(2)
        return self.setup_browser(self.headless)
    
    def is_browser_alive(self):
        """Check if browser session is still valid"""
        try:
            _ = self.driver.current_url
            return True
        except:
            return False
    
    def login_to_portal(self, login_url: str, username: str, password: str):
        """Login to portal"""
        try:
            print(f"\nNavigating to: {login_url}")
            self.driver.get(login_url)
            time.sleep(5)
            
            print(f"Current URL: {self.driver.current_url}")
            
            # Look for login fields
            username_selectors = [
                'input[name*="user"]', 'input[name*="email"]',
                'input[type="email"]', '#username', '#email'
            ]
            
            password_selectors = [
                'input[name*="pass"]', 'input[type="password"]', '#password'
            ]
            
            username_field = None
            for sel in username_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, sel)
                    for el in elements:
                        if el.is_displayed():
                            username_field = el
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
                            break
                    if password_field:
                        break
                except:
                    continue
            
            if username_field and password_field:
                print("Entering credentials...")
                username_field.clear()
                username_field.send_keys(username)
                time.sleep(0.5)
                
                password_field.clear()
                password_field.send_keys(password)
                time.sleep(0.5)
                
                # Find submit
                for sel in ['button[type="submit"]', 'input[type="submit"]', 'button']:
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, sel)
                        for btn in elements:
                            if btn.is_displayed() and btn.is_enabled():
                                btn.click()
                                time.sleep(5)
                                break
                        break
                    except:
                        continue
                
                print("✓ Login submitted")
            else:
                print("⚠️ No login form - might be SSO or already logged in")
            
            return True
            
        except Exception as e:
            print(f"⚠️ Login note: {e}")
            return True
    
    def extract_pdf_content(self):
        """Extract PDF text and hyperlinks"""
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
        
        # Filter hyperlinks - only Zavanta URLs
        zavanta_links = [l for l in hyperlinks if self.is_allowed_url(l['url'])]
        skipped_links = [l for l in hyperlinks if not self.is_allowed_url(l['url'])]
        
        print(f"✓ PDF text: {len(markdown_text):,} characters")
        print(f"✓ Total hyperlinks: {len(hyperlinks)}")
        print(f"✓ Zavanta links (will process): {len(zavanta_links)}")
        print(f"⊗ External links (skipped): {len(skipped_links)}")
        
        if skipped_links:
            print(f"\nSkipped external URLs:")
            for link in skipped_links[:5]:
                print(f"   - {link['url'][:60]}")
                self.skipped_urls.append(link['url'])
            if len(skipped_links) > 5:
                print(f"   ... and {len(skipped_links) - 5} more")
        
        print(f"\nZavanta links to process:")
        for i, link in enumerate(zavanta_links[:10], 1):
            print(f"   {i}. {link['url'][:70]}")
        if len(zavanta_links) > 10:
            print(f"   ... and {len(zavanta_links) - 10} more")
        
        return {'markdown_text': markdown_text, 'hyperlinks': zavanta_links}
    
    def extract_text_from_page(self):
        """Extract ALL text from current page"""
        try:
            time.sleep(2)
            
            # Scroll to load content
            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                self.driver.execute_script("window.scrollTo(0, 0);")
            except:
                pass
            
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Remove unwanted
            for tag in soup.find_all(['script', 'style', 'noscript', 'svg', 'path']):
                tag.decompose()
            
            # Try content containers
            text = ""
            for selector in ['main', 'article', '#content', '.content', 
                           '.document-content', '.page-content', '[role="main"]']:
                try:
                    content = soup.select_one(selector)
                    if content:
                        for unwanted in content.find_all(['header', 'footer', 'nav']):
                            unwanted.decompose()
                        text = content.get_text(separator='\n', strip=True)
                        if len(text) > 200:
                            break
                except:
                    continue
            
            # Fallback to body
            if len(text) < 200:
                body = soup.find('body')
                if body:
                    for unwanted in body.find_all(['header', 'footer', 'nav', 'aside']):
                        unwanted.decompose()
                    text = body.get_text(separator='\n', strip=True)
            
            # Clean
            text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
            text = text.strip()
            
            return text, page_source
            
        except Exception as e:
            print(f"   Text extraction error: {e}")
            return "", ""
    
    def get_child_links(self, base_url: str, page_source: str):
        """Get child links - only from allowed domains"""
        links = []
        seen = set()
        
        soup = BeautifulSoup(page_source, 'html.parser')
        
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            
            if not href or href.startswith('#') or href.startswith('javascript:'):
                continue
            
            full_url = urljoin(base_url, href)
            parsed = urlparse(full_url)
            clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if parsed.query:
                clean_url += f"?{parsed.query}"
            
            # Only allowed domains
            if not self.is_allowed_url(clean_url):
                continue
            
            if clean_url in seen or clean_url in self.visited_urls:
                continue
            
            seen.add(clean_url)
            links.append({
                'url': clean_url,
                'text': a_tag.get_text(strip=True)[:100]
            })
        
        return links
    
    def fetch_url(self, url: str, depth: int = 0, source: str = ""):
        """Fetch URL with session recovery"""
        if url in self.visited_urls:
            return None
        
        # Check if URL is allowed
        if not self.is_allowed_url(url):
            print(f"   ⊗ Skipping external URL: {url[:50]}")
            self.skipped_urls.append(url)
            return None
        
        self.visited_urls.add(url)
        self.requests_count += 1
        
        # Restart browser periodically
        if self.requests_count >= self.max_requests_before_restart:
            print(f"\n🔄 Browser restart (processed {self.requests_count} requests)")
            if not self.restart_browser():
                print("   ✗ Failed to restart browser")
                return None
        
        # Check if browser is alive
        if not self.is_browser_alive():
            print(f"\n⚠️ Browser session lost - restarting...")
            if not self.restart_browser():
                print("   ✗ Failed to restart browser")
                return None
        
        indent = "  " * min(depth, 3)
        self.total_extracted += 1
        doc_num = self.total_extracted
        
        print(f"\n{indent}[{doc_num}] D{depth}: {url[:65]}...")
        
        try:
            self.driver.get(url)
            
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except TimeoutException:
                print(f"{indent}   ⚠️ Page load timeout - continuing anyway")
            
            text, page_source = self.extract_text_from_page()
            child_links = self.get_child_links(url, page_source)
            
            print(f"{indent}   ✓ Text: {len(text):,} chars | Children: {len(child_links)}")
            
            if len(text) > 0:
                preview = text[:80].replace('\n', ' ')
                print(f"{indent}   Preview: {preview}...")
            
            return {
                'doc_number': doc_num,
                'url': url,
                'depth': depth,
                'source': source,
                'status': 'success',
                'title': self.driver.title if self.is_browser_alive() else '',
                'text': text,
                'text_length': len(text),
                'child_links': child_links,
                'child_links_count': len(child_links)
            }
            
        except InvalidSessionIdException as e:
            print(f"{indent}   ⚠️ Session lost - restarting browser...")
            if self.restart_browser():
                # Retry this URL
                self.visited_urls.discard(url)
                self.total_extracted -= 1
                return self.fetch_url(url, depth, source)
            else:
                self.failed_urls.append({'url': url, 'error': 'Session restart failed'})
                return None
                
        except WebDriverException as e:
            error_msg = str(e)[:100]
            print(f"{indent}   ✗ WebDriver error: {error_msg}")
            
            if 'invalid session' in error_msg.lower():
                print(f"{indent}   🔄 Restarting browser...")
                if self.restart_browser():
                    self.visited_urls.discard(url)
                    self.total_extracted -= 1
                    return self.fetch_url(url, depth, source)
            
            self.failed_urls.append({'url': url, 'error': error_msg})
            return None
            
        except Exception as e:
            print(f"{indent}   ✗ Error: {str(e)[:80]}")
            self.failed_urls.append({'url': url, 'error': str(e)[:100]})
            return None
    
    def extract_all(self, max_depth: int = 3):
        """Extract everything"""
        print(f"\n{'='*80}")
        print("FULL RECURSIVE EXTRACTION")
        print(f"Max depth: {max_depth}")
        print(f"Allowed domains: {', '.join(self.allowed_domains)}")
        print('='*80)
        
        # Extract PDF
        pdf_data = self.extract_pdf_content()
        
        self.all_content['_source_pdf'] = {
            'file': self.pdf_path,
            'full_text': pdf_data['markdown_text'],
            'text_length': len(pdf_data['markdown_text']),
            'hyperlinks_count': len(pdf_data['hyperlinks'])
        }
        
        # Queue
        queue = []
        for link in pdf_data['hyperlinks']:
            queue.append({
                'url': link['url'],
                'depth': 0,
                'source': f"PDF page {link['page']}"
            })
        
        print(f"\n{'='*80}")
        print(f"STARTING - {len(queue)} Zavanta links to process")
        print('='*80)
        
        while queue:
            remaining = len(queue)
            if remaining % 10 == 0 or remaining < 10:
                print(f"\n--- Queue: {remaining} | Extracted: {self.total_extracted} | Failed: {len(self.failed_urls)} ---")
            
            item = queue.pop(0)
            url = item['url']
            depth = item['depth']
            source = item['source']
            
            if depth > max_depth:
                continue
            
            if url in self.visited_urls:
                continue
            
            result = self.fetch_url(url, depth, source)
            
            if result:
                key = f"doc_{result['doc_number']:04d}"
                self.all_content[key] = result
                
                # Add children
                if result.get('child_links'):
                    new_children = 0
                    for child in result['child_links']:
                        if child['url'] not in self.visited_urls:
                            queue.append({
                                'url': child['url'],
                                'depth': depth + 1,
                                'source': f"Child of #{result['doc_number']}"
                            })
                            new_children += 1
                    
                    if new_children > 0:
                        print(f"   → +{new_children} children added")
            
            time.sleep(0.3)
        
        print(f"\n{'='*80}")
        print("EXTRACTION COMPLETE")
        print(f"Total: {self.total_extracted} | Failed: {len(self.failed_urls)} | Skipped: {len(self.skipped_urls)}")
        print('='*80)
        
        return self.all_content
    
    def save_all_text(self, output_file: str):
        """Save all text"""
        total_chars = 0
        doc_count = 0
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("ALL EXTRACTED TEXT\n")
            f.write("="*80 + "\n\n")
            
            # PDF
            if '_source_pdf' in self.all_content:
                pdf = self.all_content['_source_pdf']
                f.write("="*80 + "\n")
                f.write(f"SOURCE PDF: {pdf['file']}\n")
                f.write(f"Text: {pdf['text_length']:,} chars\n")
                f.write("-"*80 + "\n\n")
                f.write(pdf.get('full_text', ''))
                f.write("\n\n")
                total_chars += pdf.get('text_length', 0)
            
            # Documents
            docs = [(k, v) for k, v in self.all_content.items() 
                   if k != '_source_pdf' and isinstance(v, dict) and v.get('status') == 'success']
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
                f.write(f"Text: {len(text):,} chars\n")
                f.write("-"*80 + "\n\n")
                f.write(text)
                f.write("\n\n")
            
            # Summary
            f.write("\n" + "="*80 + "\n")
            f.write("SUMMARY\n")
            f.write("="*80 + "\n")
            f.write(f"Documents: {doc_count}\n")
            f.write(f"Total chars: {total_chars:,}\n")
            f.write(f"Failed: {len(self.failed_urls)}\n")
            f.write(f"Skipped (external): {len(self.skipped_urls)}\n")
            
            if self.failed_urls:
                f.write("\nFailed URLs:\n")
                for fail in self.failed_urls[:20]:
                    f.write(f"   - {fail['url'][:60]}: {fail.get('error', '')[:50]}\n")
            
            if self.skipped_urls:
                f.write("\nSkipped external URLs:\n")
                for url in self.skipped_urls[:20]:
                    f.write(f"   - {url[:70]}\n")
        
        print(f"\n✓ Saved: {output_file}")
        print(f"   Documents: {doc_count} | Text: {total_chars:,} chars")
    
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
    print("PREMERA FULL EXTRACTOR - WITH SESSION RECOVERY")
    print("- Auto-restarts browser if it crashes")
    print("- Skips external URLs (SharePoint, etc.)")
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
        headless = input("\nRun hidden? (Y/n): ").strip().lower() != 'n'
        extractor.setup_browser(headless=headless)
        
        extractor.login_to_portal(login_url, username, password)
        
        max_depth = input("\nMax depth (Enter for 3): ").strip()
        max_depth = int(max_depth) if max_depth else 3
        
        results = extractor.extract_all(max_depth=max_depth)
        
        os.makedirs('output', exist_ok=True)
        extractor.save_all_text('output/ALL_EXTRACTED_TEXT.txt')
        extractor.save_json('output/complete_data.json')
        
        successful = sum(1 for k, c in results.items() 
                        if k != '_source_pdf' and isinstance(c, dict) and c.get('status') == 'success')
        total_text = sum(c.get('text_length', 0) for c in results.values() 
                        if isinstance(c, dict) and c.get('text_length'))
        
        print(f"\n{'='*80}")
        print("✅ COMPLETE!")
        print('='*80)
        print(f"\n📊 Results:")
        print(f"   • Extracted: {successful}")
        print(f"   • Total text: {total_text:,} chars")
        print(f"   • Failed: {len(extractor.failed_urls)}")
        print(f"   • Skipped: {len(extractor.skipped_urls)}")
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
