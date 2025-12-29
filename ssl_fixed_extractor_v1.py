"""
Premera Selenium Extractor - For JavaScript-Rendered Pages
Uses Selenium to render JS content before extracting text

Install: pip install selenium webdriver-manager
"""
import pymupdf4llm
import pymupdf as fitz
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json
import time
import os
import re
from dotenv import load_dotenv

load_dotenv()

class PremeraSeleniumExtractor:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.visited_urls = set()
        self.all_content = {}
        self.driver = None
        
    def setup_browser(self, headless: bool = True):
        """Setup Chrome browser"""
        print("Setting up browser...")
        
        options = Options()
        if headless:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-ssl-errors')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.set_page_load_timeout(60)
        
        print("✓ Browser ready")
    
    def login_to_portal(self, login_url: str, username: str, password: str):
        """Login using Selenium"""
        try:
            print(f"\nLogging in to {login_url}...")
            self.driver.get(login_url)
            time.sleep(3)
            
            # Try to find and fill login form
            try:
                # Find username field
                username_field = None
                for selector in ['input[name*="user"]', 'input[name*="email"]', 'input[type="email"]', '#username', '#email']:
                    try:
                        username_field = self.driver.find_element(By.CSS_SELECTOR, selector)
                        break
                    except:
                        continue
                
                # Find password field
                password_field = None
                for selector in ['input[name*="pass"]', 'input[type="password"]', '#password']:
                    try:
                        password_field = self.driver.find_element(By.CSS_SELECTOR, selector)
                        break
                    except:
                        continue
                
                if username_field and password_field:
                    username_field.clear()
                    username_field.send_keys(username)
                    time.sleep(0.5)
                    
                    password_field.clear()
                    password_field.send_keys(password)
                    time.sleep(0.5)
                    
                    # Find and click submit
                    for selector in ['button[type="submit"]', 'input[type="submit"]', 'button:contains("Login")', '.login-btn']:
                        try:
                            submit_btn = self.driver.find_element(By.CSS_SELECTOR, selector)
                            submit_btn.click()
                            break
                        except:
                            continue
                    
                    time.sleep(5)  # Wait for login to complete
                    print("✓ Login submitted")
                else:
                    print("⚠️ Could not find login form fields")
                    
            except Exception as e:
                print(f"⚠️ Login form issue: {e}")
            
            return True
            
        except Exception as e:
            print(f"✗ Login error: {e}")
            return False
    
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
        
        print(f"✓ PDF: {len(markdown_text)} chars, {len(hyperlinks)} links")
        
        return {'markdown_text': markdown_text, 'hyperlinks': hyperlinks}
    
    def fetch_url(self, url: str, depth: int = 0):
        """Fetch and extract text from URL using Selenium"""
        if url in self.visited_urls:
            return None
        
        self.visited_urls.add(url)
        
        try:
            indent = "  " * depth
            print(f"\n{indent}[Depth {depth}] {url[:70]}...")
            
            self.driver.get(url)
            
            # Wait for page to load
            time.sleep(3)
            
            # Wait for body to be present
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except:
                pass
            
            # Scroll to load lazy content
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            
            # Get page source after JS rendering
            page_source = self.driver.page_source
            
            # Extract text
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Remove unwanted elements
            for tag in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'noscript']):
                tag.decompose()
            
            # Try to get main content
            text = ""
            for selector in ['main', 'article', '.content', '#content', '.document-content', 'body']:
                content = soup.select_one(selector)
                if content:
                    text = content.get_text(separator='\n', strip=True)
                    if len(text) > 100:
                        break
            
            if not text:
                text = soup.get_text(separator='\n', strip=True)
            
            # Clean text
            text = re.sub(r'\n\s*\n', '\n\n', text)
            text = text.strip()
            
            # Get child links
            child_links = []
            base_domain = urlparse(url).netloc
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                full_url = urljoin(url, href)
                if base_domain in urlparse(full_url).netloc and full_url not in self.visited_urls:
                    if full_url not in [c['url'] for c in child_links]:
                        child_links.append({
                            'url': full_url,
                            'text': a_tag.get_text(strip=True)[:100]
                        })
            
            print(f"{indent}✓ Text: {len(text)} chars, Links: {len(child_links)}")
            
            # Preview
            preview = text[:150].replace('\n', ' ')
            print(f"{indent}   Preview: {preview}...")
            
            return {
                'url': url,
                'depth': depth,
                'status': 'success',
                'title': self.driver.title,
                'text': text,
                'text_length': len(text),
                'child_links': child_links
            }
            
        except Exception as e:
            print(f"{indent}✗ Error: {str(e)}")
            return None
    
    def extract_all(self, max_depth: int = 2, max_links: int = 10):
        """Extract everything"""
        print(f"\n{'='*80}")
        print("SELENIUM EXTRACTION")
        print('='*80)
        
        # Extract PDF
        pdf_data = self.extract_pdf_content()
        
        self.all_content['_source_pdf'] = {
            'file': self.pdf_path,
            'full_text': pdf_data['markdown_text'],
            'text_length': len(pdf_data['markdown_text'])
        }
        
        # Queue links
        to_visit = [{'url': l['url'], 'depth': 0} for l in pdf_data['hyperlinks']]
        
        print(f"\n{'='*80}")
        print(f"FETCHING {len(to_visit)} LINKS")
        print('='*80)
        
        processed = 0
        
        while to_visit:
            item = to_visit.pop(0)
            
            if item['url'] in self.visited_urls or item['depth'] > max_depth:
                continue
            
            result = self.fetch_url(item['url'], item['depth'])
            
            if result:
                processed += 1
                self.all_content[f"doc_{processed:03d}"] = result
                
                # Add child links
                if item['depth'] < max_depth and result.get('child_links'):
                    for child in result['child_links'][:max_links]:
                        if child['url'] not in self.visited_urls:
                            to_visit.append({
                                'url': child['url'],
                                'depth': item['depth'] + 1
                            })
        
        return self.all_content
    
    def save_all_text(self, output_file: str):
        """Save all text"""
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("ALL EXTRACTED TEXT (SELENIUM)\n")
            f.write("="*80 + "\n\n")
            
            # PDF
            if '_source_pdf' in self.all_content:
                pdf = self.all_content['_source_pdf']
                f.write("="*80 + "\n")
                f.write(f"SOURCE PDF: {pdf['file']}\n")
                f.write("="*80 + "\n\n")
                f.write(pdf.get('full_text', ''))
                f.write("\n\n")
            
            # Documents
            for key, content in self.all_content.items():
                if key == '_source_pdf' or content.get('status') != 'success':
                    continue
                
                f.write("="*80 + "\n")
                f.write(f"URL: {content.get('url')}\n")
                f.write(f"Title: {content.get('title')}\n")
                f.write(f"Text: {content.get('text_length')} chars\n")
                f.write("-"*80 + "\n\n")
                f.write(content.get('text', ''))
                f.write("\n\n")
        
        print(f"✓ Saved: {output_file}")
    
    def save_json(self, output_file: str):
        """Save JSON"""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.all_content, f, indent=2, ensure_ascii=False)
        print(f"✓ Saved: {output_file}")
    
    def close(self):
        """Close browser"""
        if self.driver:
            self.driver.quit()
            print("✓ Browser closed")

def main():
    print("="*80)
    print("PREMERA SELENIUM EXTRACTOR")
    print("For JavaScript-rendered pages")
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
    
    extractor = PremeraSeleniumExtractor(pdf_file)
    
    try:
        # Setup browser
        headless = input("Run headless? (Y/n): ").strip().lower() != 'n'
        extractor.setup_browser(headless=headless)
        
        # Login
        extractor.login_to_portal(login_url, username, password)
        
        # Extract
        results = extractor.extract_all(max_depth=2, max_links=10)
        
        # Save
        os.makedirs('output', exist_ok=True)
        extractor.save_all_text('output/ALL_TEXT_SELENIUM.txt')
        extractor.save_json('output/data_selenium.json')
        
        # Summary
        successful = sum(1 for k, c in results.items() 
                        if k != '_source_pdf' and c.get('status') == 'success')
        total_text = sum(c.get('text_length', 0) for c in results.values())
        
        print(f"\n{'='*80}")
        print("✅ COMPLETE!")
        print('='*80)
        print(f"Documents: {successful}")
        print(f"Total text: {total_text:,} characters")
        print(f"\n📁 Output:")
        print(f"   ⭐ output/ALL_TEXT_SELENIUM.txt")
        print(f"   📊 output/data_selenium.json")
        
    finally:
        extractor.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
