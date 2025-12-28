import pymupdf4llm
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
        
    def login_to_portal(self, login_url: str, username: str, password: str, 
                       login_data: Dict = None, method: str = 'auto'):
        """
        Login to the portal with credentials
        
        Args:
            login_url: URL of the login page
            username: Username/email
            password: Password
            login_data: Custom form data (optional)
            method: 'auto', 'form', or 'basic'
        """
        try:
            print(f"Attempting login to {login_url}...")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            if method == 'basic':
                # HTTP Basic Authentication
                self.session.auth = (username, password)
                response = self.session.get(login_url, headers=headers)
                
            else:
                # Form-based authentication
                # First, get the login page to find form fields
                response = self.session.get(login_url, headers=headers)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find the login form
                form = soup.find('form')
                if not form:
                    print("Warning: No form found on login page")
                    return False
                
                # Prepare login data
                if login_data is None:
                    # Auto-detect common field names
                    login_data = {}
                    
                    # Common username field names
                    username_fields = ['username', 'user', 'email', 'login', 'userid', 'user_name']
                    password_fields = ['password', 'pass', 'pwd']
                    
                    # Find input fields
                    for input_tag in form.find_all('input'):
                        name = input_tag.get('name', '').lower()
                        input_type = input_tag.get('type', '').lower()
                        
                        # Check for username field
                        if any(field in name for field in username_fields) or input_type == 'email':
                            login_data[input_tag.get('name')] = username
                        # Check for password field
                        elif any(field in name for field in password_fields) or input_type == 'password':
                            login_data[input_tag.get('name')] = password
                        # Include hidden fields (like CSRF tokens)
                        elif input_type == 'hidden':
                            login_data[input_tag.get('name')] = input_tag.get('value', '')
                
                # Get form action URL
                action = form.get('action', '')
                if action:
                    login_post_url = urljoin(login_url, action)
                else:
                    login_post_url = login_url
                
                print(f"Posting login data to: {login_post_url}")
                print(f"Form fields: {list(login_data.keys())}")
                
                # Submit login form
                response = self.session.post(
                    login_post_url,
                    data=login_data,
                    headers=headers,
                    allow_redirects=True
                )
            
            # Check if login was successful
            if response.status_code == 200:
                # Check for common success indicators
                if 'logout' in response.text.lower() or 'sign out' in response.text.lower():
                    self.authenticated = True
                    print("✓ Login successful!")
                    return True
                elif 'error' in response.text.lower() or 'invalid' in response.text.lower():
                    print("✗ Login failed - check credentials")
                    return False
                else:
                    # Assume success if no error indicators
                    self.authenticated = True
                    print("✓ Login appears successful (verify manually if needed)")
                    return True
            else:
                print(f"✗ Login failed with status code: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"✗ Login error: {str(e)}")
            return False
    
    def extract_pdf_content(self):
        """Extract text and hyperlinks from PDF"""
        print("Extracting PDF content...")
        
        # Get markdown text
        markdown_text = pymupdf4llm.to_markdown(self.pdf_path)
        
        # Get all hyperlinks
        doc = fitz.open(self.pdf_path)
        links = []
        
        for page_num, page in enumerate(doc, start=1):
            for link in page.get_links():
                url = link.get('uri', '')
                if url and url.startswith('http'):
                    # Get anchor text
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
        
        return {
            'markdown_text': markdown_text,
            'links': links
        }
    
    def fetch_url_content(self, url: str, timeout: int = 30) -> Dict:
        """Fetch content from a URL using authenticated session"""
        try:
            print(f"Fetching: {url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
            }
            
            # Use session (maintains cookies/auth)
            response = self.session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            response.raise_for_status()
            
            # Check if we got redirected to login page
            if 'login' in response.url.lower() and url.lower() not in response.url.lower():
                return {
                    'url': url,
                    'status': 'auth_required',
                    'error': 'Redirected to login page - authentication may have expired'
                }
            
            # Parse HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()
            
            # Get text content
            text = soup.get_text(separator='\n', strip=True)
            
            # Extract child links
            child_links = []
            base_domain = urlparse(url).netloc
            
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                full_url = urljoin(url, href)
                
                # Only include links from same domain
                if urlparse(full_url).netloc == base_domain:
                    child_links.append({
                        'url': full_url,
                        'text': a_tag.get_text(strip=True)
                    })
            
            return {
                'url': url,
                'status': 'success',
                'text': text,
                'title': soup.title.string if soup.title else '',
                'child_links': child_links,
                'final_url': response.url
            }
            
        except requests.exceptions.Timeout:
            return {'url': url, 'status': 'timeout', 'error': 'Request timed out'}
        except requests.exceptions.ConnectionError:
            return {'url': url, 'status': 'connection_error', 'error': 'Could not connect'}
        except requests.exceptions.HTTPError as e:
            return {'url': url, 'status': 'http_error', 'error': str(e)}
        except Exception as e:
            return {'url': url, 'status': 'error', 'error': str(e)}
    
    def extract_child_links(self, parent_url: str, max_depth: int = 1, current_depth: int = 0):
        """Recursively extract content from child links"""
        if current_depth >= max_depth or parent_url in self.visited_urls:
            return
        
        self.visited_urls.add(parent_url)
        
        # Fetch parent content
        content = self.fetch_url_content(parent_url)
        self.all_content[parent_url] = content
        
        # Small delay to be respectful
        time.sleep(1)
        
        # Process child links if successful
        if content['status'] == 'success' and current_depth < max_depth:
            for child in content.get('child_links', [])[:10]:  # Limit to 10 child links per page
                child_url = child['url']
                if child_url not in self.visited_urls:
                    print(f"  → Child link (depth {current_depth + 1}): {child_url}")
                    self.extract_child_links(child_url, max_depth, current_depth + 1)
    
    def process_all_links(self, max_depth: int = 1, domain_filter: str = None):
        """Process all links from PDF"""
        pdf_data = self.extract_pdf_content()
        
        print(f"\nFound {len(pdf_data['links'])} links in PDF")
        print("=" * 80)
        
        # Filter links if domain specified
        links_to_process = pdf_data['links']
        if domain_filter:
            links_to_process = [l for l in links_to_process if domain_filter in l['url']]
            print(f"Filtered to {len(links_to_process)} links matching '{domain_filter}'")
        
        # Process each link
        for link_data in links_to_process:
            url = link_data['url']
            print(f"\nPage {link_data['page']}: {url}")
            if link_data['anchor_text']:
                print(f"Anchor text: {link_data['anchor_text']}")
            
            self.extract_child_links(url, max_depth=max_depth)
        
        return {
            'pdf_content': pdf_data,
            'fetched_content': self.all_content
        }
    
    def save_results(self, output_file: str = 'extracted_content.json'):
        """Save all extracted content to JSON"""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.all_content, f, indent=2, ensure_ascii=False)
        print(f"\nResults saved to {output_file}")
    
    def save_text_summary(self, output_file: str = 'extracted_summary.txt'):
        """Save a readable text summary"""
        with open(output_file, 'w', encoding='utf-8') as f:
            for url, content in self.all_content.items():
                f.write(f"\n{'=' * 80}\n")
                f.write(f"URL: {url}\n")
                f.write(f"Status: {content['status']}\n")
                
                if content['status'] == 'success':
                    f.write(f"Title: {content.get('title', 'N/A')}\n")
                    f.write(f"\nContent:\n{'-' * 80}\n")
                    f.write(content['text'][:2000])
                    if len(content['text']) > 2000:
                        f.write(f"\n... (truncated, total length: {len(content['text'])} chars)")
                    f.write(f"\n\nChild links found: {len(content.get('child_links', []))}\n")
                else:
                    f.write(f"Error: {content.get('error', 'Unknown')}\n")
        
        print(f"Text summary saved to {output_file}")


# Example usage
if __name__ == "__main__":
    # Your credentials
    PREMERA_USERNAME = "your_username"
    PREMERA_PASSWORD = "your_password"
    PREMERA_LOGIN_URL = "https://premera.zavanta.com/login"  # Adjust if needed
    
    pdf_path = "your_document.pdf"
    
    # Initialize extractor
    extractor = PDFHyperlinkExtractor(pdf_path)
    
    # Login to Premera portal
    login_success = extractor.login_to_portal(
        login_url=PREMERA_LOGIN_URL,
        username=PREMERA_USERNAME,
        password=PREMERA_PASSWORD,
        method='auto'  # Try 'form' or 'basic' if auto doesn't work
    )
    
    if login_success:
        print("\n" + "=" * 80)
        print("Starting content extraction...")
        print("=" * 80)
        
        # Process Premera links
        results = extractor.process_all_links(
            max_depth=1,
            domain_filter="premera.zavanta.com"
        )
        
        # Save results
        extractor.save_results('premera_content.json')
        extractor.save_text_summary('premera_summary.txt')
        
        # Print summary
        print("\n" + "=" * 80)
        print("EXTRACTION COMPLETE")
        print("=" * 80)
        print(f"Total URLs processed: {len(extractor.all_content)}")
        
        successful = sum(1 for c in extractor.all_content.values() if c['status'] == 'success')
        print(f"Successful: {successful}")
        print(f"Failed: {len(extractor.all_content) - successful}")
    else:
        print("\n✗ Could not login. Please check credentials and login URL.")
        print("\nTroubleshooting tips:")
        print("1. Verify the login URL is correct")
        print("2. Check username and password")
        print("3. Try method='form' or method='basic' in login_to_portal()")
        print("4. Check if the site uses captcha or 2FA")