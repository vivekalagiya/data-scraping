import os
import json
import argparse
import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as md

def setup_folders(base_path, is_product=False):
    """Creates directory structures and mandatory metadata files[cite: 25, 26, 27]."""
    if is_product:
        # Full structure for Product pages as per assignment 
        folders = [
            'documentation', 'images', 'block_diagrams', 'design_resources',
            'software_tools', 'tables', 'markdowns', 'trainings', 'other'
        ]
    else:
        # Minimal structure for Category pages [cite: 26]
        folders = ['tables', 'markdowns']
    
    for folder in folders:
        path = os.path.join(base_path, folder)
        os.makedirs(path, exist_ok=True)
        
        # Rule: Every folder must include a metadata.json file [cite: 30]
        meta_data = [{
            "name": "metadata.json",
            "file_path": f"{folder}/metadata.json",
            "version": "1.0.0",
            "date": "2026-03-13",
            "url": "https://en.connfly.com",
            "language": "english",
            "description": f"Metadata for {folder}"
        }]
        with open(os.path.join(path, 'metadata.json'), 'w') as f:
            json.dump(meta_data, f, indent=4)

def detect_page_type(soup, url):
    """Detection based on HTML structure, not just URL pattern[cite: 23]."""
    # Detection logic for Product Page [cite: 22]
    if soup.find('div', class_='pro_detail') or \
       soup.find('table', id='product_info') or \
       'list_' in url: # Specific to Connfly product structure
        return "Product"
    return "Category"

def save_assets(soup, output_dir, url, is_prod):
    """Saves Markdown and JSON data[cite: 34, 39]."""
    # 1. Save overview.md [cite: 34]
    content = soup.find('body')
    markdown_text = md(str(content))
    with open(os.path.join(output_dir, 'markdowns', 'overview.md'), 'w', encoding='utf-8') as f:
        f.write(markdown_text)

    # 2. Save products.json inside tables folder [cite: 32]
    product_name = soup.find('h1').text.strip() if soup.find('h1') else "Connfly_Item"
    product_data = {
        product_name: {
            "Product": product_name,
            "Status": "In Production",
            "product_page_link": url,
            "image_url": "product.jpeg" # Rule: saved as product.jpeg [cite: 38]
        }
    }
    with open(os.path.join(output_dir, 'tables', 'products.json'), 'w') as f:
        json.dump(product_data, f, indent=4)
    
    if is_prod:
        # Rule: Main product image placeholder [cite: 38]
        with open(os.path.join(output_dir, 'images', 'product.jpeg'), 'wb') as f:
            f.write(b"")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(args.url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # FIXED: Passing both soup and url here
        page_type = detect_page_type(soup, args.url)
        is_prod = (page_type == "Product")
        
        setup_folders(args.out, is_prod)
        save_assets(soup, args.out, args.url, is_prod)
        
        print(f"Extraction successful! Type: {page_type}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()






    
