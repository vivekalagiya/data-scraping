import argparse
import os
import json
import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from datetime import date

def download_image(img_url,folder):
    os.makedirs(folder,exist_ok=True)

    filename = img_url.split("/")[-1]
    filepath = os.path.join (folder,filename)

    r = requests.get (img_url)
    with open(filepath, "wb") as f: f.write(r.content)
    print("Image saved:",filepath)


# -------------------------------
# Parse command line arguments
# -------------------------------
def parse_arguments():
    parser = argparse.ArgumentParser(description="Web Data Extraction Tool")

    parser.add_argument("--url", required=True, help="URL of page")
    parser.add_argument("--out", required=True, help="Output directory")

    return parser.parse_args()


# -------------------------------
# Download page
# -------------------------------
def fetch_page(url):

    try:
        response = requests.get(url)

        if response.status_code == 200:
            return response.text
        else:
            print("Failed to download page")
            return None

    except Exception as e:
        print("Error:", e)
        return None


# -------------------------------
# Detect page type
# -------------------------------
# -------------------------------
# Detect page type
# -------------------------------
def detect_page_type(soup, url):

    # Product page detection
    if "features" in url or "soldering-iron" in url:
        return "product"

    # Category page detection
    links = soup.find_all("a")

    if len(links) > 20:
        return "category"

    return "unknown"


# -------------------------------
# Create category structure
# -------------------------------
def create_category_structure(base):

    tables = os.path.join(base, "tables")
    markdowns = os.path.join(base, "markdowns")

    os.makedirs(tables, exist_ok=True)
    os.makedirs(markdowns, exist_ok=True)

    # create empty files
    with open(os.path.join(tables, "products.json"), "w") as f:
        json.dump({}, f, indent=4)

    with open(os.path.join(tables, "metadata.json"), "w") as f:
        json.dump([], f, indent=4)


# -------------------------------
# Create product structure
# -------------------------------
def create_product_structure(base):

    folders = [
        "documentation",
        "images",
        "block_diagrams",
        "design_resources",
        "software_tools",
        "tables",
        "markdowns",
        "trainings",
        "other"
    ]

    for folder in folders:
        os.makedirs(os.path.join(base, folder), exist_ok=True)

    # create metadata.json in folders
    for folder in folders:
        path = os.path.join(base, folder, "metadata.json")

        with open(path, "w") as f:
            json.dump([], f, indent=4)


# -------------------------------
# Create markdown file
# -------------------------------
def create_markdown(html, output):

    markdown_text = md(html)

    md_path = os.path.join(output, "markdowns", "overview.md")

    os.makedirs(os.path.join(output, "markdowns"), exist_ok=True)

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(markdown_text)


# -------------------------------
# Main
# -------------------------------
def main():

    args = parse_arguments()

    print("Downloading page...")

    html = fetch_page(args.url)

    if html is None:
        return

    soup = BeautifulSoup(html, "lxml")
    img = soup.find ("img")

    if img:
        img_url = img.get("src")
        download_image(img_url,"output/images")
        
    page_type = detect_page_type(soup, args.url)

    print("Detected page type:", page_type)

    if page_type == "category":

        print("Creating category folder structure")

        create_category_structure(args.out)

    elif page_type == "product":

        print("Creating product folder structure")

        create_product_structure(args.out)

    else:

        print("Page type unknown")

    create_markdown(html, args.out)

    print("Markdown file created")


if __name__ == "__main__":
    main()