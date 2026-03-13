# import os
# import json
# import argparse
# import requests
# from bs4 import BeautifulSoup
# from markdownify import markdownify as md
# from urllib.parse import urljoin

# HEADERS = {
#     "User-Agent": "Mozilla/5.0"
# }


# # create folder
# def create_folder(path):
#     os.makedirs(path, exist_ok=True)


# # create metadata file
# def save_metadata(folder):
#     metadata_path = os.path.join(folder, "metadata.json")
#     if not os.path.exists(metadata_path):
#         with open(metadata_path, "w", encoding="utf-8") as f:
#             json.dump([], f, indent=4)


# # detect page type
# def detect_page_type(url):
#     if "details.aspx" in url:
#         return "category"
#     elif "info.aspx" in url:
#         return "product"
#     else:
#         return "product"


# # CATEGORY PAGE HANDLER
# def handle_category(url, out_dir):

#     res = requests.get(url, headers=HEADERS)
#     soup = BeautifulSoup(res.text, "html.parser")

#     tables_dir = os.path.join(out_dir, "tables")
#     markdown_dir = os.path.join(out_dir, "markdowns")

#     create_folder(tables_dir)
#     create_folder(markdown_dir)

#     save_metadata(tables_dir)
#     save_metadata(markdown_dir)

#     data = []

#     tables = soup.find_all("table")

#     for table in tables:
#         rows = table.find_all("tr")

#         for row in rows[1:]:
#             cols = row.find_all("td")

#             if len(cols) >= 2:
#                 item = {
#                     "name": cols[0].get_text(strip=True),
#                     "value": cols[1].get_text(strip=True)
#                 }
#                 data.append(item)

#     with open(os.path.join(tables_dir, "products.json"), "w", encoding="utf-8") as f:
#         json.dump(data, f, indent=4)

#     markdown_content = md(str(soup))

#     with open(os.path.join(markdown_dir, "overview.md"), "w", encoding="utf-8") as f:
#         f.write(markdown_content)


# # IMAGE DOWNLOADER
# def download_product_image(soup, base_url, folder):

#     img = soup.find("img")

#     if not img:
#         return

#     src = img.get("src")

#     if not src:
#         return

#     img_url = urljoin(base_url, src)

#     try:
#         r = requests.get(img_url, headers=HEADERS, timeout=15)

#         if r.status_code == 200:

#             path = os.path.join(folder, "product.jpeg")

#             with open(path, "wb") as f:
#                 f.write(r.content)

#             print("Product image downloaded")

#     except:
#         print("Image download skipped")


# # PRODUCT PAGE HANDLER
# def handle_product(url, out_dir):

#     res = requests.get(url, headers=HEADERS)
#     soup = BeautifulSoup(res.text, "html.parser")

#     folders = [
#         "documentation",
#         "images",
#         "block_diagrams",
#         "design_resources",
#         "software_tools",
#         "tables",
#         "markdowns",
#         "trainings",
#         "other",
#     ]

#     for folder in folders:
#         path = os.path.join(out_dir, folder)
#         create_folder(path)
#         save_metadata(path)

#     images_dir = os.path.join(out_dir, "images")

#     download_product_image(soup, url, images_dir)

#     markdown_content = md(str(soup))

#     with open(os.path.join(out_dir, "markdowns", "overview.md"), "w", encoding="utf-8") as f:
#         f.write(markdown_content)


# # MAIN FUNCTION
# def main():

#     parser = argparse.ArgumentParser()

#     parser.add_argument("--url", required=True)
#     parser.add_argument("--out", required=True)

#     args = parser.parse_args()

#     page_type = detect_page_type(args.url)

#     if page_type == "category":
#         handle_category(args.url, args.out)
#     else:
#         handle_product(args.url, args.out)


# if __name__ == "__main__":
#     main()

# #python Crawl.py --url "https://www.chogori-tech.com/product/details.aspx?lcid=32" --out output/category
# #python Crawl.py --url "https://www.chogori-tech.com/product/info.aspx?itemid=487&lcid=65" --out output/part 


import os
import json
import argparse
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from markdownify import markdownify as md

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def create_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)


def save_metadata(folder):
    with open(os.path.join(folder, "metadata.json"), "w") as f:
        json.dump([], f, indent=4)


def detect_page_type(url):
    if "details.aspx" in url:
        return "category"
    elif "info.aspx" in url:
        return "product"
    return "product"


# ✅ MAX 3 IMAGE DOWNLOAD
def download_images(soup, base_url, folder):

    create_folder(folder)

    images = soup.find_all("img")

    count = 0

    for img in images:

        if count >= 3:   # ⭐ IMPORTANT LIMIT
            break

        src = img.get("src")

        if not src:
            continue

        img_url = urljoin(base_url, src)

        try:
            r = requests.get(img_url, headers=HEADERS, timeout=10)

            if r.status_code == 200 and "image" in r.headers.get("Content-Type", ""):

                filename = f"image_{count}.jpeg"
                path = os.path.join(folder, filename)

                with open(path, "wb") as f:
                    f.write(r.content)

                print("Image downloaded:", filename)

                count += 1

        except:
            pass


def extract_tables(soup, folder):

    tables = soup.find_all("table")

    data = []

    for table in tables:

        rows = table.find_all("tr")

        for row in rows:

            cols = row.find_all("td")

            if len(cols) >= 2:

                key = cols[0].text.strip()
                value = cols[1].text.strip()

                data.append({key: value})

    with open(os.path.join(folder, "specifications.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


def handle_product(url, out_dir):

    print("Crawling product:", url)

    try:
        res = requests.get(url, headers=HEADERS, timeout=20)
    except:
        print("Network error")
        return

    soup = BeautifulSoup(res.text, "html.parser")

    folders = [
        "documentation",
        "images",
        "block_diagrams",
        "design_resources",
        "software_tools",
        "tables",
        "markdowns",
        "trainings",
        "other",
    ]

    for folder in folders:

        path = os.path.join(out_dir, folder)

        create_folder(path)

        save_metadata(path)

    # ⭐ Download images (max 3)
    download_images(soup, url, os.path.join(out_dir, "images"))

    # ⭐ Extract tables
    extract_tables(soup, os.path.join(out_dir, "tables"))

    # ⭐ Markdown documentation
    markdown_content = md(str(soup))

    with open(os.path.join(out_dir, "markdowns", "overview.md"), "w", encoding="utf-8") as f:
        f.write(markdown_content)

    print("Product crawl finished\n")


def handle_category(url, out_dir):

    print("Crawling category:", url)

    try:
        res = requests.get(url, headers=HEADERS, timeout=20)
    except:
        print("Network error")
        return

    soup = BeautifulSoup(res.text, "html.parser")

    links = soup.find_all("a")

    product_links = []

    for link in links:

        href = link.get("href")

        if href and "info.aspx" in href:

            full_url = urljoin(url, href)

            product_links.append(full_url)

    product_links = list(set(product_links))

    print("Products found:", len(product_links))

    for i, product_url in enumerate(product_links):

        product_folder = os.path.join(out_dir, f"product_{i}")

        handle_product(product_url, product_folder)


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--url", required=True)

    parser.add_argument("--out", required=True)

    args = parser.parse_args()

    page_type = detect_page_type(args.url)

    if page_type == "category":

        handle_category(args.url, args.out)

    else:

        handle_product(args.url, args.out)


if __name__ == "__main__":
    main()