import os
import time
import curl_cffi.requests as requests
from curl_cffi.requests import RequestsError
from base64 import b64decode
import filetype
import json
import argparse
import sys
from bs4 import BeautifulSoup
from markdownify import markdownify as md
import logging
import random
import re
from urllib.parse import urljoin, urlparse, urlunparse, unquote, parse_qs
from PyPDF2 import PdfReader

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

CONFIG = {
    "verify_ssl": True  # Disable SSL verification for problematic sites
}

ZYTE_API_KEY = os.getenv("ZYTE_API_KEY")  # set this in your environment
ZYTE_API_URL = "https://api.zyte.com/v1/extract"

SITE_CONFIG = {

    # Category page: top-level category with subcategory links (no product grid)
    # Detected by: presence of subcategory nav tabs but NO product SKU cards
    "category": {
        "main_selector": ".rootCategory",          # top-level category container
        "markdown": [
            "h1.page-title",
            ".category-description",
            ".rootCategory"
        ],
        "documentation": []
    },

    # Group page: subcategory with a product listing grid
    # Detected by: product cards with SKU labels
    "group": {
        "main_selector": ".product-items",         # product grid container
        "product_container": ".product-item",      # each product card
        "markdown": [
            "h1.page-title",
            ".category-description"
        ],
        "documentation": [],
        "products": {
            "name": ".product-item-info .product-item-link",
            "sku": ".product-item-sku-value",
            "product_page_link": ".product-item-info .product-item-link::attr(href)",
            "pdf_link": "",
            "pdf_filename": "",
            "image_url": ".product-item-photo img::attr(src)",
            "pricing": ".price",
            "description": ".product-item-description"
        }
    },

    # Product (part) page: individual product detail page
    # Detected by: product detail section with SKU + specs
    "part": {
        "main_selector": ".product-view",          # product detail wrapper
        "markdown": [
            ".product-description",
            ".product-view"
        ],
        "images": [
            ".product-media img",
            ".product-image-photo"
        ],
        "documentation": [
            "a[href$='.pdf']"
        ],
        "products": {
            "name": "h1.page-title",
            "sku": ".product-sku-value",
            "product_page_link": "",
            "pdf_link": "a[href$='.pdf']::attr(href)",
            "pdf_filename": "",
            "image_url": ".product-media img::attr(src)",
            "pricing": ".product-price .price",
            "description": ".product-description"
        }
    }
}


def extract_value(element, selector):
    if not selector:
        return None

    if "::attr(" in selector:
        sel, attr = selector.split("::attr(")
        attr = attr.replace(")", "")
        tag = element.select_one(sel.strip())
        return tag.get(attr) if tag else None

    tag = element.select_one(selector)
    return tag.get_text(strip=True) if tag else None


class Category:
    def markdown(soup, url):
        markdown = {}
        overview = []
        for sel in SITE_CONFIG["category"]["markdown"]:
            cat_overview = Core.write_overview_markdown(soup, sel, "Category", url)
            overview.append(cat_overview)
        markdown['overview'] = overview
        return markdown

    def documentation(soup):
        documents = {}
        metadata = []
        for sel in SITE_CONFIG["category"]["documentation"]:
            for a in soup.select(sel):
                href = a["href"].strip()
                parsed = urlparse(href)
                path = parsed.path.lower()
                if not path.endswith(".pdf"):
                    continue
                url = a["href"].strip()
                parsed = urlparse(url)
                filename = parsed.path.split("/")[-1]
                metadata.append({
                    "name": filename,
                    "url": url,
                    "file_path": "",
                    "version": None,
                    "date": None,
                    "language": None,
                    "description": None
                })
        if metadata:
            documents["metadata"] = metadata
        return documents


# ---------- Group (Sub-category with product listings) ----------
class Group:
    def markdown(soup, url):
        markdown = {}
        overview = []
        for sel in SITE_CONFIG["group"]["markdown"]:
            cat_overview = Core.write_overview_markdown(soup, sel, "Category", url)
            overview.append(cat_overview)
        markdown["overview"] = overview
        return markdown

    def tables(soup, url):
        tables = {}
        products = []

        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        page_num = 1
        visited = set()
        while True:
            page_url = url if page_num == 1 else f"{url}?page={page_num}"
            if page_url in visited:
                break
            visited.add(page_url)

            html = Core.fetch_html(page_url, "request")
            if not html:
                break

            soup = BeautifulSoup(html, "html.parser")

            logger.info("---- DEBUG PAGE DETECTION ----")
            logger.info(f"Category selector found: {bool(soup.select_one('.category-view'))}")
            logger.info(f"Group selector found: {bool(soup.select_one('.products.wrapper.grid'))}")
            logger.info(f"Product selector found: {bool(soup.select_one('.product-info-main'))}")
            logger.info("-------------------------------")

            Core.fix_lazy_loaded_images(soup)
            container = SITE_CONFIG["group"].get("product_container")

            if not soup.select(container):
                break

            for div in soup.select(container):
                prod_url = extract_value(div, SITE_CONFIG["group"]["products"]["product_page_link"])
                if prod_url and not prod_url.startswith("http"):
                    prod_url = urljoin(base_url, prod_url)
                sku = extract_value(div, SITE_CONFIG["group"]["products"]["sku"])
                name = extract_value(div, SITE_CONFIG["group"]["products"]["name"])
                price = extract_value(div, SITE_CONFIG["group"]["products"]["pricing"])

                img_src = extract_value(div, SITE_CONFIG["group"]["products"]["image_url"])
                if img_src and not img_src.startswith("http"):
                    img_src = urljoin(base_url, img_src)

                pdflink = extract_value(div, SITE_CONFIG["group"]["products"]["pdf_link"])
                pdfname = pdflink.split("/")[-1] if pdflink else None

                product = {
                    "Product": sku or None,
                    "name": name or None,
                    "product_page_link": prod_url or None,
                    "pdf_link": pdflink,
                    "pdf_filename": pdfname,
                    "image_url": img_src,
                    "Pricing": price or None,
                }
                products.append(product)
            page_num += 1
            # Stop if no next page link exists
            if not soup.select_one(f'a[href*="page={page_num}"]'):
                break

        tables["products"] = products
        return tables

    def documentation(soup):
        documents = {}
        metadata = []
        for sel in SITE_CONFIG["group"]["documentation"]:
            for a in soup.select(sel):
                href = a["href"].strip()
                parsed = urlparse(href)
                path = parsed.path.lower()
                if not path.endswith(".pdf"):
                    continue
                url = a["href"].strip()
                parsed = urlparse(url)
                filename = parsed.path.split("/")[-1]
                metadata.append({
                    "name": filename,
                    "url": url,
                    "file_path": "",
                    "version": None,
                    "date": None,
                    "language": None,
                    "description": None
                })
        if metadata:
            documents["metadata"] = metadata
        return documents


class Product:
    def tables(soup, url):
        tables = {}
        products = []
        product_data = {}

        pull_right = soup.select_one("h1 .pull-right")
        if pull_right:
            pull_right.decompose()

        product_data["Product"] = extract_value(soup, SITE_CONFIG["part"]["products"]["sku"])
        product_data["name"] = extract_value(soup, SITE_CONFIG["part"]["products"]["name"])
        product_data["Pricing"] = extract_value(soup, SITE_CONFIG["part"]["products"]["pricing"])
        product_data["image_url"] = extract_value(soup, SITE_CONFIG["part"]["products"]["image_url"])
        product_data["product_page_link"] = url

        pdf_links = []
        pdf_names = []
        for a in soup.select(SITE_CONFIG["part"]["products"]["pdf_link"]):
            href = a.get("href")
            if not href:
                continue
            pdf_url = href.strip()
            pdf_links.append(pdf_url)
            pdf_names.append(pdf_url.split("/")[-1])

        if pdf_links:
            if len(pdf_links) == 1:
                product_data["pdf_link"] = pdf_links[0]
                product_data["pdf_filename"] = pdf_names[0]
            else:
                product_data["pdf_link"] = pdf_links
                product_data["pdf_filename"] = pdf_names

        lead_time = soup.select_one("strong p")
        if lead_time:
            product_data["lead_time"] = lead_time.get_text(strip=True).replace("Standard Lead Time:", "").strip()

        pdf_links = []
        pdf_filenames = []
        accordion = soup.select_one("#accordion")
        if accordion:
            panels = accordion.select(".panel-heading a")
            for panel in panels:
                section_name = panel.get_text(" ", strip=True).replace("+", "").strip()
                target_id = panel.get("href")
                if not target_id or not target_id.startswith("#"):
                    continue

                panel_id = target_id[1:]
                panel_div = accordion.find(id=panel_id)
                if not panel_div:
                    continue

                body = panel_div.select_one(".panel-body")
                if not body:
                    continue

                if section_name == "Description":
                    text = " ".join(body.stripped_strings)
                    product_data["Description"] = text

                elif section_name == "Specifications":
                    specs = {}
                    for tr in body.select("tr"):
                        tds = tr.select("td")
                        if len(tds) == 2:
                            key = tds[0].get_text(strip=True).replace(":", "")
                            value = tds[1].get_text(strip=True)
                            specs[key] = value
                    if specs:
                        product_data["Specifications"] = specs

                elif section_name == "Features and Benefits":
                    features = []
                    lis = body.select("li")
                    if lis:
                        for li in lis:
                            text = " ".join(li.stripped_strings)
                            if text:
                                features.append(text)
                    elif body.select_one("ul"):
                        ul_text = " ".join(body.select_one("ul").stripped_strings)
                        parts = [p.strip() for p in ul_text.split("  ") if p.strip()]
                        features.extend(parts)
                    else:
                        text = " ".join(body.stripped_strings)
                        if text:
                            product_data["Features and Benefits"] = text
                            continue

                    if features:
                        product_data["Features and Benefits"] = features if len(features) > 1 else features[0]

                elif section_name == "Product Comprise of":
                    items = []
                    for li in body.select("li"):
                        text = li.get_text(strip=True)
                        if text:
                            items.append(text)
                    if items:
                        product_data["Product Comprise of"] = items

                elif section_name == "Additional Resources":
                    resources = []
                    for a in body.select("a[href]"):
                        href = a["href"].strip()
                        name = a.get_text(strip=True)
                        parsed = urlparse(href)
                        path = parsed.path.lower()
                        if not path.endswith(".pdf"):
                            continue
                        filename = path.split("/")[-1]
                        resources.append({"name": name, "url": href})
                        if "data-sheet" not in name.lower():
                            continue
                        pdf_links.append(href)
                        pdf_filenames.append(filename)

                    if resources:
                        product_data["Additional Resources"] = resources

            if pdf_links:
                if len(pdf_links) == 1:
                    product_data["pdf_link"] = pdf_links[0]
                    product_data["pdf_filename"] = pdf_filenames[0]
                else:
                    product_data["pdf_link"] = pdf_links
                    product_data["pdf_filename"] = pdf_filenames

        products.append(product_data)
        tables["products"] = products
        return tables

    def markdown(soup, url):
        markdown = {}
        overview = []
        for sel in SITE_CONFIG["part"]["markdown"]:
            cat_overview = Core.write_overview_markdown(soup, sel, "", url)
            overview.append(cat_overview)
        markdown["overview"] = overview
        return markdown

    def documentation(soup):
        documents = {}
        metadata = []
        for sel in SITE_CONFIG["part"]["documentation"]:
            for a in soup.select(sel):
                href = a["href"].strip()
                parsed = urlparse(href)
                path = parsed.path.lower()
                if not path.endswith(".pdf"):
                    continue
                url = a["href"].strip()
                parsed = urlparse(url)
                filename = parsed.path.split("/")[-1]
                metadata.append({
                    "name": filename,
                    "url": url,
                    "file_path": "",
                    "version": None,
                    "date": None,
                    "language": None,
                    "description": None
                })
        if metadata:
            documents["metadata"] = metadata
        return documents

    def images(soup, url):
        images = {}
        metadata = []
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        for sel in SITE_CONFIG["part"]["images"]:
            for img in soup.select(sel):
                img_url = img.get("src")
                if img_url and not img_url.startswith("http"):
                    img_url = urljoin(base_url, img_url)
                if img_url and img_url.startswith("http://"):
                    img_url = img_url.replace("http://", "https://", 1)
                if img_url:
                    img_filename = img_url.split("/")[-1]
                    metadata.append({
                        "name": img_filename,
                        "url": img_url,
                        "file_path": "",
                        "version": None,
                        "date": None,
                        "language": None,
                        "description": None
                    })
        if metadata:
            images["metadata"] = metadata
        return images


class Core:
    def init(topic_folder, data, update_prices_only=False):
        structure_folders = {
            "documentation": Core.download_general_files,
            "images": Core.download_images_files,
            "block_diagrams": Core.download_block_diagrams_files,
            "design_resources": Core.download_general_files,
            "software_tools": Core.download_general_files,
            "trainings": Core.download_general_files,
            "other": Core.download_general_files,
            "tables": Core.prepare_products_table,
            "markdowns": Core.prepare_markdown_file
        }

        page_type = data.get("page_type", None)
        if page_type == "product":
            structure_folders_filter = structure_folders
        else:
            structure_folders_filter = {
                k: v for k, v in structure_folders.items() if k in data
            }

        Core.create_output_folders(topic_folder, page_type, update_prices_only)

        for folder, method in structure_folders_filter.items():
            final_structure_folder = os.path.join(topic_folder, folder)

            if not data.get(folder):
                continue

            if callable(method):
                try:
                    if folder == "markdowns":
                        success = method(
                            data.get(folder),
                            final_structure_folder,
                            "overview.md")
                    elif folder == "tables":
                        success = method(
                            data.get(folder),
                            final_structure_folder,
                            "products.json")
                    else:
                        success = method(
                            data.get(folder),
                            final_structure_folder,
                            3,   # max_retries
                            2,   # retry_delay
                            False  # rename_by_detected_type
                        )

                    if success == True:
                        logger.info(f"✅ Successfully processed {folder}")
                    else:
                        logger.error(f"❌ Failed to process {folder}")

                except Exception as cb_err:
                    logger.error(f"⚠️ Callback error for {method}: {cb_err}")

    def create_output_folders(out_dir, type_name=None, update_prices_only=False):
        os.makedirs(out_dir, exist_ok=True)

        if type_name == "product":
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
        else:
            folders = ["tables", "markdowns"]

        dirs = {name: os.path.join(out_dir, name) for name in folders}

        for folder_path in dirs.values():
            os.makedirs(folder_path, exist_ok=True)

        file_map = {
            "tables": "metadata.json",
            "markdowns": "overview.md",
        }

        if not update_prices_only:
            file_map["images"] = "metadata.json"
            file_map["documentation"] = "metadata.json"
            file_map["block_diagrams"] = "block_diagram_mappings.json"
            file_map["design_resources"] = "metadata.json"
            file_map["software_tools"] = "metadata.json"
            file_map["trainings"] = "metadata.json"
            file_map["other"] = "metadata.json"

        for folder, file_name in file_map.items():
            if folder in dirs:
                file_path = os.path.join(dirs[folder], file_name)
                if file_name.endswith(".json"):
                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump([], f, indent=2)
                else:
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write("")

        return dirs

    def fetch_html_with_zyte(url: str, max_retries: int = 3, selector=None) -> str:
        if not ZYTE_API_KEY:
            logger.critical("ZYTE_API_KEY not set in environment")
            sys.exit(1)

        payload = {
            "url": url,
            "browserHtml": True,
            "javascript": True
        }
        if selector:
            payload["actions"] = [{
                "action": "waitForSelector",
                "selector": {"type": "css", "value": selector},
                "timeout": 15
            }]

        for attempt in range(max_retries):
            try:
                resp = requests.post(
                    ZYTE_API_URL,
                    auth=(ZYTE_API_KEY, ""),
                    json=payload,
                    timeout=90
                )

                if resp.status_code == 200:
                    data = resp.json()
                    html = data.get("browserHtml") or data.get("httpResponseBody")
                    if html:
                        logger.info(f"✅ Zyte fetched page successfully on attempt {attempt + 1}")
                        return html
                    else:
                        raise Exception("Zyte returned empty HTML or body")

                raise Exception(f"Zyte API returned {resp.status_code}: {resp.text}")

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep((2 ** attempt) + random.uniform(0, 1))
                else:
                    logger.error(f"❌ Failed after {max_retries} attempts for {url}")
                    raise

    def get_requests(url, headers=None, timeout=60, stream=False, retries=3):
        """
        Fetch URL with retry logic:
        1. Try curl_cffi.requests with retries
        2. Fallback to Zyte API with browserHtml (full JS rendering) — FIXED
        """
        headers = headers or {'User-Agent': "Mozilla/5.0"}

        # ----------------------------
        # 1. Regular requests retries
        # ----------------------------
        for attempt in range(retries):
            try:
                logger.info(f"ℹ️ Attempt {attempt + 1}/{retries} (regular request): {url}")
                verify_ssl = CONFIG.get("verify_ssl", True)
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=timeout,
                    stream=stream,
                    verify=verify_ssl,
                    impersonate="chrome110"
                )
                response.raise_for_status()
                logger.info(f"✅ Success (regular request): {url}")
                if stream:
                    return response
                return response.content

            except RequestsError as e:
                if attempt < retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(
                        f"⚠️ Regular request failed ({attempt + 1}/{retries}) for {url}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"❌ All regular request attempts failed for {url}. "
                        f"Falling back to Zyte API..."
                    )

        # ----------------------------
        # 2. Zyte fallback — FIXED: use browserHtml for JS rendering
        # ----------------------------
        try:
            logger.info(f"ℹ️ Zyte API attempt 1/1: {url}")

            api_response = requests.post(
                ZYTE_API_URL,
                auth=(ZYTE_API_KEY, ""),
                json={
                    "url": url,
                    "browserHtml": True,   # ✅ FIXED: was httpResponseBody (no JS)
                    "javascript": True,    # ✅ FIXED: enable JS execution
                },
                timeout=90,
            )

            api_response.raise_for_status()
            data = api_response.json()

            html = data.get("browserHtml")
            if not html:
                raise Exception("Zyte returned empty browserHtml")

            logger.info(f"✅ Success (Zyte API browserHtml): {url}")
            return html.encode("utf-8")   # ✅ Return bytes, consistent with regular path

        except Exception as e:
            logger.error(f"❌ Zyte API failed for {url}: {e}")
            return None

    def fetch_html(url: str, runner="request", max_retries=3, selector=None) -> str | None:
        try:
            logger.info("Loading page...")

            if os.path.exists(url):
                with open(url, "r", encoding="utf-8") as fh:
                    return fh.read()

            if runner == "zyte":
                return Core.fetch_html_with_zyte(url=url, selector=selector)

            raw = Core.get_requests(url, retries=max_retries)

            if not raw:
                return None

            if isinstance(raw, (bytes, bytearray)):
                return Core.fix_encoding(raw.decode("utf-8", errors="ignore"))

            return raw

        except Exception as e:
            logger.error(f"Failed to load {url}: {e}")
            return None

    def fix_encoding(text):
        """Fix mojibake (badly decoded UTF-8 as Latin-1)."""
        if not text:
            return text
        try:
            return text.encode('latin1').decode('utf-8')
        except Exception:
            return text

    def prepare_markdown_file(structure_data, structure_folder, filename="overview.md"):
        if not structure_data or "overview" not in structure_data:
            logger.warning(f"Missing '{structure_folder}.overview' in input data.")
            return

        overview_list = structure_data["overview"]
        if not isinstance(overview_list, list):
            raise TypeError("md_list must be a list of strings")

        save_path = os.path.join(structure_folder, filename)
        try:
            with open(save_path, "w", encoding="utf-8") as f:
                for item in overview_list:
                    f.write(item)
                    f.write("\n\n")
            logger.info(f"✅ Markdown file created: {save_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to create markdown: {e}")
            return False

    def download_images_files(
        structure_data,
        structure_folder="images",
        max_retries=3,
        retry_delay=2,
        rename_by_detected_type=False
    ):
        REQUIRED_KEYS = ["language", "description", "version", "date"]

        if not structure_data or "metadata" not in structure_data:
            logger.warning(f"Missing '{structure_folder}.metadata' in input data.")
            return

        metadata_list = structure_data["metadata"]
        on_file_downloaded = structure_data.get("callback", None)

        if not isinstance(metadata_list, list):
            logger.warning(f"'{structure_folder}.metadata' must be a list of objects.")
            return

        os.makedirs(structure_folder, exist_ok=True)
        seen_filenames = set()
        first_image_done = False

        for item in metadata_list:
            name = item.get("name")
            url = item.get("url")

            for key in REQUIRED_KEYS:
                item.setdefault(key, None)

            if not name or not url:
                logger.warning(f"⚠️ Skipping entry with missing name or url: {item}")
                continue

            base_name, orig_ext = os.path.splitext(name)
            final_name = name
            save_path = os.path.join(structure_folder, final_name)

            counter = 1
            while os.path.exists(save_path) or final_name in seen_filenames:
                final_name = f"{base_name}({counter}){orig_ext}"
                save_path = os.path.join(structure_folder, final_name)
                counter += 1

            attempt = 0
            success = False
            while attempt < max_retries and not success:
                attempt += 1
                try:
                    logger.info(f"⬇️ Downloading {final_name} (attempt {attempt}/{max_retries}) from {url} ...")
                    response = Core.get_requests(url, retries=max_retries, stream=True)

                    if not response or response.status_code != 200:
                        raise Exception("Failed to download file")

                    file_content = b"".join(response.iter_content(chunk_size=8192))
                    if not file_content:
                        raise Exception("Empty file content")

                    kind = filetype.guess(file_content)
                    detected_ext = f".{kind.extension}" if kind else orig_ext.lower()

                    if not first_image_done:
                        final_name = f"product{detected_ext}"
                        save_path = os.path.join(structure_folder, final_name)
                        first_image_done = True
                    elif rename_by_detected_type and detected_ext != orig_ext.lower():
                        base_name_no_ext, _ = os.path.splitext(final_name)
                        counter = 0
                        final_name = f"{base_name_no_ext}{detected_ext}"
                        save_path = os.path.join(structure_folder, final_name)
                        while os.path.exists(save_path):
                            counter += 1
                            final_name = f"{base_name_no_ext}({counter}){detected_ext}"
                            save_path = os.path.join(structure_folder, final_name)

                    seen_filenames.add(final_name)

                    with open(save_path, "wb") as f:
                        f.write(file_content)

                    size = os.path.getsize(save_path)
                    file_path = os.path.join(structure_folder, final_name).replace("\\", "/")
                    item["file_path"] = file_path
                    item["name"] = final_name
                    success = True
                    logger.info(f"✅ Saved: {save_path} ({size} bytes), detected type: {detected_ext}")

                except Exception as e:
                    logger.error(f"❌ Attempt {attempt} failed for {final_name}: {e}")
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"🚫 Giving up after {max_retries} attempts for {final_name}")
                        item["file_path"] = "Failed to download"

                finally:
                    if success and callable(on_file_downloaded):
                        try:
                            updated_item = on_file_downloaded(item, file_content, detected_ext)
                            if isinstance(updated_item, dict):
                                item.update(updated_item)
                        except Exception as cb_err:
                            logger.error(f"⚠️ Callback error for {final_name}: {cb_err}")

        Core.save_metadata(metadata_list, structure_folder)
        return True

    def download_block_diagrams_files(
        structure_data,
        structure_folder="documentation",
        max_retries=3,
        retry_delay=2,
        rename_by_detected_type=False
    ):
        REQUIRED_KEYS = ["language", "description", "version", "date"]

        if not structure_data or "metadata" not in structure_data:
            logger.warning(f"Missing '{structure_folder}.metadata' in input data.")
            return

        metadata_list = structure_data["metadata"]
        on_file_downloaded = structure_data.get("callback", None)

        if not isinstance(metadata_list, list):
            logger.warning(f"'{structure_folder}.metadata' must be a list of objects.")
            return

        os.makedirs(structure_folder, exist_ok=True)
        seen_filenames = set()

        for item in metadata_list:
            name = item.get("name")
            url = item.get("url")

            for key in REQUIRED_KEYS:
                item.setdefault(key, None)

            if not name or not url:
                logger.warning(f"⚠️ Skipping entry with missing name or url: {item}")
                continue

            base_name, orig_ext = os.path.splitext(name)
            final_name = name
            save_path = os.path.join(structure_folder, final_name)

            counter = 1
            while os.path.exists(save_path) or final_name in seen_filenames:
                final_name = f"{base_name}({counter}){orig_ext}"
                save_path = os.path.join(structure_folder, final_name)
                counter += 1

            seen_filenames.add(final_name)

            attempt = 0
            success = False
            while attempt < max_retries and not success:
                attempt += 1
                try:
                    logger.info(f"⬇️ Downloading {final_name} (attempt {attempt}/{max_retries}) from {url} ...")
                    response = requests.get(url, timeout=60)
                    response.raise_for_status()
                    file_content = response.content

                    if len(file_content) == 0:
                        raise Exception("Empty file content")

                    kind = filetype.guess(file_content)
                    detected_ext = f".{kind.extension}" if kind else orig_ext.lower()

                    if rename_by_detected_type and detected_ext != orig_ext.lower():
                        base_name_no_ext, _ = os.path.splitext(final_name)
                        counter = 1
                        final_name = f"{base_name_no_ext}({counter}){detected_ext}"
                        save_path = os.path.join(structure_folder, final_name)
                        while os.path.exists(save_path):
                            counter += 1
                            final_name = f"{base_name_no_ext}({counter}){detected_ext}"
                            save_path = os.path.join(structure_folder, final_name)

                    with open(save_path, "wb") as f:
                        f.write(file_content)

                    size = os.path.getsize(save_path)
                    file_path = os.path.join(structure_folder, final_name).replace("\\", "/")
                    item["file_path"] = file_path
                    item["name"] = final_name
                    success = True
                    logger.info(f"✅ Saved: {save_path} ({size} bytes), detected type: {detected_ext}")

                except Exception as e:
                    logger.error(f"❌ Attempt {attempt} failed for {final_name}: {e}")
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"🚫 Giving up after {max_retries} attempts for {final_name}")
                        status = getattr(response, "status_code", None)
                        item["file_path"] = f"Failed to download : {status}" if status else "Failed to download"

                finally:
                    if success and callable(on_file_downloaded):
                        try:
                            updated_item = on_file_downloaded(item, file_content, detected_ext)
                            if isinstance(updated_item, dict):
                                item.update(updated_item)
                        except Exception as cb_err:
                            logger.error(f"⚠️ Callback error for {final_name}: {cb_err}")

        Core.save_metadata(metadata_list, structure_folder, "block_diagram_mappings.json")
        return True

    def get_filename_from_response(response):
        cd = response.headers.get("Content-Disposition", "")
        if not cd:
            return None
        m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd)
        if m:
            return m.group(1).strip()
        return None

    def download_general_files(
        structure_data,
        structure_folder="documentation",
        max_retries=3,
        retry_delay=2,
        rename_by_detected_type=False
    ):
        REQUIRED_KEYS = ["language", "description", "version", "date"]

        if not structure_data or "metadata" not in structure_data:
            logger.warning(f"Missing '{structure_folder}.metadata' in input data.")
            return

        metadata_list = structure_data["metadata"]
        on_file_downloaded = structure_data.get("callback", None)

        if not isinstance(metadata_list, list):
            logger.warning(f"'{structure_folder}.metadata' must be a list of objects.")
            return

        os.makedirs(structure_folder, exist_ok=True)
        seen_filenames = set()

        for item in metadata_list:
            name = item.get("name")
            url = item.get("url")

            for key in REQUIRED_KEYS:
                item.setdefault(key, None)

            if not name or not url:
                logger.warning(f"⚠️ Skipping entry with missing name or url: {item}")
                continue

            base_name, orig_ext = os.path.splitext(name)
            final_name = name
            save_path = os.path.join(structure_folder, final_name)

            counter = 1
            while os.path.exists(save_path) or final_name in seen_filenames:
                final_name = f"{base_name}({counter}){orig_ext}"
                save_path = os.path.join(structure_folder, final_name)
                counter += 1

            seen_filenames.add(final_name)

            attempt = 0
            success = False
            while attempt < max_retries and not success:
                attempt += 1
                try:
                    logger.info(f"⬇️ Downloading {final_name} (attempt {attempt}/{max_retries}) from {url} ...")
                    response = Core.get_requests(url, retries=max_retries, stream=True)

                    if not response or response.status_code != 200:
                        raise Exception("Failed to download file")

                    file_content = b"".join(response.iter_content(chunk_size=8192))
                    if not file_content:
                        raise Exception("Empty file content")

                    server_filename = Core.get_filename_from_response(response)
                    if server_filename:
                        final_name = server_filename
                        base_name, orig_ext = os.path.splitext(final_name)
                        save_path = os.path.join(structure_folder, final_name)

                    kind = filetype.guess(file_content)
                    detected_ext = f".{kind.extension}" if kind else orig_ext.lower()

                    if rename_by_detected_type and detected_ext != orig_ext.lower():
                        base_name_no_ext, _ = os.path.splitext(final_name)
                        final_name = f"{base_name_no_ext}{detected_ext}"
                        save_path = os.path.join(structure_folder, final_name)
                        counter = 1
                        while os.path.exists(save_path):
                            final_name = f"{base_name_no_ext}({counter}){detected_ext}"
                            save_path = os.path.join(structure_folder, final_name)
                            counter += 1

                    with open(save_path, "wb") as f:
                        f.write(file_content)

                    size = os.path.getsize(save_path)
                    file_path = save_path.replace("\\", "/")
                    item["file_path"] = file_path
                    item["name"] = final_name
                    success = True
                    logger.info(f"✅ Saved: {save_path} ({size} bytes), detected type: {detected_ext}")

                except Exception as e:
                    logger.error(f"❌ Attempt {attempt} failed for {final_name}: {e}")
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"🚫 Giving up after {max_retries} attempts for {final_name}")
                        status = getattr(response, "status_code", None)
                        item["file_path"] = f"Failed to download : {status}" if status else "Failed to download"

                finally:
                    if success and callable(on_file_downloaded):
                        try:
                            updated_item = on_file_downloaded(item, file_content, detected_ext)
                            if isinstance(updated_item, dict):
                                item.update(updated_item)
                        except Exception as cb_err:
                            logger.error(f"⚠️ Callback error for {final_name}: {cb_err}")

        Core.save_metadata(metadata_list, structure_folder)
        return True

    def save_metadata(metadata_list, structure_folder="documentation", filename="metadata.json"):
        if not isinstance(metadata_list, list):
            logger.warning("metadata_list must be a list of objects")
            return

        updated_metadata = []
        for item in metadata_list:
            if not isinstance(item, dict):
                continue

            pdf_path = item.get("file_path")
            if pdf_path and os.path.exists(pdf_path) and pdf_path.lower().endswith(".pdf"):
                try:
                    reader = PdfReader(pdf_path)
                    doc_info = reader.metadata
                    if doc_info:
                        if not item.get("description") and doc_info.get("/Title"):
                            item["description"] = doc_info.get("/Title")
                        if not item.get("version") and doc_info.get("/Version"):
                            item["version"] = doc_info.get("/Version")
                        if not item.get("date") and doc_info.get("/CreationDate"):
                            raw_date = doc_info.get("/CreationDate")
                            if raw_date.startswith("D:") and len(raw_date) >= 10:
                                item["date"] = f"{raw_date[2:6]}-{raw_date[6:8]}-{raw_date[8:10]}"
                            else:
                                item["date"] = raw_date
                        if not item.get("language") and doc_info.get("/Language"):
                            item["language"] = doc_info.get("/Language")
                except Exception as e:
                    logger.warning(f"Failed to extract PDF metadata for {pdf_path}: {e}")

            updated_metadata.append(item)

        os.makedirs(structure_folder, exist_ok=True)
        save_path = os.path.join(structure_folder, filename)
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(updated_metadata, f, ensure_ascii=False, indent=4)

        logger.info(f"✅ Metadata saved at: {save_path}")
        return save_path

    def prepare_products_table(structure_data, structure_folder="tables", filename="products.json"):
        if not structure_data or "products" not in structure_data:
            logger.warning(f"Missing '{structure_folder}.products' in input data.")
            return

        products = structure_data["products"]
        if not isinstance(products, list):
            raise TypeError("products must be a list of dictionaries")

        mandatory_fields = [
            "Product",
            "product_page_link",
            "pdf_link",
            "pdf_filename",
            "image_url",
        ]

        products_dict = {}
        for product in products:
            if not isinstance(product, dict):
                raise TypeError(f"Each product must be a dictionary: {product}")
            if "Product" not in product:
                raise ValueError(f"Each product must have a 'Product' key: {product}")

            for key in mandatory_fields:
                product.setdefault(key, None)

            for field, value in list(product.items()):
                if isinstance(value, list) and len(value) == 1:
                    product[field] = value[0]

            key = product["Product"]
            products_dict[key] = product

        save_path = os.path.join(structure_folder, filename)
        try:
            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(products_dict, f, ensure_ascii=False, indent=4)
            logger.info(f"✅ Products JSON saved at: {save_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to save products.json: {e}")
            return False

    def write_overview_markdown(soup, div_selector, section_title=None, url=None):
        div = soup.select_one(div_selector)
        if not div:
            return ""

        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        for tag in div.select("a[href], img[src]"):
            if tag.name == "a" and tag["href"].startswith("javascript"):
                tag["href"] = ""
            if tag.name == "a" and tag["href"].startswith("/"):
                tag["href"] = (base_url.rstrip("/") if base_url else "") + tag["href"]
            elif tag.name == "img" and tag["src"].startswith("/"):
                tag["src"] = (base_url.rstrip("/") if base_url else "") + tag["src"]

        for btn in div.select("button[onclick]"):
            onclick = btn.get("onclick", "")
            m = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", onclick)
            if not m:
                continue
            href = m.group(1)
            if href.startswith("/"):
                href = (base_url.rstrip("/") if base_url else "") + href
            a = soup.new_tag("a", href=href)
            text = btn.get_text(strip=True)
            a.string = text if text else "Download"
            btn.replace_with(a)

        html_content = div.decode_contents().strip()
        if not html_content:
            return ""

        markdown_text = md(html_content, heading_style="ATX")
        markdown_text = Core._html_to_str(markdown_text)
        markdown_text = Core.clean_html_spaces(markdown_text)
        markdown_text = re.sub(r"\n{3,}", "\n\n", markdown_text.strip())
        markdown_text = "\n".join(line.strip() for line in markdown_text.splitlines())

        if section_title:
            section_header = f"## {section_title}\n\n"
        else:
            section_header = ""

        return section_header + markdown_text.strip() + "\n"

    def _html_to_str(html):
        if not html:
            return ""
        if isinstance(html, list):
            return " ".join(str(x) for x in html if x)
        if isinstance(html, (dict, int, float)):
            return str(html)
        return str(html)

    def clean_html_spaces(text: str, full_clean=False) -> str:
        if not text:
            return ""
        text = text.replace("&nbsp;", " ").replace("\xa0", " ").replace("\u00a0", " ")
        return text if not full_clean else re.sub(r"\s+", " ", text).strip()

    def fix_lazy_loaded_images(soup):
        for img in soup.find_all("img"):
            src = img.get("src", "")
            real_src = img.get("data-amsrc")
            if src.startswith("data:image") and real_src:
                img["src"] = real_src
        return soup


def init(url, update_prices_only=False):
    crawl_array = {}
    soup = None
    try:
        html = Core.fetch_html(url, "request")
        if not html:
            logging.error(f"Empty HTML for {url}")
            return None
        soup = BeautifulSoup(html, "html.parser")
        Core.fix_lazy_loaded_images(soup)

    except Exception as e:
        logging.error(f"Failed to load URL/file {url}: {e}")
        return

    # ✅ Debug: log which selectors matched so you can tune SITE_CONFIG
    cat_sel = SITE_CONFIG["category"]["main_selector"]
    grp_sel = SITE_CONFIG["group"]["main_selector"]
    prt_sel = SITE_CONFIG["part"]["main_selector"]

    has_cat = bool(cat_sel and soup.select_one(cat_sel))
    has_grp = bool(grp_sel and soup.select_one(grp_sel))
    has_prt = bool(prt_sel and soup.select_one(prt_sel))

    logger.info(f"🔍 Selector check → category='{cat_sel}': {has_cat} | "
                f"group='{grp_sel}': {has_grp} | "
                f"part='{prt_sel}': {has_prt}")

    # Product page — check FIRST (most specific selector)
    if has_prt:
        crawl_array["page_type"] = "product"
        crawl_array["markdowns"] = Product.markdown(soup, url)
        crawl_array["tables"] = Product.tables(soup, url)
        if not update_prices_only:
            crawl_array["documentation"] = Product.documentation(soup)
            crawl_array["images"] = Product.images(soup, url)

    # Group / Sub-category with product grid
    elif has_grp:
        crawl_array["page_type"] = "group"
        crawl_array["markdowns"] = Group.markdown(soup, url)
        crawl_array["tables"] = Group.tables(soup, url)

    # Category page (top-level, no product grid)
    elif has_cat:
        crawl_array["page_type"] = "category"
        crawl_array["markdowns"] = Category.markdown(soup, url)

    else:
        # No selector matched — log HTML snippet to help diagnose correct selectors
        logger.error("❌ No page type selector matched. Check SITE_CONFIG selectors.")
        logger.info(f"📄 HTML snippet (first 3000 chars):\n{html[:3000]}")
        return None

    return crawl_array


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Array-driven scraper")
    parser.add_argument("--url", required=True, help="URL or local HTML file path to scrape")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument("--update-only-prices", action="store_true", help="Only update prices")
    args = parser.parse_args()

    crawl_array = init(args.url, args.update_only_prices)

    if not crawl_array:
        logger.error("data extraction failed.")
    else:
        Core.init(args.out, crawl_array, args.update_only_prices)
        logger.info("Done")
