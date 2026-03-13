import os
import time
import json
import argparse
import sys
import re
import random
import logging
from base64 import b64decode
from urllib.parse import urlparse, urljoin

import curl_cffi.requests as requests
from curl_cffi.requests import RequestsError
import filetype
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from pypdf import PdfReader

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Zyte (optional fallback)
# ─────────────────────────────────────────────
ZYTE_API_KEY = os.environ.get("ZYTE_API_KEY", "")
ZYTE_API_URL = "https://api.zyte.com/v1/extract"

CONFIG = {
    "verify_ssl": True
}

# ─────────────────────────────────────────────
# Site config for aafintl.com
# ─────────────────────────────────────────────
SITE_CONFIG = {
    # ── Category page ──────────────────────────────────────────────────────
    # A "category" page shows sub-categories / landing content, NO product rows
    "category": {
        # Selector that is present ONLY on category/landing pages
        "main_selector": ".field--name-field-category-description",
        "markdown": [
            ".field--name-field-category-description",
            ".field--name-body",
        ],
    },

    # ── Group page ─────────────────────────────────────────────────────────
    # A "group" page lists multiple products (cards / table rows)
    "group": {
        "main_selector": ".view-products",
        "markdown": [
            ".view-header",
            ".view-content",
        ],
        "tables": {
            # Each product card / row
            "product_selector": ".views-row",
            "name_selector":    "h3, .views-field-title",
            "link_selector":    "a",
            "image_selector":   "img",
            "pdf_selector":     "a[href$='.pdf']",
        },
    },

    # ── Product (part) page ────────────────────────────────────────────────
    "part": {
        "main_selector": ".field--name-field-product-description",
        "markdown": [
            ".field--name-field-product-description",
            ".field--name-body",
            ".field--name-field-features",
            ".field--name-field-specifications",
        ],
        "tables": {
            "name_selector":    "h1.page-header, h1",
            "spec_selector":    ".field--name-field-specifications table, table.product-specs",
            "image_selector":   ".field--name-field-product-images img, .product-image img, .field--type-image img",
            "pdf_selector":     "a[href$='.pdf']",
        },
        "documentation": [
            "a[href$='.pdf']",
        ],
        "images": [
            ".field--name-field-product-images img",
            ".product-image img",
            ".field--type-image img",
            "article img",
        ],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Page scrapers
# ─────────────────────────────────────────────────────────────────────────────

class Category:
    def markdown(soup, url):
        markdown = {}
        overview = []
        for sel in SITE_CONFIG["category"]["markdown"]:
            text = Core.write_overview_markdown(soup, sel, "", url)
            if text:
                overview.append(text)
        markdown["overview"] = overview
        return markdown


class Group:
    def markdown(soup, url):
        markdown = {}
        overview = []
        for sel in SITE_CONFIG["group"]["markdown"]:
            text = Core.write_overview_markdown(soup, sel, "", url)
            if text:
                overview.append(text)
        markdown["overview"] = overview
        return markdown

    def tables(soup, url):
        tables = {}
        products = []
        cfg = SITE_CONFIG["group"]["tables"]

        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        rows = soup.select(cfg["product_selector"])
        if not rows:
            # fallback: grab all links that look like product pages
            rows = soup.select("a[href*='/products/']")

        for row in rows:
            product_data = {
                "Product": None,
                "product_page_link": None,
                "pdf_link": None,
                "pdf_filename": None,
                "image_url": None,
            }

            # Name
            name_el = row.select_one(cfg["name_selector"])
            if name_el:
                product_data["Product"] = name_el.get_text(strip=True)

            # Link
            link_el = row.select_one(cfg["link_selector"]) if cfg["link_selector"] else None
            if not link_el and row.name == "a":
                link_el = row
            if link_el and link_el.get("href"):
                href = link_el["href"]
                if not href.startswith("http"):
                    href = urljoin(base_url, href)
                product_data["product_page_link"] = href
                if not product_data["Product"]:
                    product_data["Product"] = link_el.get_text(strip=True)

            # Image
            img_el = row.select_one(cfg["image_selector"])
            if img_el:
                src = img_el.get("src") or img_el.get("data-src")
                if src and not src.startswith("http"):
                    src = urljoin(base_url, src)
                product_data["image_url"] = src

            # PDF
            pdf_el = row.select_one(cfg["pdf_selector"])
            if pdf_el:
                pdf_href = pdf_el.get("href", "")
                if not pdf_href.startswith("http"):
                    pdf_href = urljoin(base_url, pdf_href)
                product_data["pdf_link"] = pdf_href
                product_data["pdf_filename"] = pdf_href.split("/")[-1]

            if product_data["Product"]:
                products.append(product_data)

        tables["products"] = products
        return tables


class Product:
    def tables(soup, url):
        tables = {}
        products = []
        cfg = SITE_CONFIG["part"]["tables"]

        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        product_data = {
            "Product": None,
            "product_page_link": url,
            "pdf_link": None,
            "pdf_filename": None,
            "image_url": None,
        }

        # Name — try multiple selectors in priority order
        name_selectors = [
            "h1", "title",
            "nav li:last-child", ".breadcrumb li:last-child",
            ".page-header", "h2",
        ]
        for sel in name_selectors:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(strip=True)
                if sel == "title":
                    for sep in ["|", "-", "–"]:
                        if sep in text:
                            text = text.split(sep)[0].strip()
                            break
                # Skip generic/short names
                if text and len(text) > 3 and text.lower() not in ["products", "home", "aaf"]:
                    product_data["Product"] = text
                    break

        # Fallback: extract name from URL path
        if not product_data["Product"] or product_data["Product"].lower() in ["products", "home"]:
            path_parts = [p for p in urlparse(url).path.split("/") if p]
            if path_parts:
                raw = path_parts[-1].replace("-", " ").replace("--", " ").title()
                product_data["Product"] = raw

        # Image
        for img_sel in cfg["image_selector"].split(", "):
            img_el = soup.select_one(img_sel.strip())
            if img_el:
                src = img_el.get("src") or img_el.get("data-src")
                if src:
                    if not src.startswith("http"):
                        src = urljoin(base_url, src)
                    product_data["image_url"] = src
                    break

        # PDF
        for pdf_sel in cfg["pdf_selector"].split(", "):
            pdf_el = soup.select_one(pdf_sel.strip())
            if pdf_el:
                pdf_href = pdf_el.get("href", "")
                if not pdf_href.startswith("http"):
                    pdf_href = urljoin(base_url, pdf_href)
                product_data["pdf_link"] = pdf_href
                product_data["pdf_filename"] = pdf_href.split("/")[-1]
                break

        # Description - clean text only, no HTML
        desc_selectors = ["meta[name='description']", "meta[property='og:description']", "p"]
        for sel in desc_selectors:
            el = soup.select_one(sel)
            if el:
                text = el.get("content") or el.get_text(strip=True)
                # Strip any HTML tags from text
                text = re.sub(r'<[^>]+>', '', text).strip()
                if text and len(text) > 20:
                    product_data["description"] = text
                    break

        # Specs from table
        spec_table = soup.select_one(cfg["spec_selector"])
        if spec_table:
            for row in spec_table.select("tr"):
                cells = row.select("td, th")
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True)
                    val = cells[1].get_text(strip=True)
                    if key:
                        product_data[key] = val

        products.append(product_data)
        tables["products"] = products
        return tables

    def markdown(soup, url):
        markdown = {}
        overview = []
        for sel in SITE_CONFIG["part"]["markdown"]:
            text = Core.write_overview_markdown(soup, sel, "", url)
            if text:
                overview.append(text)
        markdown["overview"] = overview
        return markdown

    def documentation(soup):
        documents = {}
        metadata = []
        base_url = "https://www.aafintl.com"
        for sel in SITE_CONFIG["part"]["documentation"]:
            for a in soup.select(sel):
                href = a.get("href", "").strip()
                parsed = urlparse(href)
                path = parsed.path.lower()
                if not path.endswith(".pdf"):
                    continue
                if not href.startswith("http"):
                    href = urljoin(base_url, href)
                filename = parsed.path.split("/")[-1]
                metadata.append({
                    "name": filename,
                    "url": href,
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
                img_url = img.get("src") or img.get("data-src")
                if not img_url:
                    continue
                if not img_url.startswith("http"):
                    img_url = urljoin(base_url, img_url)
                if img_url.startswith("http://"):
                    img_url = img_url.replace("http://", "https://", 1)
                img_filename = img_url.split("/")[-1]
                # avoid duplicates
                if any(m["url"] == img_url for m in metadata):
                    continue
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


# ─────────────────────────────────────────────────────────────────────────────
# Core helpers
# ─────────────────────────────────────────────────────────────────────────────

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
                        success = method(data.get(folder), final_structure_folder, "overview.md")
                    elif folder == "tables":
                        success = method(data.get(folder), final_structure_folder, "products.json")
                    else:
                        success = method(data.get(folder), final_structure_folder, 3, 2, False)

                    if success:
                        logger.info(f"✅ Successfully processed {folder}")
                    else:
                        logger.error(f"❌ Failed to process {folder}")

                except Exception as cb_err:
                    logger.error(f"⚠️ Callback error for {method}: {cb_err}")

    def create_output_folders(out_dir, type_name=None, update_prices_only=False):
        os.makedirs(out_dir, exist_ok=True)

        if type_name == "product":
            folders = [
                "documentation", "images", "block_diagrams",
                "design_resources", "software_tools", "tables",
                "markdowns", "trainings", "other",
            ]
        else:
            folders = ["tables", "markdowns"]

        dirs = {name: os.path.join(out_dir, name) for name in folders}
        for folder_path in dirs.values():
            os.makedirs(folder_path, exist_ok=True)

        file_map = {"tables": "metadata.json", "markdowns": "overview.md"}
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

        payload = {"url": url, "browserHtml": True, "javascript": True}
        if selector:
            payload["actions"] = [{
                "action": "waitForSelector",
                "selector": {"type": "css", "value": selector},
                "timeout": 15
            }]

        for attempt in range(max_retries):
            try:
                resp = requests.post(ZYTE_API_URL, auth=(ZYTE_API_KEY, ""), json=payload, timeout=90)
                if resp.status_code == 200:
                    data = resp.json()
                    html = data.get("browserHtml") or data.get("httpResponseBody")
                    if html:
                        logger.info(f"✅ Zyte fetched page on attempt {attempt + 1}")
                        return html
                    raise Exception("Zyte returned empty HTML")
                raise Exception(f"Zyte API returned {resp.status_code}: {resp.text}")
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep((2 ** attempt) + random.uniform(0, 1))
                else:
                    logger.error(f"❌ Failed after {max_retries} attempts for {url}")
                    raise

    def get_requests(url, headers=None, timeout=60, stream=False, retries=3):
        headers = headers or {"User-Agent": "Mozilla/5.0"}

        for attempt in range(retries):
            try:
                logger.info(f"ℹ️ Attempt {attempt + 1}/{retries} (regular request): {url}")
                verify_ssl = CONFIG.get("verify_ssl", True)
                response = requests.get(
                    url, headers=headers, timeout=timeout,
                    stream=stream, verify=verify_ssl, impersonate="chrome110"
                )
                response.raise_for_status()
                logger.info(f"✅ Success (regular request): {url}")
                if stream:
                    return response
                return response.content

            except RequestsError as e:
                if attempt < retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"⚠️ Regular request failed ({attempt + 1}/{retries}). Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ All regular request attempts failed for {url}. Falling back to Zyte API...")

        try:
            logger.info(f"ℹ️ Zyte API attempt 1/1: {url}")
            api_response = requests.post(
                ZYTE_API_URL, auth=(ZYTE_API_KEY, ""),
                json={"url": url, "httpResponseBody": True}, timeout=60,
            )
            api_response.raise_for_status()
            http_response_body = b64decode(api_response.json()["httpResponseBody"])
            logger.info(f"✅ Success (Zyte API): {url}")
            return http_response_body
        except RequestsError as e:
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
        if not text:
            return text
        try:
            return text.encode("latin1").decode("utf-8")
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
                for md_text in overview_list:
                    f.write(md_text)
                    f.write("\n\n")
            logger.info(f"✅ Markdown file created: {save_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to create markdown: {e}")
            return False

    def download_images_files(structure_data, structure_folder="images", max_retries=3, retry_delay=2, rename_by_detected_type=False):
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

                    seen_filenames.add(final_name)

                    with open(save_path, "wb") as f:
                        f.write(file_content)

                    size = os.path.getsize(save_path)
                    file_path = os.path.join(structure_folder, final_name).replace("\\", "/")
                    item["file_path"] = file_path
                    item["name"] = final_name
                    success = True
                    logger.info(f"✅ Saved: {save_path} ({size} bytes)")

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

    def download_block_diagrams_files(structure_data, structure_folder="documentation", max_retries=3, retry_delay=2, rename_by_detected_type=False):
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
                    response = requests.get(url, timeout=60)
                    response.raise_for_status()
                    file_content = response.content

                    if len(file_content) == 0:
                        raise Exception("Empty file content")

                    kind = filetype.guess(file_content)
                    detected_ext = f".{kind.extension}" if kind else orig_ext.lower()

                    with open(save_path, "wb") as f:
                        f.write(file_content)

                    size = os.path.getsize(save_path)
                    file_path = os.path.join(structure_folder, final_name).replace("\\", "/")
                    item["file_path"] = file_path
                    item["name"] = final_name
                    success = True
                    logger.info(f"✅ Saved: {save_path} ({size} bytes)")

                except Exception as e:
                    logger.error(f"❌ Attempt {attempt} failed for {final_name}: {e}")
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                    else:
                        item["file_path"] = "Failed to download"

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

    def download_general_files(structure_data, structure_folder="documentation", max_retries=3, retry_delay=2, rename_by_detected_type=False):
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
            response = None
            while attempt < max_retries and not success:
                attempt += 1
                try:
                    response = Core.get_requests(url, retries=max_retries, stream=True)
                    response.raise_for_status()

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

                    with open(save_path, "wb") as f:
                        f.write(file_content)

                    size = os.path.getsize(save_path)
                    file_path = save_path.replace("\\", "/")
                    item["file_path"] = file_path
                    item["name"] = final_name
                    success = True
                    logger.info(f"✅ Saved: {save_path} ({size} bytes)")

                except Exception as e:
                    logger.error(f"❌ Attempt {attempt} failed for {final_name}: {e}")
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                    else:
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

        mandatory_fields = ["Product", "product_page_link", "pdf_link", "pdf_filename", "image_url"]

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
            if key:
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
            if tag.name == "a" and tag.get("href", "").startswith("javascript"):
                tag["href"] = ""
            if tag.name == "a" and tag.get("href", "").startswith("/"):
                tag["href"] = base_url.rstrip("/") + tag["href"]
            elif tag.name == "img" and tag.get("src", "").startswith("/"):
                tag["src"] = base_url.rstrip("/") + tag["src"]

        for btn in div.select("button[onclick]"):
            onclick = btn.get("onclick", "")
            m = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", onclick)
            if not m:
                continue
            href = m.group(1)
            if href.startswith("/"):
                href = base_url.rstrip("/") + href
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

        section_header = f"## {section_title}\n\n" if section_title else ""
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


# ─────────────────────────────────────────────────────────────────────────────
# Main entry
# ─────────────────────────────────────────────────────────────────────────────

def detect_page_type(url, soup):
    """
    Detect page type using URL depth + HTML signals.
    aafintl.com URL structure:
      /us/products/<category>                          → category (2 segments after /products/)
      /us/products/<category>/<subcategory>            → group (3 segments)
      /us/products/<category>/<subcategory>/<product>  → product (4+ segments)
    """
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    # Count segments after /products/
    parts = [p for p in path.split("/") if p]
    try:
        prod_idx = parts.index("products")
        depth = len(parts) - prod_idx - 1  # segments after "products"
    except ValueError:
        depth = 0

    logger.info(f"ℹ️ URL depth after /products/: {depth}")

    # HTML signals (try selectors first)
    # Product page signals
    product_signals = [
        "article.node--type-product",
        ".node--type-product",
        ".product-detail",
        "h1.page-header",
        ".field--name-field-product-image",
        ".field--name-body",
    ]
    # Group/listing signals
    group_signals = [
        ".view-products",
        ".views-row",
        ".product-listing",
        ".view-content .views-row",
    ]
    # Category signals
    category_signals = [
        ".node--type-product-category",
        ".field--name-field-category-description",
        ".category-landing",
    ]

    has_product = any(soup.select_one(s) for s in product_signals)
    has_group   = any(soup.select_one(s) for s in group_signals)
    has_category = any(soup.select_one(s) for s in category_signals)

    # Decision logic
    if has_product and depth >= 3:
        return "product"
    if has_group:
        return "group"
    if has_category:
        return "category"

    # Fallback: use URL depth
    if depth >= 3:
        logger.info("ℹ️ Detected as PRODUCT page via URL depth")
        return "product"
    elif depth == 2:
        logger.info("ℹ️ Detected as GROUP page via URL depth")
        return "group"
    else:
        logger.info("ℹ️ Detected as CATEGORY page via URL depth")
        return "category"


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

    # ── Detect page type ─────────────────────────────────────────────────────
    page_type = detect_page_type(url, soup)
    logger.info(f"✅ Page type detected: {page_type.upper()}")

    if page_type == "product":
        crawl_array["page_type"] = "product"
        crawl_array["markdowns"] = Product.markdown(soup, url)
        crawl_array["tables"] = Product.tables(soup, url)
        if not update_prices_only:
            crawl_array["documentation"] = Product.documentation(soup)
            crawl_array["images"] = Product.images(soup, url)

    elif page_type == "group":
        crawl_array["page_type"] = "group"
        crawl_array["markdowns"] = Group.markdown(soup, url)
        crawl_array["tables"] = Group.tables(soup, url)

    else:
        crawl_array["page_type"] = "category"
        main = soup.select_one("main, .main-content, article, #content, .layout-container")
        if main:
            html_content = main.decode_contents().strip()
            markdown_text = md(html_content, heading_style="ATX")
            markdown_text = re.sub(r"\n{3,}", "\n\n", markdown_text.strip())
            crawl_array["markdowns"] = {"overview": [markdown_text]}
        else:
            crawl_array["markdowns"] = {"overview": []}

    return crawl_array


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AAF International scraper")
    parser.add_argument("--url", required=True, help="URL or local HTML file path to scrape")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument("--update-only-prices", action="store_true", help="Only update prices")
    args = parser.parse_args()

    crawl_array = init(args.url, args.update_only_prices)

    if not crawl_array:
        logger.error("Data extraction failed.")
    else:
        Core.init(args.out, crawl_array, args.update_only_prices)
        logger.info("Done ✅")