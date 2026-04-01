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
    "verify_ssl": True
}

ZYTE_API_KEY = os.getenv("ZYTE_API_KEY")
ZYTE_API_URL = "https://api.zyte.com/v1/extract"

# =============================================================================
# SITE CONFIG  –  songchuanusa.com
#
# Page types
# ----------
#   category  →  application listing pages, e.g.
#                  https://songchuanusa.com/industrial-automation-relays/
#                Each relay series appears as an <article class="post"> card.
#
#   part      →  individual relay product pages, e.g.
#                  https://songchuanusa.com/all-relays/sclb-scld-5a-…/
#                Single <article class="post"> with spec <table>(s) and a
#                PDF datasheet link inside .entry-content.
#
# Detection logic (HTML-structure based, NOT URL pattern):
#   • part     → ONE article.post  +  at least one <table> in .entry-content
#                OR a PDF / Google Drive link in .entry-content
#   • category → MORE THAN TWO article.post elements (relay card grid)
#
# HTML notes (WordPress theme)
# ----------------------------
#   Category pages:
#     <div class="entry-content"> wraps the intro text + relay card grid
#     Each card: <article class="post …">
#       h2.entry-title  →  relay series name
#       a[href]         →  link to the product page
#       img.wp-post-image or first <img>  →  thumbnail
#       .entry-summary p  →  short description
#
#   Product pages:
#     <h1 class="entry-title">  →  full product name / SKU
#     <div class="entry-content">  →  all content
#       paragraphs  →  description, part/competitor series, status
#       <ul>/<li>   →  feature bullets
#       <table>     →  spec rows (Contact Rating, Coil Voltage, …)
#       <a href="*.pdf"> or "Data Sheet" links  →  PDF datasheet
#       <img>       →  product images
# =============================================================================
SITE_CONFIG = {
    # ── CATEGORY ─────────────────────────────────────────────────────────────
    "category": {
        # Unique to category pages: .entry-content exists on both, but
        # multiple article.post cards are the real discriminator (handled
        # in detect_page_type, not by selector alone).
        "main_selector": ".entry-content",
        "markdown": [".entry-content"],
        "documentation": [],          # no bulk PDFs on category listing pages
    },

    # ── GROUP (not present on this site – kept for template compatibility) ───
    "group": {
        "main_selector": None,        # disabled
        "product_container": "article.post",
        "markdown": [".entry-content"],
        "documentation": [],
        "products": {
            "name": "h2.entry-title",
            "sku": "h2.entry-title",
            "product_page_link": "h2.entry-title a::attr(href)",
            "pdf_link": "",
            "pdf_filename": "",
            "image_url": "img.wp-post-image::attr(src)",
            "pricing": "",
            "description": ".entry-summary p"
        }
    },

    # ── PART (product page) ───────────────────────────────────────────────────
    "part": {
        # Present on product pages: single article.post with spec table
        "main_selector": "article.post",
        "markdown": [".entry-content"],
        # All <img> elements inside the post (skip logos/icons in caller)
        "images": ["article.post img", ".entry-content img"],
        # All <a href> links inside entry-content; filtered to .pdf in caller
        "documentation": [".entry-content a[href]"],
        "block_diagrams": [],
        "design_resources": [],
        "software_tools": [],
        "products": {
            # <h1 class="entry-title"> holds the relay name / part series
            "name": "h1.entry-title",
            "sku": "h1.entry-title",
            "product_page_link": "",   # injected at runtime (= the URL itself)
            "pdf_link": "",            # resolved dynamically from .entry-content
            "pdf_filename": "",
            "image_url": ".entry-content img::attr(src)",
            "pricing": "",             # not shown on songchuanusa.com
            "description": ".entry-content p",
            "features": "",
            "application": "",
            "specification": "",
            "variants": {
                "name": "", "sku": "", "product_page_link": "",
                "pdf_link": "", "pdf_filename": "", "image_url": "",
                "pricing": "", "description": ""
            }
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


# =============================================================================
# CATEGORY
# =============================================================================
class Category:
    def markdown(soup, url):
        markdown = {}
        overview = []
        for sel in SITE_CONFIG["category"]["markdown"]:
            cat_overview = Core.write_overview_markdown(soup, sel, "Category", url)
            overview.append(cat_overview)
        markdown["overview"] = overview
        return markdown

    def documentation(soup):
        documents = {}
        metadata = []
        for sel in SITE_CONFIG["category"]["documentation"]:
            for a in soup.select(sel):
                href = a.get("href", "").strip()
                parsed = urlparse(href)
                if not parsed.path.lower().endswith(".pdf"):
                    continue
                filename = parsed.path.split("/")[-1]
                metadata.append({
                    "name": filename, "url": href, "file_path": "",
                    "version": None, "date": None, "language": None, "description": None
                })
        if metadata:
            documents["metadata"] = metadata
        return documents

    def tables(soup, url):
        """
        Build products list from category page relay cards.
        Each <article class="post"> is one relay series.
        """
        tables_data = {}
        products = []
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        for article in soup.select("article.post"):
            # Name
            title_tag = article.select_one("h2.entry-title, h3.entry-title, .entry-title")
            name = title_tag.get_text(strip=True) if title_tag else ""

            # Product page URL
            link_tag = (
                article.select_one("h2.entry-title a, .entry-title a")
                or article.select_one("a.entry-title-link")
                or article.select_one("a[href]")
            )
            prod_url = ""
            if link_tag:
                href = link_tag.get("href", "")
                prod_url = urljoin(base_url, href) if not href.startswith("http") else href

            # Thumbnail
            img_tag = article.select_one("img.wp-post-image, img")
            img_src = ""
            if img_tag:
                img_src = img_tag.get("src", "")
                if img_src and not img_src.startswith("http"):
                    img_src = urljoin(base_url, img_src)

            # Short description
            desc_tag = article.select_one(".entry-summary p, .entry-content p")
            description = desc_tag.get_text(strip=True) if desc_tag else ""

            products.append({
                "Product": name or None,
                "name": name or None,
                "product_page_link": prod_url or None,
                "pdf_link": None,
                "pdf_filename": None,
                "image_url": img_src or None,
                "Pricing": None,
                "description": description or None,
            })

        tables_data["products"] = products
        return tables_data


# =============================================================================
# GROUP  (not used on this site – mirrors Category for template compatibility)
# =============================================================================
class Group:
    def markdown(soup, url):
        return Category.markdown(soup, url)

    def tables(soup, url):
        return Category.tables(soup, url)

    def documentation(soup):
        return Category.documentation(soup)


# =============================================================================
# PRODUCT  (part page)
# =============================================================================
class Product:
    def tables(soup, url):
        tables_data = {}
        products = []
        product_data = {}

        # Remove any status badge injected inside h1
        for badge in soup.select("h1 .pull-right, h1 .badge, h1 span.status"):
            badge.decompose()

        # ── Name / SKU ────────────────────────────────────────────────────
        product_data["Product"] = extract_value(soup, SITE_CONFIG["part"]["products"]["sku"])
        product_data["name"] = extract_value(soup, SITE_CONFIG["part"]["products"]["name"])
        product_data["Pricing"] = None   # not available on this site

        # ── Product page link ─────────────────────────────────────────────
        product_data["product_page_link"] = url

        # ── Main product image ────────────────────────────────────────────
        img_url = None
        for img in soup.select("article.post img, .entry-content img"):
            src = img.get("src", "")
            if src and not re.search(r"logo|icon|banner|header|footer|social|arrow|sprite", src, re.I):
                img_url = urljoin(url, src) if not src.startswith("http") else src
                break
        product_data["image_url"] = img_url

        # ── Part Series & Competitor Series ───────────────────────────────
        page_text = soup.get_text(separator=" ")
        part_match = re.search(r"Part Series[:\s]+([^\|\n<]+)", page_text)
        if part_match:
            product_data["Part Series"] = part_match.group(1).strip()

        comp_match = re.search(r"Competitor Series[:\s]+([^\|\n<]+)", page_text)
        if comp_match:
            product_data["Competitor Series"] = comp_match.group(1).strip()

        # ── Status ────────────────────────────────────────────────────────
        product_data["Status"] = (
            "Discontinued" if re.search(r"discontinued", page_text, re.I) else "In Production"
        )

        # ── Description (first substantial paragraph) ─────────────────────
        desc = ""
        for p in soup.select(".entry-content p"):
            txt = p.get_text(strip=True)
            if txt and len(txt) > 15:
                desc = txt
                break
        product_data["description"] = desc

        # ── Features (bullet list) ────────────────────────────────────────
        features = [
            li.get_text(strip=True)
            for li in soup.select(".entry-content ul li, .entry-content ol li")
            if li.get_text(strip=True)
        ]
        if features:
            product_data["Features"] = features

        # ── Specifications (from <table> rows) ────────────────────────────
        specs = {}
        for table in soup.select(".entry-content table"):
            for row in table.select("tr"):
                cells = row.select("th, td")
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True).rstrip(":")
                    val = " | ".join(c.get_text(strip=True) for c in cells[1:])
                    if key and val:
                        specs[key] = val
        if specs:
            product_data["Specifications"] = specs

        # ── PDF Datasheet links ───────────────────────────────────────────
        pdf_links = []
        pdf_filenames = []

        for a in soup.select(".entry-content a[href]"):
            href = a.get("href", "").strip()
            link_text = a.get_text(strip=True).lower()
            parsed_href = urlparse(href)

            is_pdf = parsed_href.path.lower().endswith(".pdf")
            is_datasheet = re.search(r"data.?sheet|datasheet", link_text)

            if is_pdf or is_datasheet:
                full_href = urljoin(url, href) if not href.startswith("http") else href
                filename = parsed_href.path.split("/")[-1] or "datasheet.pdf"
                if full_href not in pdf_links:
                    pdf_links.append(full_href)
                    pdf_filenames.append(filename)

        if pdf_links:
            if len(pdf_links) == 1:
                product_data["pdf_link"] = pdf_links[0]
                product_data["pdf_filename"] = pdf_filenames[0]
            else:
                product_data["pdf_link"] = pdf_links
                product_data["pdf_filename"] = pdf_filenames
        else:
            product_data["pdf_link"] = None
            product_data["pdf_filename"] = None

        products.append(product_data)
        tables_data["products"] = products
        return tables_data

    def markdown(soup, url):
        markdown = {}
        overview = []
        for sel in SITE_CONFIG["part"]["markdown"]:
            overview_md = Core.write_overview_markdown(soup, sel, "", url)
            overview.append(overview_md)
        markdown["overview"] = overview
        return markdown

    def documentation(soup):
        """All PDF links found in .entry-content."""
        documents = {}
        metadata = []
        for sel in SITE_CONFIG["part"]["documentation"]:
            for a in soup.select(sel):
                href = a.get("href", "").strip()
                parsed = urlparse(href)
                if not parsed.path.lower().endswith(".pdf"):
                    continue
                filename = parsed.path.split("/")[-1]
                metadata.append({
                    "name": filename,
                    "url": href,
                    "file_path": "",
                    "version": None,
                    "date": None,
                    "language": None,
                    "description": a.get_text(strip=True) or None
                })
        if metadata:
            documents["metadata"] = metadata
        return documents

    def images(soup, url):
        images_data = {}
        metadata = []
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        seen_urls = set()

        for sel in SITE_CONFIG["part"]["images"]:
            for img in soup.select(sel):
                img_url = img.get("src", "")
                if not img_url:
                    continue
                if re.search(r"logo|icon|banner|header|footer|social|arrow|sprite", img_url, re.I):
                    continue
                if not img_url.startswith("http"):
                    img_url = urljoin(base_url, img_url)
                if img_url.startswith("http://"):
                    img_url = img_url.replace("http://", "https://", 1)
                if img_url in seen_urls:
                    continue
                seen_urls.add(img_url)

                img_filename = img_url.split("/")[-1].split("?")[0] or "image.jpg"
                metadata.append({
                    "name": img_filename,
                    "url": img_url,
                    "file_path": "",
                    "version": None,
                    "date": None,
                    "language": None,
                    "description": img.get("alt") or None
                })

        if metadata:
            images_data["metadata"] = metadata
        return images_data


# =============================================================================
# CORE  (unchanged from template – only fix_lazy_loaded_images extended)
# =============================================================================
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
                "documentation", "images", "block_diagrams",
                "design_resources", "software_tools",
                "tables", "markdowns", "trainings", "other",
            ]
        else:
            folders = ["tables", "markdowns"]

        dirs = {name: os.path.join(out_dir, name) for name in folders}

        for folder_path in dirs.values():
            os.makedirs(folder_path, exist_ok=True)

        file_map = {"tables": "metadata.json", "markdowns": "overview.md"}

        if not update_prices_only:
            file_map.update({
                "images": "metadata.json",
                "documentation": "metadata.json",
                "block_diagrams": "block_diagram_mappings.json",
                "design_resources": "metadata.json",
                "software_tools": "metadata.json",
                "trainings": "metadata.json",
                "other": "metadata.json",
            })

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
                        logger.info(f"✅ Zyte fetched page successfully on attempt {attempt + 1}")
                        return html
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
        headers = headers or {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }

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
                return response if stream else response.content

            except RequestsError as e:
                if attempt < retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"⚠️ Regular request failed ({attempt + 1}/{retries}) for {url}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ All regular request attempts failed for {url}. Falling back to Zyte API...")

        # Zyte fallback
        try:
            logger.info(f"ℹ️ Zyte API attempt 1/1: {url}")
            api_response = requests.post(
                ZYTE_API_URL,
                auth=(ZYTE_API_KEY, ""),
                json={"url": url, "httpResponseBody": True},
                timeout=60,
            )
            api_response.raise_for_status()
            body = b64decode(api_response.json()["httpResponseBody"])
            logger.info(f"✅ Success (Zyte API): {url}")
            return body
        except RequestsError as e:
            logger.error(f"❌ Zyte API failed for {url}: {e}")
            return None

    def fetch_html(url: str, runner="request", max_retries=3, selector=None) -> str:
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

    def download_images_files(
        structure_data, structure_folder="images",
        max_retries=3, retry_delay=2, rename_by_detected_type=False
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
            file_content = b""
            detected_ext = orig_ext.lower()

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
        structure_data, structure_folder="documentation",
        max_retries=3, retry_delay=2, rename_by_detected_type=False
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
            file_content = b""
            detected_ext = orig_ext.lower()
            response = None

            while attempt < max_retries and not success:
                attempt += 1
                try:
                    logger.info(f"⬇️ Downloading {final_name} (attempt {attempt}/{max_retries}) from {url} ...")
                    response = requests.get(url, timeout=60)
                    response.raise_for_status()
                    file_content = response.content
                    if not file_content:
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
        return m.group(1).strip() if m else None

    def download_general_files(
        structure_data, structure_folder="documentation",
        max_retries=3, retry_delay=2, rename_by_detected_type=False
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
            file_content = b""
            detected_ext = orig_ext.lower()
            response = None

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
            if tag.name == "a":
                href = tag.get("href", "")
                if href.startswith("javascript"):
                    tag["href"] = ""
                elif href.startswith("/"):
                    tag["href"] = base_url.rstrip("/") + href
            elif tag.name == "img":
                src = tag.get("src", "")
                if src.startswith("/"):
                    tag["src"] = base_url.rstrip("/") + src

        for btn in div.select("button[onclick]"):
            onclick = btn.get("onclick", "")
            m = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", onclick)
            if not m:
                continue
            href = m.group(1)
            if href.startswith("/"):
                href = base_url.rstrip("/") + href
            a = soup.new_tag("a", href=href)
            a.string = btn.get_text(strip=True) or "Download"
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
        """Handle WordPress lazy-load patterns and generic data-src attributes."""
        for img in soup.find_all("img"):
            src = img.get("src", "")
            real_src = (
                img.get("data-amsrc")
                or img.get("data-src")
                or img.get("data-lazy-src")
                or img.get("data-original")
            )
            if (not src or src.startswith("data:image")) and real_src:
                img["src"] = real_src
        return soup


# =============================================================================
# PAGE-TYPE DETECTION  –  HTML structure only, NOT URL pattern
#
# songchuanusa.com signals:
#   part     → single article.post  +  spec <table> in .entry-content
#              OR a .pdf / Google Drive link in .entry-content
#   category → multiple (>2) article.post elements (relay card grid)
# =============================================================================
def detect_page_type(soup: BeautifulSoup) -> str:
    articles = soup.select("article.post")
    entry_content = soup.select_one(".entry-content")

    has_spec_table = bool(entry_content and entry_content.select("table"))
    has_pdf_or_ds_link = bool(
        entry_content and entry_content.select(
            "a[href$='.pdf'], a[href*='drive.google'], a[href*='Data-Sheet'], a[href*='datasheet']"
        )
    )

    is_product_page = len(articles) <= 2 and (has_spec_table or has_pdf_or_ds_link)
    is_category_page = len(articles) > 2

    if is_product_page and not is_category_page:
        return "part"
    return "category"


# =============================================================================
# ENTRY POINT
# =============================================================================
def init(url, update_prices_only=False):
    crawl_array = {}

    try:
        html = Core.fetch_html(url, "request")
        if not html:
            logging.error(f"Empty HTML for {url}")
            return None
        soup = BeautifulSoup(html, "html.parser")
        Core.fix_lazy_loaded_images(soup)
    except Exception as e:
        logging.error(f"Failed to load URL/file {url}: {e}")
        return None

    page_type = detect_page_type(soup)
    logger.info(f"🔍 Detected page type: {page_type.upper()}")

    if page_type == "category":
        crawl_array["page_type"] = "category"
        crawl_array["markdowns"] = Category.markdown(soup, url)
        crawl_array["tables"] = Category.tables(soup, url)

    elif page_type == "part":
        crawl_array["page_type"] = "product"    # Core.init expects "product"
        crawl_array["markdowns"] = Product.markdown(soup, url)
        crawl_array["tables"] = Product.tables(soup, url)
        if not update_prices_only:
            crawl_array["documentation"] = Product.documentation(soup)
            crawl_array["images"] = Product.images(soup, url)

    return crawl_array


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="songchuanusa.com scraper")
    parser.add_argument("--url", required=True, help="URL or local HTML file path to scrape")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument("--update-only-prices", action="store_true", help="Only update prices")
    args = parser.parse_args()

    crawl_array = init(args.url, args.update_only_prices)

    if not crawl_array:
        logger.error("Data extraction failed.")
        sys.exit(1)
    else:
        Core.init(args.out, crawl_array, args.update_only_prices)
        logger.info("✅ Done.")