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
    "verify_ssl": False  # Disable SSL verification for problematic sites
}

ZYTE_API_KEY = ""  # set this in your environment if needed
ZYTE_API_URL = "https://api.zyte.com/v1/extract"

# ---------------------------------------------------------------------------
# AFL Global site configuration
#
# Page type detection (checked in order in init()):
#   "part"     → detected when the rendered page has an <h1> AND a
#                download/spec-sheet link  (a[href*='stylelabs'] or a[href$='.pdf'])
#   "group"    → detected when the page body contains product card links
#                that point deeper into /Products/ hierarchy (≥5 path segments)
#   "category" → fallback (top-level listing, no individual product cards)
#
# All selectors may need minor tuning once you have the rendered Zyte/browser HTML.
# Edit SITE_CONFIG here — the rest of the code reads from it automatically.
# ---------------------------------------------------------------------------
SITE_CONFIG = {
    "category": {
        # Category pages on AFL Global show sub-category tiles.
        # We fall through to "group" for any page that has product links.
        # main_selector=None means: never match as category first; let group/part win.
        "main_selector": None,
        "markdown": ["main", "body"],
        "documentation": [],
    },

    "group": {
        # Triggered when the page has anchor links leading to deeper /Products/ paths.
        # AFL Global product listing cards are <a href="/en/apac/Products/..."> elements.
        "main_selector": "body",
        # The product_container selector picks individual product card links.
        # We filter further in Group.tables() to skip nav/footer links.
        "product_container": "a[href]",
        "markdown": ["main", "body"],
        "documentation": [],
        "products": {
            # Within each card <a>, extract these fields:
            "name": "h3, h2, h4, p, span",   # product title inside the card
            "sku":  "",                         # no separate SKU on listing pages
            "product_page_link": "",            # the <a> href itself (handled in tables())
            "pdf_link": "",
            "pdf_filename": "",
            "image_url": "img::attr(src)",
            "pricing": "",
            "description": ""
        }
    },

    "part": {
        # Detected when the page has product-specific sections or a spec-sheet download
        "main_selector": "#afl-feature-section, #afl-description-section, #afl-spec-section, a[href*='stylelabs'], a[href$='.pdf']",
        "markdown": ["main", "body"],
        # Images: AFL Global product pages usually have an OG image or a hero image.
        "images": [
            "meta[property='og:image']",   # handled specially via attr, see Product.images()
            "img[class*='product']",
            ".product-image img",
            "img[alt]",
        ],
        "documentation": [
            "a[href*='stylelabs']",
            "a[href$='.pdf']",
        ],
        "block_diagrams": [],
        "design_resources": [],
        "software_tools": [],
        "products": {
            "name": "h1, title",
            "sku":  "h1, title",
            "product_page_link": "",
            "pdf_link": "",      # filled programmatically in Product.tables()
            "pdf_filename": "",  # filled programmatically in Product.tables()
            "image_url": "meta[property='og:image']::attr(content)",
            "pricing": "",
            "description": "",
            "features": "",
            "application": "",
            "specification": "",
        }
    }
}


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Helpers for AFL Global section parsing
# ---------------------------------------------------------------------------
def _extract_list_items(section_el):
    """Return a list of text strings from <li> items inside *section_el*."""
    items = []
    for li in section_el.select("li"):
        text = " ".join(li.stripped_strings)
        if text:
            items.append(text)
    return items


def _extract_paragraphs(section_el):
    """Return joined paragraph text from *section_el*."""
    parts = []
    for p in section_el.select("p"):
        text = " ".join(p.stripped_strings)
        if text:
            parts.append(text)
    if not parts:
        # fallback: all text
        text = " ".join(section_el.stripped_strings)
        if text:
            parts.append(text)
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Category
# ---------------------------------------------------------------------------
class Category:
    def markdown(soup, url):
        markdown = {}
        overview = []
        for sel in SITE_CONFIG["category"]["markdown"]:
            cat_overview = Core.write_overview_markdown(soup, sel, "Category", url)
            if cat_overview:
                overview.append(cat_overview)
                break   # first selector that gives content is enough
        markdown['overview'] = overview
        return markdown

    def documentation(soup):
        documents = {}
        metadata = []
        for sel in SITE_CONFIG["category"]["documentation"]:
            for a in soup.select(sel):
                href = a.get("href", "").strip()
                parsed = urlparse(href)
                path = parsed.path.lower()
                if not path.endswith(".pdf"):
                    continue
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


# ---------------------------------------------------------------------------
# Group  (category / sub-category listing with product card links)
# ---------------------------------------------------------------------------
class Group:
    def markdown(soup, url):
        markdown = {}
        overview = []
        for sel in SITE_CONFIG["group"]["markdown"]:
            cat_overview = Core.write_overview_markdown(soup, sel, "Category", url)
            if cat_overview:
                overview.append(cat_overview)
                break
        markdown["overview"] = overview
        return markdown

    def tables(soup, url):
        tables = {}
        products = []

        parsed_base = urlparse(url)
        base_url = f"{parsed_base.scheme}://{parsed_base.netloc}"
        # We want only links that go *deeper* into /Products/ (i.e. are product pages,
        # not sibling navigation).  AFL Global URLs look like:
        #   /en/apac/Products/<Category>/<SubCategory>/<ProductName>
        # Minimum 5 path segments means it is a product/sub-category page, not just the
        # top /Products/ page or a 1-level category.
        seen_urls = set()
        for a in soup.select(SITE_CONFIG["group"]["product_container"]):
            href = a.get("href", "")
            if not href:
                continue

            # Make absolute
            if not href.startswith("http"):
                href = urljoin(base_url, href)

            # Must belong to the same domain
            a_parsed = urlparse(href)
            if a_parsed.netloc and a_parsed.netloc != parsed_base.netloc:
                continue

            # Only /Products/ paths with depth strictly greater than the current page qualify.
            # This prevents breadcrumb links (which go UP the hierarchy) from being counted as products.
            current_path_parts = [p for p in parsed_base.path.split("/") if p]
            path_parts = [p for p in a_parsed.path.split("/") if p]
            if len(path_parts) <= len(current_path_parts):
                continue

            # Must be under /Products/ (case-insensitive)
            if not any(p.lower() == "products" for p in path_parts):
                continue

            # Skip anchors, js links, and the base URL itself
            if href.rstrip("/") == url.rstrip("/"):
                continue
            if "#" in href:
                href = href.split("#")[0]
            if not href or href in seen_urls:
                continue
            seen_urls.add(href)

            # --- extract card content ---
            name = None
            for name_sel in ["h3", "h2", "h4", "p", "span"]:
                tag = a.select_one(name_sel)
                if tag:
                    name = tag.get_text(strip=True)
                    if name:
                        break
            if not name:
                name = a.get_text(strip=True)

            img_src = extract_value(a, SITE_CONFIG["group"]["products"]["image_url"])
            if img_src and not img_src.startswith("http"):
                img_src = urljoin(base_url, img_src)

            product = {
                "Product": name or href.rstrip("/").split("/")[-1],
                "name": name or None,
                "product_page_link": href,
                "pdf_link": None,
                "pdf_filename": None,
                "image_url": img_src,
                "Pricing": None,
            }
            products.append(product)

        tables["products"] = products
        return tables

    def documentation(soup):
        documents = {}
        metadata = []
        for sel in SITE_CONFIG["group"]["documentation"]:
            for a in soup.select(sel):
                href = a.get("href", "").strip()
                parsed = urlparse(href)
                path = parsed.path.lower()
                if not path.endswith(".pdf"):
                    continue
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


# ---------------------------------------------------------------------------
# Product  (individual product/part page)
# ---------------------------------------------------------------------------
class Product:
    def tables(soup, url):
        tables = {}
        products = []
        product_data = {}

        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        # --- Core fields ---
        product_data["Product"] = extract_value(soup, SITE_CONFIG["part"]["products"]["sku"])
        product_data["name"]    = extract_value(soup, SITE_CONFIG["part"]["products"]["name"])
        product_data["Pricing"] = extract_value(soup, SITE_CONFIG["part"]["products"]["pricing"])
        product_data["product_page_link"] = url

        # --- Image (prefer OG image tag) ---
        og_img = soup.select_one("meta[property='og:image']")
        if og_img:
            product_data["image_url"] = og_img.get("content", "")
        else:
            product_data["image_url"] = extract_value(soup, SITE_CONFIG["part"]["products"]["image_url"])

        # --- PDF / spec-sheet links ---
        # AFL Global hosts documents on afl-delivery.stylelabs.cloud
        pdf_links = []
        pdf_names = []
        doc_resources = []

        for sel in SITE_CONFIG["part"]["documentation"]:
            for a in soup.select(sel):
                href = a.get("href", "").strip()
                if not href:
                    continue
                if not href.startswith("http"):
                    href = urljoin(base_url, href)

                link_text = a.get_text(strip=True)

                # Build a filename from the URL or text
                a_parsed = urlparse(href)
                raw_name = a_parsed.path.split("/")[-1] or link_text or "document"
                # Clean query-params from name
                raw_name = raw_name.split("?")[0]
                if not raw_name:
                    raw_name = "document"
                # Ensure .pdf extension for display
                doc_name = raw_name if "." in raw_name else raw_name + ".pdf"

                doc_resources.append({"name": link_text or doc_name, "url": href})
                pdf_links.append(href)
                pdf_names.append(doc_name)

        if doc_resources:
            product_data["Resources"] = doc_resources

        if pdf_links:
            if len(pdf_links) == 1:
                product_data["pdf_link"]     = pdf_links[0]
                product_data["pdf_filename"] = pdf_names[0]
            else:
                product_data["pdf_link"]     = pdf_links
                product_data["pdf_filename"] = pdf_names

        # -------------------------------------------------------------------
        # AFL Global section parsing
        # The rendered page has named sections / anchor IDs:
        #   #afl-feature-section     → Features
        #   #afl-description-section → Description
        #   #afl-Resources-section   → Resources (already handled above via <a>)
        # We look for these IDs and fall back to heuristic heading search.
        # -------------------------------------------------------------------

        # ---- Features ----
        feat_section = (
            soup.find(id="afl-feature-section")
            or soup.find(id="afl-features-section")
        )
        if feat_section:
            items = _extract_list_items(feat_section)
            if items:
                product_data["Features"] = items if len(items) > 1 else items[0]
            else:
                text = _extract_paragraphs(feat_section)
                if text:
                    product_data["Features"] = text
        else:
            # Heuristic: find heading that contains "Feature"
            for heading in soup.find_all(["h2", "h3", "h4"]):
                if "feature" in heading.get_text(strip=True).lower():
                    sibling_ul = heading.find_next_sibling("ul")
                    if sibling_ul:
                        items = [li.get_text(strip=True) for li in sibling_ul.select("li") if li.get_text(strip=True)]
                        if items:
                            product_data["Features"] = items if len(items) > 1 else items[0]
                    break

        # ---- Applications ----
        app_section = (
            soup.find(id="afl-application-section")
            or soup.find(id="afl-applications-section")
        )
        if app_section:
            items = _extract_list_items(app_section)
            if items:
                product_data["Applications"] = items if len(items) > 1 else items[0]
            else:
                text = _extract_paragraphs(app_section)
                if text:
                    product_data["Applications"] = text
        else:
            for heading in soup.find_all(["h2", "h3", "h4"]):
                if "application" in heading.get_text(strip=True).lower():
                    sibling_ul = heading.find_next_sibling("ul")
                    if sibling_ul:
                        items = [li.get_text(strip=True) for li in sibling_ul.select("li") if li.get_text(strip=True)]
                        if items:
                            product_data["Applications"] = items if len(items) > 1 else items[0]
                    break

        # ---- Description ----
        desc_section = (
            soup.find(id="afl-description-section")
            or soup.find(id="afl-desc-section")
        )
        if desc_section:
            text = _extract_paragraphs(desc_section)
            if text:
                product_data["Description"] = text
        else:
            # Heuristic: find the first <p> that is long enough after the <h1>
            h1 = soup.find("h1")
            if h1:
                for sib in h1.find_all_next("p"):
                    text = " ".join(sib.stripped_strings)
                    if len(text) > 80:
                        product_data["Description"] = text
                        break

        # ---- Specifications ----
        spec_section = (
            soup.find(id="afl-spec-section")
            or soup.find(id="afl-specifications-section")
        )
        if spec_section:
            specs = {}
            for tr in spec_section.select("tr"):
                tds = tr.select("td")
                if len(tds) >= 2:
                    key   = tds[0].get_text(strip=True).rstrip(":")
                    value = tds[1].get_text(strip=True)
                    if key:
                        specs[key] = value
            if specs:
                product_data["Specifications"] = specs

        products.append(product_data)
        tables["products"] = products
        return tables

    def markdown(soup, url):
        markdown = {}
        overview = []
        for sel in SITE_CONFIG["part"]["markdown"]:
            cat_overview = Core.write_overview_markdown(soup, sel, "", url)
            if cat_overview:
                overview.append(cat_overview)
                break
        markdown["overview"] = overview
        return markdown

    def documentation(soup, base_page_url):
        documents = {}
        metadata = []

        parsed_base = urlparse(base_page_url)
        base_url = f"{parsed_base.scheme}://{parsed_base.netloc}"

        for sel in SITE_CONFIG["part"]["documentation"]:
            for a in soup.select(sel):
                href = a.get("href", "").strip()
                if not href:
                    continue
                if not href.startswith("http"):
                    href = urljoin(base_url, href)

                a_parsed = urlparse(href)
                raw_name = a_parsed.path.split("/")[-1].split("?")[0]
                if not raw_name:
                    raw_name = "document"
                doc_name = raw_name if "." in raw_name else raw_name + ".pdf"

                metadata.append({
                    "name": doc_name,
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

        # 1. Prefer OG image
        og_img = soup.select_one("meta[property='og:image']")
        if og_img:
            img_url = og_img.get("content", "").strip()
            if img_url:
                img_filename = img_url.split("/")[-1].split("?")[0] or "product.jpg"
                metadata.append({
                    "name": img_filename,
                    "url": img_url,
                    "file_path": "",
                    "version": None,
                    "date": None,
                    "language": None,
                    "description": "og:image"
                })

        # 2. Additional images from configured selectors
        selectors_to_try = [
            "img[class*='product']",
            ".product-image img",
            "img[alt]",
        ]
        seen = {m["url"] for m in metadata}
        for sel in selectors_to_try:
            for img in soup.select(sel):
                img_url = img.get("src", "").strip()
                if not img_url:
                    continue
                if not img_url.startswith("http"):
                    img_url = urljoin(base_url, img_url)
                if img_url.startswith("http://"):
                    img_url = img_url.replace("http://", "https://", 1)
                if img_url in seen:
                    continue
                # Skip tiny icons / spacers
                if any(kw in img_url.lower() for kw in ["icon", "logo", "sprite", "blank", "pixel", "1x1"]):
                    continue
                seen.add(img_url)
                img_filename = img_url.split("/")[-1].split("?")[0] or "image.jpg"
                metadata.append({
                    "name": img_filename,
                    "url": img_url,
                    "file_path": "",
                    "version": None,
                    "date": None,
                    "language": None,
                    "description": img.get("alt", "") or None
                })

        if metadata:
            images_data["metadata"] = metadata
        return images_data


# ===========================================================================
# Core  — unchanged utility class (identical interface to eecoswitch version)
# ===========================================================================
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
                        success = method(
                            data.get(folder),
                            final_structure_folder,
                            3,     # max_retries
                            2,     # retry_delay
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
            file_map["images"]          = "metadata.json"
            file_map["documentation"]   = "metadata.json"
            file_map["block_diagrams"]  = "block_diagram_mappings.json"
            file_map["design_resources"]= "metadata.json"
            file_map["software_tools"]  = "metadata.json"
            file_map["trainings"]       = "metadata.json"
            file_map["other"]           = "metadata.json"

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
        2. Fallback to Zyte API with ONLY 1 retry (only if ZYTE_API_KEY is set)
        """
        headers = headers or {"User-Agent": "Mozilla/5.0"}

        for attempt in range(retries):
            try:
                logger.info(f"ℹ️ Attempt {attempt + 1}/{retries} (regular request): {url}")
                verify_ssl = CONFIG.get("verify_ssl", True)
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"
                }
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
                    logger.error(f"❌ All regular request attempts failed for {url}")
                    return None

        # Zyte fallback (only if key is set)
        if not ZYTE_API_KEY:
            return None

        try:
            logger.info(f"ℹ️ Zyte API attempt 1/1: {url}")
            api_response = requests.post(
                ZYTE_API_URL,
                auth=(ZYTE_API_KEY, ""),
                json={"url": url, "httpResponseBody": True},
                timeout=60,
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
        """Fix mojibake (badly decoded UTF-8 as Latin-1)."""
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
            url  = item.get("url")

            for key in REQUIRED_KEYS:
                item.setdefault(key, None)

            if not name or not url:
                logger.warning(f"⚠️ Skipping entry with missing name or url: {item}")
                continue

            base_name, orig_ext = os.path.splitext(name)
            final_name = name
            save_path  = os.path.join(structure_folder, final_name)

            counter = 1
            while os.path.exists(save_path) or final_name in seen_filenames:
                final_name = f"{base_name}({counter}){orig_ext}"
                save_path  = os.path.join(structure_folder, final_name)
                counter += 1

            attempt = 0
            success = False
            while attempt < max_retries and not success:
                attempt += 1
                try:
                    logger.info(f"⬇️ Downloading {final_name} (attempt {attempt}/{max_retries}) from {url} ...")
                    response = Core.get_requests(url, retries=max_retries, stream=True)

                    if not response:
                        raise Exception("Failed to download file, response was None")

                    if isinstance(response, bytes):
                        file_content = response
                    else:
                        response.raise_for_status()
                        file_content = b"".join(response.iter_content(chunk_size=8192))

                    kind = filetype.guess(file_content)
                    detected_ext = f".{kind.extension}" if kind else orig_ext.lower()

                    if not first_image_done:
                        final_name = f"product{detected_ext}"
                        save_path  = os.path.join(structure_folder, final_name)
                        first_image_done = True
                    elif rename_by_detected_type and detected_ext != orig_ext.lower():
                        base_name_no_ext, _ = os.path.splitext(final_name)
                        counter = 0
                        final_name = f"{base_name_no_ext}{detected_ext}"
                        save_path  = os.path.join(structure_folder, final_name)
                        while os.path.exists(save_path):
                            counter += 1
                            final_name = f"{base_name_no_ext}({counter}){detected_ext}"
                            save_path  = os.path.join(structure_folder, final_name)

                    seen_filenames.add(final_name)

                    with open(save_path, "wb") as f:
                        f.write(file_content)

                    size      = os.path.getsize(save_path)
                    file_path = os.path.join(structure_folder, final_name).replace("\\", "/")
                    item["file_path"] = file_path
                    item["name"]      = final_name
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

        metadata_list      = structure_data["metadata"]
        on_file_downloaded = structure_data.get("callback", None)

        if not isinstance(metadata_list, list):
            logger.warning(f"'{structure_folder}.metadata' must be a list of objects.")
            return

        os.makedirs(structure_folder, exist_ok=True)
        seen_filenames = set()

        for item in metadata_list:
            name = item.get("name")
            url  = item.get("url")

            for key in REQUIRED_KEYS:
                item.setdefault(key, None)

            if not name or not url:
                logger.warning(f"⚠️ Skipping entry with missing name or url: {item}")
                continue

            base_name, orig_ext = os.path.splitext(name)
            final_name = name
            save_path  = os.path.join(structure_folder, final_name)

            counter = 1
            while os.path.exists(save_path) or final_name in seen_filenames:
                final_name = f"{base_name}({counter}){orig_ext}"
                save_path  = os.path.join(structure_folder, final_name)
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
                        save_path  = os.path.join(structure_folder, final_name)
                        while os.path.exists(save_path):
                            counter += 1
                            final_name = f"{base_name_no_ext}({counter}){detected_ext}"
                            save_path  = os.path.join(structure_folder, final_name)

                    with open(save_path, "wb") as f:
                        f.write(file_content)

                    size      = os.path.getsize(save_path)
                    file_path = os.path.join(structure_folder, final_name).replace("\\", "/")
                    item["file_path"] = file_path
                    item["name"]      = final_name
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
        m = re.search(r'filename\*?=(?:UTF-8\'\')?\"?([^\";]+)\"?', cd)
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

        metadata_list      = structure_data["metadata"]
        on_file_downloaded = structure_data.get("callback", None)

        if not isinstance(metadata_list, list):
            logger.warning(f"'{structure_folder}.metadata' must be a list of objects.")
            return

        os.makedirs(structure_folder, exist_ok=True)
        seen_filenames = set()

        for item in metadata_list:
            name = item.get("name")
            url  = item.get("url")

            for key in REQUIRED_KEYS:
                item.setdefault(key, None)

            if not name or not url:
                logger.warning(f"⚠️ Skipping entry with missing name or url: {item}")
                continue

            base_name, orig_ext = os.path.splitext(name)
            final_name = name
            save_path  = os.path.join(structure_folder, final_name)

            counter = 1
            while os.path.exists(save_path) or final_name in seen_filenames:
                final_name = f"{base_name}({counter}){orig_ext}"
                save_path  = os.path.join(structure_folder, final_name)
                counter += 1

            seen_filenames.add(final_name)

            attempt = 0
            success = False
            while attempt < max_retries and not success:
                attempt += 1
                try:
                    logger.info(f"⬇️ Downloading {final_name} (attempt {attempt}/{max_retries}) from {url} ...")
                    response = Core.get_requests(url, retries=max_retries, stream=True)

                    if not response:
                        raise Exception("Failed to download file, response was None")

                    if isinstance(response, bytes):
                        file_content = response
                    else:
                        response.raise_for_status()
                        file_content = b"".join(response.iter_content(chunk_size=8192))

                    server_filename = Core.get_filename_from_response(response) if not isinstance(response, bytes) else None
                    if server_filename:
                        final_name = server_filename
                        base_name, orig_ext = os.path.splitext(final_name)
                        save_path  = os.path.join(structure_folder, final_name)

                    if len(file_content) == 0:
                        raise Exception("Empty file content")

                    kind = filetype.guess(file_content)
                    detected_ext = f".{kind.extension}" if kind else orig_ext.lower()

                    if rename_by_detected_type and detected_ext != orig_ext.lower():
                        base_name_no_ext, _ = os.path.splitext(final_name)
                        final_name = f"{base_name_no_ext}{detected_ext}"
                        save_path  = os.path.join(structure_folder, final_name)
                        counter = 1
                        while os.path.exists(save_path):
                            final_name = f"{base_name_no_ext}({counter}){detected_ext}"
                            save_path  = os.path.join(structure_folder, final_name)
                            counter += 1

                    with open(save_path, "wb") as f:
                        f.write(file_content)

                    size      = os.path.getsize(save_path)
                    file_path = save_path.replace("\\", "/")
                    item["file_path"] = file_path
                    item["name"]      = final_name
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
                    reader   = PdfReader(pdf_path)
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

        parsed   = urlparse(url) if url else None
        base_url = f"{parsed.scheme}://{parsed.netloc}" if parsed else ""

        for tag in div.select("a[href], img[src]"):
            try:
                if tag.name == "a" and tag.get("href", "").startswith("javascript"):
                    tag["href"] = ""
                if tag.name == "a" and tag.get("href", "").startswith("/"):
                    tag["href"] = (base_url.rstrip("/") if base_url else "") + tag["href"]
                elif tag.name == "img" and tag.get("src", "").startswith("/"):
                    tag["src"] = (base_url.rstrip("/") if base_url else "") + tag["src"]
            except Exception:
                pass

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
            src      = img.get("src", "")
            real_src = img.get("data-amsrc")
            if src.startswith("data:image") and real_src:
                img["src"] = real_src
        return soup


# ===========================================================================
# Entry point
# ===========================================================================
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
        return None

    # --- Product / part page ---
    # Detected by: has an <h1> AND (product sections OR spec links OR deep path)
    parsed = urlparse(url)
    path_len = len([p for p in parsed.path.split("/") if p])
    part_sel = SITE_CONFIG["part"]["main_selector"]
    
    is_part = False
    if part_sel and soup.select_one(part_sel):
        is_part = True
    elif path_len >= 5:
        # We test if Group.tables finds NO deeper products, if so it's a part page
        temp_group_tables = Group.tables(soup, url)
        if not temp_group_tables.get("products"):
            is_part = True
            
    if is_part:
        crawl_array["page_type"] = "product"
        crawl_array["markdowns"] = Product.markdown(soup, url)
        crawl_array["tables"]   = Product.tables(soup, url)
        if not update_prices_only:
            crawl_array["documentation"] = Product.documentation(soup, url)
            crawl_array["images"]        = Product.images(soup, url)

    # --- Group / category listing page ---
    # Detected by: body exists; product card links found inside.
    # Group.tables() internally filters to valid /Products/ deep links.
    elif SITE_CONFIG["group"]["main_selector"] and soup.select_one(SITE_CONFIG["group"]["main_selector"]):
        group_tables = Group.tables(soup, url)
        if group_tables.get("products"):
            crawl_array["page_type"] = "group"
            crawl_array["markdowns"] = Group.markdown(soup, url)
            crawl_array["tables"]    = group_tables
        else:
            # No product cards found → treat as category overview
            crawl_array["page_type"] = "category"
            crawl_array["markdowns"] = Category.markdown(soup, url)

    # --- Pure category page ---
    elif SITE_CONFIG["category"]["main_selector"] and soup.select_one(SITE_CONFIG["category"]["main_selector"]):
        crawl_array["page_type"] = "category"
        crawl_array["markdowns"] = Category.markdown(soup, url)

    return crawl_array


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AFL Global web scraper")
    parser.add_argument("--url",  required=True, help="URL or local HTML file path to scrape")
    parser.add_argument("--out",  required=True, help="Output directory")
    parser.add_argument("--update-only-prices", action="store_true", help="Only update prices")
    args = parser.parse_args()

    crawl_array = init(args.url, args.update_only_prices)

    if not crawl_array:
        logger.error("Data extraction failed.")
    else:
        Core.init(args.out, crawl_array, args.update_only_prices)
        logger.info("Done")
