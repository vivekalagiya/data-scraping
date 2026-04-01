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
from urllib.parse import urljoin, urlparse
from PyPDF2 import PdfReader

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

CONFIG = {"verify_ssl": True}

ZYTE_API_KEY = os.getenv("ZYTE_API_KEY")
ZYTE_API_URL = "https://api.zyte.com/v1/extract"

# =============================================================================
# SITE CONFIG  –  songchuanusa.com  (WordPress theme)
#
# Page layout
# ───────────
#   Category page  (/industrial-automation-relays/)
#     Many <article class="post"> cards, each with h2.entry-title + img + excerpt
#
#   Product page  (/all-relays/<slug>/)
#     <h1 class="entry-title">   → product name / SKU
#     <div class="entry-content">
#       <p>Song Chuan Part Series: SCLB / SCLD| Competitor Series: …</p>
#       <p>Discontinued Series …</p>
#       <table>                  → spec rows  (Contact Config, Coil Voltage, …)
#         <tr><td>key</td><td>value</td></tr>
#       </table>
#       <p>5A general purpose … </p>   ← description
#       <ul><li>…</li></ul>            ← feature bullets (sometimes)
#       <img …/>                       ← product image
#       <a href="….pdf">Data Sheet</a> ← datasheet PDF link
#     </div>
#
# NOTE: Some pages have specs in plain <p> tags instead of <table>.
#       Both cases are handled in _parse_specs().
# =============================================================================
SITE_CONFIG = {
    "category": {
        "main_selector": ".entry-content",
        "markdown": [".entry-content"],
        "documentation": [],
    },
    "group": {           # not used on this site – kept for template compatibility
        "main_selector": None,
        "product_container": "article.post",
        "markdown": [".entry-content"],
        "documentation": [],
        "products": {
            "name": "h2.entry-title",
            "sku":  "h2.entry-title",
            "product_page_link": "h2.entry-title a::attr(href)",
            "pdf_link": "", "pdf_filename": "",
            "image_url": "img.wp-post-image::attr(src)",
            "pricing": "", "description": ".entry-summary p"
        }
    },
    "part": {
        "main_selector": "article.post",
        "markdown": [".entry-content"],
        "images": [".entry-content img"],          # single selector → no duplicates
        "documentation": [".entry-content a[href]"],
        "block_diagrams": [], "design_resources": [],
        "software_tools": [],
        "products": {
            "name": "h1.entry-title",              # ← exact WordPress selector
            "sku":  "h1.entry-title",
            "product_page_link": "",
            "pdf_link": "", "pdf_filename": "",
            "image_url": ".entry-content img::attr(src)",
            "pricing": "", "description": ".entry-content p",
            "features": "", "application": "", "specification": "",
            "variants": {
                "name": "", "sku": "", "product_page_link": "",
                "pdf_link": "", "pdf_filename": "",
                "image_url": "", "pricing": "", "description": ""
            }
        }
    }
}


# =============================================================================
# SHARED HELPERS
# =============================================================================
def extract_value(element, selector):
    if not selector:
        return None
    if "::attr(" in selector:
        sel, attr = selector.split("::attr(")
        attr = attr.rstrip(")")
        tag = element.select_one(sel.strip())
        return tag.get(attr) if tag else None
    tag = element.select_one(selector)
    return tag.get_text(strip=True) if tag else None


def _is_datasheet_link(a_tag) -> bool:
    """True if this <a> is a PDF / datasheet resource link."""
    href = a_tag.get("href", "").strip()
    text = a_tag.get_text(strip=True).lower()
    if not href:
        return False
    path = urlparse(href).path.lower()
    is_pdf_url  = path.endswith(".pdf")
    is_ds_text  = bool(re.search(r"data[\s\-_]?sheet", text))
    is_drive    = "drive.google" in href or "docs.google" in href
    return is_pdf_url or is_ds_text or (is_drive and is_ds_text)


def _parse_specs(soup: BeautifulSoup) -> dict:
    """
    Extract spec key/value pairs. Tries two strategies:
      1. Real <table> rows  (td/td or th/td)
      2. Plain <p> tags where spec name and value share one paragraph,
         separated by whitespace (no table on some pages).
    """
    specs = {}

    # ── Strategy 1: HTML table ────────────────────────────────────────────
    for table in soup.select(".entry-content table"):
        for row in table.select("tr"):
            cells = row.select("th, td")
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True).rstrip(":")
                val = " | ".join(c.get_text(strip=True) for c in cells[1:])
                if key and val:
                    specs[key] = val
    if specs:
        return specs

    # ── Strategy 2: <p> tags that start with a known spec keyword ────────
    spec_start = re.compile(
        r"^(Contact|Coil|Power|Current|Voltage|Load|Rating|Insulation|"
        r"Operate|Release|Ambient|Temperature|Dimension|Weight|Terminal|"
        r"Switching|Dielectric|Impulse|Life|Resistance|Pickup)", re.I
    )
    for p in soup.select(".entry-content p"):
        text = p.get_text(strip=True)
        if not spec_start.match(text):
            continue
        # Split on first run of 2+ spaces  →  "Contact Configuration  2C, 4C"
        parts = re.split(r"\s{2,}", text, maxsplit=1)
        if len(parts) == 2:
            specs[parts[0].rstrip(":")] = parts[1]
            continue
        # Fallback: split on first digit/non-alpha boundary after the key
        m = re.match(r"^([A-Za-z][A-Za-z\s\(\)/\-]+?)\s{1,}(\S.+)$", text)
        if m:
            specs[m.group(1).rstrip(":")] = m.group(2)

    return specs


def _parse_series(page_text: str) -> tuple[str, str]:
    """
    Extract Part Series and Competitor Series from a line like:
      "Song Chuan Part Series: SCLB / SCLD| Competitor Series: MY2 | MY4| AZ1309 | HF18FF"
    """
    part, comp = "", ""
    m = re.search(
        r"Part Series[:\s]+(.+?)\s*\|\s*Competitor Series[:\s]+(.+?)(?:\n|Discontinued|Click|$)",
        page_text, re.I | re.DOTALL
    )
    if m:
        part = m.group(1).strip().rstrip("|").strip()
        comp = m.group(2).strip().rstrip("|").strip()
    else:
        pm = re.search(r"Part Series[:\s]+([^\n|]+)", page_text, re.I)
        cm = re.search(r"Competitor Series[:\s]+([^\n]+)", page_text, re.I)
        if pm:
            part = pm.group(1).strip()
        if cm:
            comp = cm.group(1).strip()
    return part, comp


# =============================================================================
# CATEGORY
# =============================================================================
class Category:
    def markdown(soup, url):
        overview = [
            Core.write_overview_markdown(soup, sel, "Category", url)
            for sel in SITE_CONFIG["category"]["markdown"]
        ]
        return {"overview": overview}

    def documentation(soup):
        return {}

    def tables(soup, url):
        products = []
        parsed   = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        for article in soup.select("article.post"):
            title_tag = article.select_one(".entry-title, h2, h3")
            name      = title_tag.get_text(strip=True) if title_tag else ""

            link_tag = (
                article.select_one(".entry-title a")
                or article.select_one("a.entry-title-link")
                or article.select_one("a[href]")
            )
            prod_url = ""
            if link_tag:
                h = link_tag.get("href", "")
                prod_url = h if h.startswith("http") else urljoin(base_url, h)

            img_tag = article.select_one("img.wp-post-image, img")
            img_src = ""
            if img_tag:
                s = img_tag.get("src", "")
                img_src = s if s.startswith("http") else urljoin(base_url, s)

            desc_tag    = article.select_one(".entry-summary p, .entry-content p")
            description = desc_tag.get_text(strip=True) if desc_tag else ""

            products.append({
                "Product":           name or None,
                "name":              name or None,
                "product_page_link": prod_url or None,
                "pdf_link":          None,
                "pdf_filename":      None,
                "image_url":         img_src or None,
                "Pricing":           None,
                "description":       description or None,
            })

        return {"products": products}


# =============================================================================
# GROUP  (mirrors Category – not used on this site)
# =============================================================================
class Group:
    def markdown(soup, url):      return Category.markdown(soup, url)
    def tables(soup, url):        return Category.tables(soup, url)
    def documentation(soup):      return {}


# =============================================================================
# PRODUCT  (part page)
# =============================================================================
class Product:
    def tables(soup, url):
        product_data = {}

        # ── 1. Remove injected badges from h1 ────────────────────────────
        for badge in soup.select("h1 .pull-right, h1 .badge, h1 span.status"):
            badge.decompose()

        # ── 2. Product / SKU name ─────────────────────────────────────────
        # FIX: use multiple fallback selectors so Product key is never None
        name = None
        for sel in ["h1.entry-title", "h1.page-title", "h1", ".entry-title"]:
            tag = soup.select_one(sel)
            if tag:
                name = tag.get_text(strip=True)
                break
        if not name:
            # Last resort: use the URL slug
            name = urlparse(url).path.strip("/").split("/")[-1].replace("-", " ").title()
            logger.warning(f"h1.entry-title not found – using slug as name: {name}")

        product_data["Product"]           = name
        product_data["name"]              = name
        product_data["Pricing"]           = None
        product_data["product_page_link"] = url

        # ── 3. Part Series & Competitor Series ────────────────────────────
        page_text = soup.get_text(separator="\n")
        part_series, comp_series = _parse_series(page_text)
        if part_series:
            product_data["Part Series"]       = part_series
        if comp_series:
            product_data["Competitor Series"] = comp_series

        # ── 4. Status ─────────────────────────────────────────────────────
        product_data["Status"] = (
            "Discontinued" if re.search(r"discontinued", page_text, re.I)
            else "In Production"
        )

        # ── 5. Main image ─────────────────────────────────────────────────
        img_url = None
        for img in soup.select(".entry-content img"):
            src = img.get("src", "")
            if src and not re.search(
                r"logo|icon|banner|header|footer|social|arrow|sprite|separator",
                src, re.I
            ):
                img_url = src if src.startswith("http") else urljoin(url, src)
                break
        product_data["image_url"] = img_url

        # ── 6. Specifications ─────────────────────────────────────────────
        specs = _parse_specs(soup)
        if specs:
            product_data["Specifications"] = specs

        # ── 7. Description ────────────────────────────────────────────────
        # Skip lines that are series info, status notices, spec names, or footnotes
        skip = re.compile(
            r"^(song chuan|part series|competitor|discontinued|click images|"
            r"contact|coil|power|\*opens|opens in|copyright|welding|iron,|"
            r"renewable|energy storage|refrigerator|street light)",
            re.I
        )
        desc = ""
        for p in soup.select(".entry-content p"):
            txt = p.get_text(strip=True)
            if txt and len(txt) > 20 and not skip.match(txt):
                desc = txt
                break
        product_data["description"] = desc

        # ── 8. Features (bullet lists) ────────────────────────────────────
        features = [
            li.get_text(strip=True)
            for li in soup.select(".entry-content ul li, .entry-content ol li")
            if li.get_text(strip=True)
        ]
        if features:
            product_data["Features"] = features

        # ── 9. PDF / Datasheet links ──────────────────────────────────────
        pdf_links, pdf_filenames = [], []
        for a in soup.select(".entry-content a[href]"):
            if not _is_datasheet_link(a):
                continue
            href      = a.get("href", "").strip()
            full_href = href if href.startswith("http") else urljoin(url, href)
            filename  = urlparse(full_href).path.split("/")[-1] or "datasheet.pdf"
            if "." not in filename:
                filename = "datasheet.pdf"
            if full_href not in pdf_links:
                pdf_links.append(full_href)
                pdf_filenames.append(filename)

        if len(pdf_links) == 1:
            product_data["pdf_link"]     = pdf_links[0]
            product_data["pdf_filename"] = pdf_filenames[0]
        elif len(pdf_links) > 1:
            product_data["pdf_link"]     = pdf_links
            product_data["pdf_filename"] = pdf_filenames
        else:
            product_data["pdf_link"]     = None
            product_data["pdf_filename"] = None

        return {"products": [product_data]}

    def markdown(soup, url):
        overview = [
            Core.write_overview_markdown(soup, sel, "", url)
            for sel in SITE_CONFIG["part"]["markdown"]
        ]
        return {"overview": overview}

    def documentation(soup):
        """
        Collect all datasheet / PDF links.
        FIX: old code only accepted href.endswith('.pdf') – missed 'Data Sheet'
             text links and Google Drive links.
        FIX: returns {"metadata": [...]} always so Core.init will call
             download_general_files → save_metadata → non-empty metadata.json.
             Returns {} only when truly no links found (keeps pre-created [] file).
        """
        metadata  = []
        seen_urls: set = set()

        for sel in SITE_CONFIG["part"]["documentation"]:
            for a in soup.select(sel):
                if not _is_datasheet_link(a):
                    continue
                href = a.get("href", "").strip()
                if not href or href in seen_urls:
                    continue
                seen_urls.add(href)

                path     = urlparse(href).path
                filename = path.split("/")[-1] or "datasheet.pdf"
                if "." not in filename:
                    filename = "datasheet.pdf"

                metadata.append({
                    "name":        filename,
                    "url":         href,
                    "file_path":   "",
                    "version":     None,
                    "date":        None,
                    "language":    None,
                    "description": a.get_text(strip=True) or None
                })

        # FIX: always return dict with "metadata" key even if list is empty,
        #      so save_metadata is always called and writes the real state.
        return {"metadata": metadata}

    def images(soup, url):
        """
        FIX: single selector + seen_urls set eliminates duplicates.
        """
        metadata  = []
        parsed    = urlparse(url)
        base_url  = f"{parsed.scheme}://{parsed.netloc}"
        seen_urls: set = set()

        for img in soup.select(".entry-content img"):
            src = img.get("src", "")
            if not src:
                continue
            if re.search(
                r"logo|icon|banner|header|footer|social|arrow|sprite|separator",
                src, re.I
            ):
                continue
            full_url = src if src.startswith("http") else urljoin(base_url, src)
            if full_url.startswith("http://"):
                full_url = full_url.replace("http://", "https://", 1)
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            filename = full_url.split("/")[-1].split("?")[0] or "image.jpg"
            metadata.append({
                "name":        filename,
                "url":         full_url,
                "file_path":   "",
                "version":     None,
                "date":        None,
                "language":    None,
                "description": img.get("alt") or None
            })

        return {"metadata": metadata}


# =============================================================================
# CORE
# =============================================================================
class Core:
    def init(topic_folder, data, update_prices_only=False):
        structure_folders = {
            "documentation":    Core.download_general_files,
            "images":           Core.download_images_files,
            "block_diagrams":   Core.download_block_diagrams_files,
            "design_resources": Core.download_general_files,
            "software_tools":   Core.download_general_files,
            "trainings":        Core.download_general_files,
            "other":            Core.download_general_files,
            "tables":           Core.prepare_products_table,
            "markdowns":        Core.prepare_markdown_file,
        }
        page_type = data.get("page_type", None)
        sf_filter = (
            structure_folders if page_type == "product"
            else {k: v for k, v in structure_folders.items() if k in data}
        )
        Core.create_output_folders(topic_folder, page_type, update_prices_only)

        for folder, method in sf_filter.items():
            final_folder = os.path.join(topic_folder, folder)
            if not data.get(folder):
                continue
            if callable(method):
                try:
                    if folder == "markdowns":
                        success = method(data[folder], final_folder, "overview.md")
                    elif folder == "tables":
                        success = method(data[folder], final_folder, "products.json")
                    else:
                        success = method(data[folder], final_folder, 3, 2, False)
                    logger.info(f"{'✅' if success else '❌'} {folder}")
                except Exception as e:
                    logger.error(f"⚠️  Error in {folder}: {e}")

    def create_output_folders(out_dir, type_name=None, update_prices_only=False):
        os.makedirs(out_dir, exist_ok=True)
        folders = (
            ["documentation", "images", "block_diagrams", "design_resources",
             "software_tools", "tables", "markdowns", "trainings", "other"]
            if type_name == "product"
            else ["tables", "markdowns"]
        )
        dirs = {n: os.path.join(out_dir, n) for n in folders}
        for p in dirs.values():
            os.makedirs(p, exist_ok=True)

        file_map = {"tables": "metadata.json", "markdowns": "overview.md"}
        if not update_prices_only:
            file_map.update({
                "images": "metadata.json", "documentation": "metadata.json",
                "block_diagrams": "block_diagram_mappings.json",
                "design_resources": "metadata.json", "software_tools": "metadata.json",
                "trainings": "metadata.json", "other": "metadata.json",
            })
        for folder, fname in file_map.items():
            if folder in dirs:
                fp = os.path.join(dirs[folder], fname)
                with open(fp, "w", encoding="utf-8") as f:
                    json.dump([], f, indent=2) if fname.endswith(".json") else f.write("")
        return dirs

    # ── HTTP helpers ──────────────────────────────────────────────────────────
    def fetch_html_with_zyte(url: str, max_retries: int = 3, selector=None) -> str:
        if not ZYTE_API_KEY:
            logger.critical("ZYTE_API_KEY not set")
            sys.exit(1)
        payload = {"url": url, "browserHtml": True, "javascript": True}
        if selector:
            payload["actions"] = [{"action": "waitForSelector",
                                   "selector": {"type": "css", "value": selector},
                                   "timeout": 15}]
        for attempt in range(max_retries):
            try:
                resp = requests.post(ZYTE_API_URL, auth=(ZYTE_API_KEY, ""),
                                     json=payload, timeout=90)
                if resp.status_code == 200:
                    d    = resp.json()
                    html = d.get("browserHtml") or d.get("httpResponseBody")
                    if html:
                        return html
                    raise Exception("Empty Zyte response")
                raise Exception(f"Zyte HTTP {resp.status_code}")
            except Exception as e:
                logger.warning(f"Zyte attempt {attempt+1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep((2 ** attempt) + random.uniform(0, 1))
                else:
                    raise

    def get_requests(url, headers=None, timeout=60, stream=False, retries=3):
        headers = headers or {
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"),
            "Accept-Language": "en-US,en;q=0.9",
        }
        for attempt in range(retries):
            try:
                logger.info(f"ℹ️  Request {attempt+1}/{retries}: {url}")
                resp = requests.get(url, headers=headers, timeout=timeout,
                                    stream=stream, verify=CONFIG.get("verify_ssl", True),
                                    impersonate="chrome110")
                resp.raise_for_status()
                logger.info(f"✅ OK: {url}")
                return resp if stream else resp.content
            except RequestsError as e:
                if attempt < retries - 1:
                    w = 2 ** attempt
                    logger.warning(f"⚠️  Retry in {w}s: {e}")
                    time.sleep(w)
                else:
                    logger.error(f"❌ All retries failed, trying Zyte: {url}")
        try:
            logger.info(f"ℹ️  Zyte fallback: {url}")
            ar = requests.post(ZYTE_API_URL, auth=(ZYTE_API_KEY, ""),
                               json={"url": url, "httpResponseBody": True}, timeout=60)
            ar.raise_for_status()
            body = b64decode(ar.json()["httpResponseBody"])
            logger.info(f"✅ Zyte OK: {url}")
            return body
        except RequestsError as e:
            logger.error(f"❌ Zyte fallback failed: {e}")
            return None

    def fetch_html(url: str, runner="request", max_retries=3, selector=None):
        try:
            logger.info("Loading page…")
            if os.path.exists(url):
                return open(url, encoding="utf-8").read()
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

    # ── Writers ───────────────────────────────────────────────────────────────
    def prepare_markdown_file(structure_data, structure_folder, filename="overview.md"):
        if not structure_data or "overview" not in structure_data:
            logger.warning(f"No 'overview' key for {structure_folder}")
            return
        save_path = os.path.join(structure_folder, filename)
        try:
            with open(save_path, "w", encoding="utf-8") as f:
                for chunk in structure_data["overview"]:
                    f.write(chunk); f.write("\n\n")
            logger.info(f"✅ Markdown: {save_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Markdown: {e}")
            return False

    def prepare_products_table(structure_data, structure_folder="tables",
                               filename="products.json"):
        if not structure_data or "products" not in structure_data:
            logger.warning(f"No 'products' key for {structure_folder}")
            return
        products  = structure_data["products"]
        mandatory = ["Product", "product_page_link", "pdf_link",
                     "pdf_filename", "image_url"]
        products_dict = {}

        for product in products:
            if not isinstance(product, dict):
                continue

            # FIX: Never skip a product just because Product key is None.
            # Fall back to 'name' or a placeholder so we always write something.
            if not product.get("Product"):
                product["Product"] = (
                    product.get("name")
                    or product.get("product_page_link", "Unknown").rstrip("/").split("/")[-1]
                )
                logger.warning(f"Product key was empty – using fallback: {product['Product']}")

            for key in mandatory:
                product.setdefault(key, None)
            # Unwrap single-element lists
            for field, value in list(product.items()):
                if isinstance(value, list) and len(value) == 1:
                    product[field] = value[0]

            products_dict[product["Product"]] = product

        save_path = os.path.join(structure_folder, filename)
        try:
            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(products_dict, f, ensure_ascii=False, indent=4)
            logger.info(f"✅ products.json: {save_path} ({len(products_dict)} entries)")
            return True
        except Exception as e:
            logger.error(f"❌ products.json: {e}")
            return False

    def save_metadata(metadata_list, structure_folder="documentation",
                      filename="metadata.json"):
        """
        Write metadata.json.
        FIX: Always writes collected entries even if download failed (file_path="").
             Enriches from PDF metadata only when file actually exists on disk.
        """
        if not isinstance(metadata_list, list):
            logger.warning("save_metadata: expected a list")
            return

        for item in metadata_list:
            if not isinstance(item, dict):
                continue
            pdf_path = item.get("file_path", "")
            if pdf_path and os.path.isfile(pdf_path) and pdf_path.lower().endswith(".pdf"):
                try:
                    reader   = PdfReader(pdf_path)
                    doc_info = reader.metadata or {}
                    if not item.get("description") and doc_info.get("/Title"):
                        item["description"] = doc_info["/Title"]
                    if not item.get("version") and doc_info.get("/Version"):
                        item["version"] = doc_info["/Version"]
                    if not item.get("date") and doc_info.get("/CreationDate"):
                        raw = doc_info["/CreationDate"]
                        item["date"] = (
                            f"{raw[2:6]}-{raw[6:8]}-{raw[8:10]}"
                            if raw.startswith("D:") and len(raw) >= 10 else raw
                        )
                    if not item.get("language") and doc_info.get("/Language"):
                        item["language"] = doc_info["/Language"]
                except Exception as e:
                    logger.warning(f"PDF metadata read failed for {pdf_path}: {e}")

        os.makedirs(structure_folder, exist_ok=True)
        save_path = os.path.join(structure_folder, filename)
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(metadata_list, f, ensure_ascii=False, indent=4)
        logger.info(f"✅ metadata.json: {save_path} ({len(metadata_list)} entries)")
        return save_path

    # ── Download helpers ──────────────────────────────────────────────────────
    def download_images_files(structure_data, structure_folder="images",
                              max_retries=3, retry_delay=2,
                              rename_by_detected_type=False):
        REQUIRED = ["language", "description", "version", "date"]
        if not structure_data or "metadata" not in structure_data:
            logger.warning(f"No metadata for images in {structure_folder}")
            return
        metadata_list      = structure_data["metadata"]
        on_file_downloaded = structure_data.get("callback")
        if not isinstance(metadata_list, list):
            return
        os.makedirs(structure_folder, exist_ok=True)
        seen:         set = set()
        first_done        = False

        for item in metadata_list:
            name = item.get("name"); url = item.get("url")
            for k in REQUIRED: item.setdefault(k, None)
            if not name or not url:
                continue

            base, ext  = os.path.splitext(name)
            final      = name
            save_path  = os.path.join(structure_folder, final)
            c = 1
            while os.path.exists(save_path) or final in seen:
                final = f"{base}({c}){ext}"; save_path = os.path.join(structure_folder, final); c += 1

            attempt = 0; success = False; content = b""; det_ext = ext.lower()
            while attempt < max_retries and not success:
                attempt += 1
                try:
                    logger.info(f"⬇️  Image {final} ({attempt}/{max_retries})")
                    resp = Core.get_requests(url, retries=max_retries, stream=True)
                    if not resp or resp.status_code != 200:
                        raise Exception("Download failed")
                    content = b"".join(resp.iter_content(8192))
                    if not content: raise Exception("Empty")
                    kind    = filetype.guess(content)
                    det_ext = f".{kind.extension}" if kind else ext.lower()
                    if not first_done:
                        final = f"product{det_ext}"
                        save_path = os.path.join(structure_folder, final)
                        first_done = True
                    elif rename_by_detected_type and det_ext != ext.lower():
                        bn, _ = os.path.splitext(final)
                        cc = 0; final = f"{bn}{det_ext}"; save_path = os.path.join(structure_folder, final)
                        while os.path.exists(save_path):
                            cc += 1; final = f"{bn}({cc}){det_ext}"; save_path = os.path.join(structure_folder, final)
                    seen.add(final)
                    with open(save_path, "wb") as f: f.write(content)
                    item["file_path"] = os.path.join(structure_folder, final).replace("\\", "/")
                    item["name"]      = final; success = True
                    logger.info(f"✅ Saved image: {save_path}")
                except Exception as e:
                    logger.error(f"❌ Image attempt {attempt}: {e}")
                    if attempt < max_retries: time.sleep(retry_delay)
                    else: item["file_path"] = "Failed to download"
                finally:
                    if success and callable(on_file_downloaded):
                        try:
                            upd = on_file_downloaded(item, content, det_ext)
                            if isinstance(upd, dict): item.update(upd)
                        except Exception as ce: logger.error(f"⚠️  Callback: {ce}")

        Core.save_metadata(metadata_list, structure_folder)
        return True

    def download_block_diagrams_files(structure_data, structure_folder="documentation",
                                      max_retries=3, retry_delay=2,
                                      rename_by_detected_type=False):
        REQUIRED = ["language", "description", "version", "date"]
        if not structure_data or "metadata" not in structure_data:
            return
        metadata_list      = structure_data["metadata"]
        on_file_downloaded = structure_data.get("callback")
        if not isinstance(metadata_list, list): return
        os.makedirs(structure_folder, exist_ok=True)
        seen: set = set()

        for item in metadata_list:
            name = item.get("name"); url = item.get("url")
            for k in REQUIRED: item.setdefault(k, None)
            if not name or not url: continue
            base, ext = os.path.splitext(name); final = name
            save_path = os.path.join(structure_folder, final); c = 1
            while os.path.exists(save_path) or final in seen:
                final = f"{base}({c}){ext}"; save_path = os.path.join(structure_folder, final); c += 1
            seen.add(final)
            attempt = 0; success = False; content = b""; det_ext = ext.lower(); resp = None
            while attempt < max_retries and not success:
                attempt += 1
                try:
                    logger.info(f"⬇️  Block diagram {final} ({attempt}/{max_retries})")
                    resp = requests.get(url, timeout=60); resp.raise_for_status()
                    content = resp.content
                    if not content: raise Exception("Empty")
                    kind = filetype.guess(content); det_ext = f".{kind.extension}" if kind else ext.lower()
                    if rename_by_detected_type and det_ext != ext.lower():
                        bn, _ = os.path.splitext(final); cc = 1
                        final = f"{bn}({cc}){det_ext}"; save_path = os.path.join(structure_folder, final)
                        while os.path.exists(save_path): cc += 1; final = f"{bn}({cc}){det_ext}"; save_path = os.path.join(structure_folder, final)
                    with open(save_path, "wb") as f: f.write(content)
                    item["file_path"] = os.path.join(structure_folder, final).replace("\\", "/")
                    item["name"] = final; success = True
                    logger.info(f"✅ Saved: {save_path}")
                except Exception as e:
                    logger.error(f"❌ Attempt {attempt}: {e}")
                    if attempt < max_retries: time.sleep(retry_delay)
                    else:
                        st = getattr(resp, "status_code", None)
                        item["file_path"] = f"Failed to download : {st}" if st else "Failed to download"
                finally:
                    if success and callable(on_file_downloaded):
                        try:
                            upd = on_file_downloaded(item, content, det_ext)
                            if isinstance(upd, dict): item.update(upd)
                        except Exception as ce: logger.error(f"⚠️  Callback: {ce}")

        Core.save_metadata(metadata_list, structure_folder, "block_diagram_mappings.json")
        return True

    def get_filename_from_response(response):
        cd = response.headers.get("Content-Disposition", "")
        if not cd: return None
        m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd)
        return m.group(1).strip() if m else None

    def download_general_files(structure_data, structure_folder="documentation",
                               max_retries=3, retry_delay=2,
                               rename_by_detected_type=False):
        REQUIRED = ["language", "description", "version", "date"]
        if not structure_data or "metadata" not in structure_data:
            logger.warning(f"No metadata for {structure_folder}")
            return
        metadata_list      = structure_data["metadata"]
        on_file_downloaded = structure_data.get("callback")
        if not isinstance(metadata_list, list): return
        os.makedirs(structure_folder, exist_ok=True)
        seen: set = set()

        for item in metadata_list:
            name = item.get("name"); url = item.get("url")
            for k in REQUIRED: item.setdefault(k, None)
            if not name or not url: continue

            base, ext = os.path.splitext(name); final = name
            save_path = os.path.join(structure_folder, final); c = 1
            while os.path.exists(save_path) or final in seen:
                final = f"{base}({c}){ext}"; save_path = os.path.join(structure_folder, final); c += 1
            seen.add(final)

            attempt = 0; success = False; content = b""; det_ext = ext.lower(); resp = None
            while attempt < max_retries and not success:
                attempt += 1
                try:
                    logger.info(f"⬇️  File {final} ({attempt}/{max_retries}) from {url}")
                    resp = Core.get_requests(url, retries=max_retries, stream=True)
                    if not resp or resp.status_code != 200: raise Exception("Download failed")
                    content = b"".join(resp.iter_content(8192))
                    if not content: raise Exception("Empty")
                    sfn = Core.get_filename_from_response(resp)
                    if sfn:
                        final = sfn; base, ext = os.path.splitext(final)
                        save_path = os.path.join(structure_folder, final)
                    kind = filetype.guess(content); det_ext = f".{kind.extension}" if kind else ext.lower()
                    if rename_by_detected_type and det_ext != ext.lower():
                        bn, _ = os.path.splitext(final); final = f"{bn}{det_ext}"
                        save_path = os.path.join(structure_folder, final); cc = 1
                        while os.path.exists(save_path): final = f"{bn}({cc}){det_ext}"; save_path = os.path.join(structure_folder, final); cc += 1
                    with open(save_path, "wb") as f: f.write(content)
                    item["file_path"] = save_path.replace("\\", "/")
                    item["name"] = final; success = True
                    logger.info(f"✅ Saved: {save_path}")
                except Exception as e:
                    logger.error(f"❌ Attempt {attempt}: {e}")
                    if attempt < max_retries: time.sleep(retry_delay)
                    else:
                        st = getattr(resp, "status_code", None)
                        item["file_path"] = f"Failed to download : {st}" if st else "Failed to download"
                finally:
                    if success and callable(on_file_downloaded):
                        try:
                            upd = on_file_downloaded(item, content, det_ext)
                            if isinstance(upd, dict): item.update(upd)
                        except Exception as ce: logger.error(f"⚠️  Callback: {ce}")

        # FIX: save_metadata is called regardless of whether any file succeeded,
        #      so metadata.json always reflects collected links (not just downloads).
        Core.save_metadata(metadata_list, structure_folder)
        return True

    # ── Markdown builder ──────────────────────────────────────────────────────
    def write_overview_markdown(soup, div_selector, section_title=None, url=None):
        div = soup.select_one(div_selector)
        if not div: return ""
        parsed   = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        for tag in div.select("a[href], img[src]"):
            if tag.name == "a":
                h = tag.get("href", "")
                if h.startswith("javascript"): tag["href"] = ""
                elif h.startswith("/"): tag["href"] = base_url.rstrip("/") + h
            elif tag.name == "img":
                s = tag.get("src", "")
                if s.startswith("/"): tag["src"] = base_url.rstrip("/") + s
        for btn in div.select("button[onclick]"):
            onclick = btn.get("onclick", "")
            m = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", onclick)
            if not m: continue
            href = m.group(1)
            if href.startswith("/"): href = base_url.rstrip("/") + href
            a = soup.new_tag("a", href=href); a.string = btn.get_text(strip=True) or "Download"
            btn.replace_with(a)
        html_content = div.decode_contents().strip()
        if not html_content: return ""
        markdown_text = md(html_content, heading_style="ATX")
        markdown_text = Core._html_to_str(markdown_text)
        markdown_text = Core.clean_html_spaces(markdown_text)
        markdown_text = re.sub(r"\n{3,}", "\n\n", markdown_text.strip())
        markdown_text = "\n".join(line.strip() for line in markdown_text.splitlines())
        header = f"## {section_title}\n\n" if section_title else ""
        return header + markdown_text.strip() + "\n"

    def _html_to_str(html):
        if not html: return ""
        if isinstance(html, list): return " ".join(str(x) for x in html if x)
        if isinstance(html, (dict, int, float)): return str(html)
        return str(html)

    def clean_html_spaces(text: str, full_clean=False) -> str:
        if not text: return ""
        text = text.replace("&nbsp;", " ").replace("\xa0", " ").replace("\u00a0", " ")
        return text if not full_clean else re.sub(r"\s+", " ", text).strip()

    def fix_lazy_loaded_images(soup):
        for img in soup.find_all("img"):
            src      = img.get("src", "")
            real_src = (img.get("data-amsrc") or img.get("data-src")
                        or img.get("data-lazy-src") or img.get("data-original"))
            if (not src or src.startswith("data:image")) and real_src:
                img["src"] = real_src
        return soup


# =============================================================================
# PAGE-TYPE DETECTION  –  HTML structure only
# =============================================================================
def detect_page_type(soup: BeautifulSoup) -> str:
    """
    part     → ≤2 article.post  AND  (spec table  OR  datasheet link)
    category → >2 article.post  (relay card grid)

    FIX: _is_datasheet_link() now catches 'Data Sheet' text links,
         so pages without a <table> but with a PDF text link are
         correctly detected as 'part'.
    """
    articles      = soup.select("article.post")
    entry_content = soup.select_one(".entry-content")

    has_spec_table = bool(entry_content and entry_content.select("table"))

    has_ds_link = False
    if entry_content:
        for a in entry_content.select("a[href]"):
            if _is_datasheet_link(a):
                has_ds_link = True
                break

    if len(articles) <= 2 and (has_spec_table or has_ds_link):
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
            logger.error(f"Empty HTML for {url}")
            return None
        soup = BeautifulSoup(html, "html.parser")
        Core.fix_lazy_loaded_images(soup)
    except Exception as e:
        logger.error(f"Failed to load {url}: {e}")
        return None

    page_type = detect_page_type(soup)
    logger.info(f"🔍 Detected page type: {page_type.upper()}")

    if page_type == "category":
        crawl_array["page_type"] = "category"
        crawl_array["markdowns"] = Category.markdown(soup, url)
        crawl_array["tables"]    = Category.tables(soup, url)

    elif page_type == "part":
        crawl_array["page_type"] = "product"        # Core.init expects "product"
        crawl_array["markdowns"] = Product.markdown(soup, url)
        crawl_array["tables"]    = Product.tables(soup, url)
        if not update_prices_only:
            crawl_array["documentation"] = Product.documentation(soup)
            crawl_array["images"]        = Product.images(soup, url)

    return crawl_array


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="songchuanusa.com scraper")
    parser.add_argument("--url",  required=True, help="URL or local HTML file to scrape")
    parser.add_argument("--out",  required=True, help="Output directory")
    parser.add_argument("--update-only-prices", action="store_true")
    args = parser.parse_args()

    crawl_array = init(args.url, args.update_only_prices)
    if not crawl_array:
        logger.error("Extraction failed.")
        sys.exit(1)
    Core.init(args.out, crawl_array, args.update_only_prices)
    logger.info("✅ Done.")
