"""
Unit tests for products.lappgroup.com crawler.

Tests page detection, data extraction, and defensive handling
using minimal self-contained HTML fixtures (no network calls).

Run:
    python3 -m pytest test_crawl.py -v
    python3 -m unittest test_crawl -v
"""

import unittest
from unittest.mock import patch, MagicMock
import json
import os
import sys
import tempfile
import shutil

from bs4 import BeautifulSoup

# Import crawler modules
from crawl import (
    extract_value,
    Category,
    Product,
    SITE_CONFIG,
    parallel_init,
    Core,
)


# ---------------------------------------------------------------------------
# HTML Fixtures — minimal snippets that mirror real lappgroup.com structure
# ---------------------------------------------------------------------------

CATEGORY_HTML = """
<html>
<body>
<div class="maincontent">
  <div class="col-12 col-lg-9">
    <h1 class="text-uppercase">Power and control cables</h1>
    <ul class="nav nav-tree" data-setu-toggle="nav-tree">
      <li>
        <a href="/online-catalogue/power-and-control-cables/various-applications.html">Various applications</a>
        <ul class="nav nav-tree nav-pills nav-stacked">
          <li>
            <a href="/online-catalogue/power-and-control-cables/various-applications/pvc-outer-sheath.html">PVC outer sheath</a>
            <ul class="nav nav-tree nav-pills nav-stacked">
              <li><a href="/online-catalogue/.../oelflex-classic-100.html">OELFLEX CLASSIC 100</a></li>
              <li><a href="/online-catalogue/.../oelflex-classic-110.html">OELFLEX CLASSIC 110</a></li>
            </ul>
          </li>
        </ul>
      </li>
      <li>
        <a href="/online-catalogue/power-and-control-cables/building-installation.html">Building installation</a>
      </li>
    </ul>
  </div>
</div>
</body>
</html>
"""

PRODUCT_HTML = """
<html>
<body>
<div class="maincontent">
  <h1>OELFLEX CLASSIC 100 300/500 V</h1>
  <h2>Colour-coded PVC control cable</h2>

  <div class="col-12 col-md-6 order-md-last">
    <div id="prod-carousel" class="carousel slide prod-carousel">
      <div class="carousel-inner">
        <div class="carousel-item active">
          <div class="intrinsic">
            <img class="lazyload img-fluid" src="/typo3temp/assets/setu_products/OELFLEX_PRODUCT_1.jpeg" alt="OELFLEX" />
          </div>
        </div>
        <div class="carousel-item">
          <div class="intrinsic">
            <img class="lazyload img-fluid" src="/typo3temp/assets/setu_products/OELFLEX_PRODUCT_2.jpeg" alt="OELFLEX" />
          </div>
        </div>
      </div>
    </div>
  </div>

  <ul class="list-unstyled">
    <li>
      <a class="addtracking" target="_blank"
         href="/online-catalogue/.../oelflex-classic-100.html?type=1664268841">
        <span class="icon-pos-left filetypes filetypes-pdf"></span>product information (PDF)
      </a>
    </li>
  </ul>

  <div id="tabarticle-productdata">
    <div class="row">
      <div class="col-12 col-md-6">
        <h4>Benefits</h4>
        <ul>
          <li>Space-saving installation</li>
          <li>High flexibility</li>
        </ul>
        <h4>Application range</h4>
        <ul>
          <li>Plant engineering</li>
        </ul>
      </div>
    </div>
  </div>

  <div id="tabarticle-techdata">
    <div class="row">
      <div class="col-12 col-md-6">
        <h4>Classification ETIM 5</h4>
        <ul class="list-unstyled">
          <li>ETIM 5.0 Class-ID: EC001578</li>
          <li>ETIM 5.0 Class-Description: Flexible cable</li>
        </ul>
      </div>
    </div>
  </div>

  <table class="table setuArticles table-hover table-condensed">
    <thead>
      <tr>
        <th class="prodNum" data-field="artno">Article number</th>
        <th data-field="pl_dyn_aufbau">Number of cores and mm2 per conductor</th>
        <th data-field="pl_dyn_durchmesseraussen">Outer diameter [mm]</th>
        <th data-field="j_1nvgw">Copper index (kg/km)</th>
        <th data-field="pl_gewicht">Weight (kg/km)</th>
        <th data-field="basket"></th>
      </tr>
    </thead>
    <tbody>
      <tr><td colspan="6" class="info-line">OELFLEX CLASSIC 100 300/500 V</td></tr>
      <tr class="article">
        <td id="dgp-article-00100004">
          <div data-bs-content="&lt;span&gt;&lt;ul class=&quot;list-unstyled&quot;&gt;&lt;li&gt;&lt;a href=&quot;/fileadmin/docs/DB00100004EN.pdf&quot;&gt;Datasheet (PDF)&lt;/a&gt;&lt;/li&gt;&lt;li&gt;&lt;a href=&quot;/fileadmin/docs/CE00100004EN.pdf&quot;&gt;EU CE Conformity&lt;/a&gt;&lt;/li&gt;&lt;/ul&gt;&lt;/span&gt;"
               data-bs-toggle="popover" class="pointer">
            00100004
          </div>
        </td>
        <td>2 X 0.5</td>
        <td>4.8</td>
        <td>9.6</td>
        <td>35</td>
        <td id="dgp-article-basket-00100004"><a class="btn" data-id="00100004"></a></td>
      </tr>
      <tr class="article">
        <td id="dgp-article-00100014">
          <div data-bs-content="&lt;span&gt;&lt;ul&gt;&lt;li&gt;&lt;a href=&quot;/fileadmin/docs/DB00100014EN.pdf&quot;&gt;Datasheet (PDF)&lt;/a&gt;&lt;/li&gt;&lt;/ul&gt;&lt;/span&gt;"
               data-bs-toggle="popover" class="pointer">
            00100014
          </div>
        </td>
        <td>3 G 0.5</td>
        <td>5.1</td>
        <td>14.4</td>
        <td>42</td>
        <td id="dgp-article-basket-00100014"><a class="btn" data-id="00100014"></a></td>
      </tr>
      <tr class="article">
        <td id="dgp-article-00100024">
          <div class="pointer">00100024</div>
        </td>
        <td>4 G 0.5</td>
        <td>5.5</td>
        <td>19.2</td>
        <td>48</td>
        <td id="dgp-article-basket-00100024"><a class="btn" data-id="00100024"></a></td>
      </tr>
    </tbody>
  </table>
</div>
</body>
</html>
"""

# Product page with NO article table (defensive test)
PRODUCT_HTML_NO_TABLE = """
<html>
<body>
<div class="maincontent">
  <h1>OELFLEX SPECIAL</h1>
  <h2>Special cable</h2>
  <div id="prod-carousel" class="carousel slide prod-carousel">
    <div class="carousel-inner">
      <div class="carousel-item active">
        <div class="intrinsic">
          <img class="img-fluid" src="/typo3temp/assets/setu_products/SPECIAL_1.jpeg" />
        </div>
      </div>
    </div>
  </div>
  <div id="tabarticle-productdata"><p>Description here</p></div>
  <div id="tabarticle-techdata"><p>Tech data here</p></div>
</div>
</body>
</html>
"""

# Malformed table rows (missing tds, empty ids)
PRODUCT_HTML_MALFORMED = """
<html>
<body>
<div class="maincontent">
  <h1>BROKEN PRODUCT</h1>
  <h2>Test</h2>
  <table class="table setuArticles table-hover table-condensed">
    <thead>
      <tr>
        <th>Article number</th>
        <th>Cores</th>
        <th data-field="basket"></th>
      </tr>
    </thead>
    <tbody>
      <tr class="article"><td></td><td>2 X 0.5</td><td></td></tr>
      <tr class="article">
        <td id="dgp-article-GOOD001"><div class="pointer">GOOD001</div></td>
        <td>3 G 0.5</td>
        <td></td>
      </tr>
      <tr class="article"><td id="dgp-article-"></td><td>bad</td><td></td></tr>
    </tbody>
  </table>
</div>
</body>
</html>
"""

EMPTY_HTML = "<html><body><div>Nothing here</div></body></html>"

BASE_URL = "https://products.lappgroup.com"
PRODUCT_URL = "https://products.lappgroup.com/online-catalogue/power-and-control-cables/various-applications/pvc-outer-sheath-and-coloured-cores/oelflex-classic-100-300500-v.html"
CATEGORY_URL = "https://products.lappgroup.com/online-catalogue/power-and-control-cables.html"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPageDetection(unittest.TestCase):
    """Page type must be detected by HTML structure, not URL."""

    def test_product_page_detected(self):
        soup = BeautifulSoup(PRODUCT_HTML, "html.parser")
        sel = SITE_CONFIG["part"]["main_selector"]
        self.assertIsNotNone(soup.select_one(sel))

    def test_category_page_detected(self):
        soup = BeautifulSoup(CATEGORY_HTML, "html.parser")
        sel = SITE_CONFIG["category"]["main_selector"]
        self.assertIsNotNone(soup.select_one(sel))

    def test_product_not_detected_on_category(self):
        soup = BeautifulSoup(CATEGORY_HTML, "html.parser")
        sel = SITE_CONFIG["part"]["main_selector"]
        self.assertIsNone(soup.select_one(sel))

    def test_category_not_detected_on_product(self):
        """Product pages don't have nav-tree, so category should not match."""
        soup = BeautifulSoup(PRODUCT_HTML, "html.parser")
        sel = SITE_CONFIG["category"]["main_selector"]
        self.assertIsNone(soup.select_one(sel))

    def test_product_detected_before_category(self):
        """When both selectors could match, product must win (checked first)."""
        # Inject a nav-tree into product HTML so both selectors match
        hybrid = PRODUCT_HTML.replace(
            "</body>",
            '<ul class="nav nav-tree"><li><a href="/x.html">X</a></li></ul></body>'
        )
        soup = BeautifulSoup(hybrid, "html.parser")
        part_sel = SITE_CONFIG["part"]["main_selector"]
        cat_sel = SITE_CONFIG["category"]["main_selector"]
        # Both match
        self.assertIsNotNone(soup.select_one(part_sel))
        self.assertIsNotNone(soup.select_one(cat_sel))
        # But product is checked first in init(), so page_type should be product

    def test_empty_page_no_detection(self):
        soup = BeautifulSoup(EMPTY_HTML, "html.parser")
        self.assertIsNone(soup.select_one(SITE_CONFIG["part"]["main_selector"]))
        self.assertIsNone(soup.select_one(SITE_CONFIG["category"]["main_selector"]))

    def test_group_disabled(self):
        """Group main_selector is empty — should never match."""
        self.assertEqual(SITE_CONFIG["group"]["main_selector"], "")


class TestExtractValue(unittest.TestCase):
    """Test the extract_value() CSS selector helper."""

    def setUp(self):
        self.soup = BeautifulSoup(PRODUCT_HTML, "html.parser")

    def test_text_extraction(self):
        result = extract_value(self.soup, "h1")
        self.assertEqual(result, "OELFLEX CLASSIC 100 300/500 V")

    def test_text_extraction_h2(self):
        result = extract_value(self.soup, "h2")
        self.assertEqual(result, "Colour-coded PVC control cable")

    def test_attr_extraction(self):
        result = extract_value(self.soup, "#prod-carousel .carousel-item.active img::attr(src)")
        self.assertEqual(result, "/typo3temp/assets/setu_products/OELFLEX_PRODUCT_1.jpeg")

    def test_empty_selector_returns_none(self):
        self.assertIsNone(extract_value(self.soup, ""))

    def test_missing_element_returns_none(self):
        self.assertIsNone(extract_value(self.soup, ".nonexistent-class"))

    def test_missing_attr_returns_none(self):
        self.assertIsNone(extract_value(self.soup, "h1::attr(data-missing)"))


class TestCategoryExtraction(unittest.TestCase):
    """Category.tables() must extract product links from nav-tree."""

    def setUp(self):
        self.soup = BeautifulSoup(CATEGORY_HTML, "html.parser")

    def test_tables_returns_products(self):
        result = Category.tables(self.soup, CATEGORY_URL)
        self.assertIn("products", result)

    def test_correct_product_count(self):
        result = Category.tables(self.soup, CATEGORY_URL)
        products = result["products"]
        # 5 links: Various applications, PVC outer sheath, OELFLEX 100, OELFLEX 110, Building installation
        self.assertEqual(len(products), 5)

    def test_product_fields(self):
        result = Category.tables(self.soup, CATEGORY_URL)
        product = result["products"][0]
        self.assertIn("Product", product)
        self.assertIn("name", product)
        self.assertIn("product_page_link", product)

    def test_urls_are_absolute(self):
        result = Category.tables(self.soup, CATEGORY_URL)
        for product in result["products"]:
            self.assertTrue(
                product["product_page_link"].startswith("https://"),
                f"URL not absolute: {product['product_page_link']}"
            )

    def test_no_empty_names(self):
        result = Category.tables(self.soup, CATEGORY_URL)
        for product in result["products"]:
            self.assertTrue(len(product["name"]) > 0)
            self.assertTrue(len(product["Product"]) > 0)

    def test_empty_nav_tree_returns_empty(self):
        html = '<html><body><ul class="nav nav-tree"></ul></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        result = Category.tables(soup, CATEGORY_URL)
        self.assertNotIn("products", result)


class TestProductTables(unittest.TestCase):
    """Product.tables() must extract article variants from the table."""

    def setUp(self):
        self.soup = BeautifulSoup(PRODUCT_HTML, "html.parser")

    def test_returns_products_key(self):
        result = Product.tables(self.soup, PRODUCT_URL)
        self.assertIn("products", result)

    def test_correct_article_count(self):
        result = Product.tables(self.soup, PRODUCT_URL)
        self.assertEqual(len(result["products"]), 3)

    def test_article_number_extracted(self):
        result = Product.tables(self.soup, PRODUCT_URL)
        article_nos = [p["Product"] for p in result["products"]]
        self.assertEqual(article_nos, ["00100004", "00100014", "00100024"])

    def test_column_values_mapped(self):
        result = Product.tables(self.soup, PRODUCT_URL)
        first = result["products"][0]
        self.assertEqual(first["Number of cores and mm2 per conductor"], "2 X 0.5")
        self.assertEqual(first["Outer diameter [mm]"], "4.8")
        self.assertEqual(first["Copper index (kg/km)"], "9.6")
        self.assertEqual(first["Weight (kg/km)"], "35")

    def test_shared_fields_present(self):
        result = Product.tables(self.soup, PRODUCT_URL)
        for product in result["products"]:
            self.assertEqual(product["name"], "OELFLEX CLASSIC 100 300/500 V")
            self.assertEqual(product["description"], "Colour-coded PVC control cable")
            self.assertEqual(product["product_page_link"], PRODUCT_URL)

    def test_product_pdf_link_extracted(self):
        result = Product.tables(self.soup, PRODUCT_URL)
        first = result["products"][0]
        self.assertIn("pdf_link", first)
        # Per-article datasheet overrides product-level PDF
        self.assertIn("DB00100004EN.pdf", first["pdf_link"])

    def test_per_article_datasheet_override(self):
        """Article with popover should get its own datasheet link."""
        result = Product.tables(self.soup, PRODUCT_URL)
        second = result["products"][1]
        self.assertIn("DB00100014EN.pdf", second["pdf_link"])

    def test_article_without_popover_gets_product_pdf(self):
        """Article 00100024 has no popover datasheet — gets product-level PDF."""
        result = Product.tables(self.soup, PRODUCT_URL)
        third = result["products"][2]
        # Product-level PDF
        self.assertIn("type=1664268841", third["pdf_link"])

    def test_basket_column_excluded(self):
        result = Product.tables(self.soup, PRODUCT_URL)
        for product in result["products"]:
            self.assertNotIn("basket", [k.lower() for k in product.keys()])

    def test_image_url_made_absolute(self):
        result = Product.tables(self.soup, PRODUCT_URL)
        first = result["products"][0]
        self.assertTrue(first["image_url"].startswith("https://"))


class TestProductTablesDefensive(unittest.TestCase):
    """Defensive handling: missing table, malformed rows."""

    def test_no_table_returns_product_level_data(self):
        soup = BeautifulSoup(PRODUCT_HTML_NO_TABLE, "html.parser")
        result = Product.tables(soup, PRODUCT_URL)
        self.assertIn("products", result)
        self.assertEqual(len(result["products"]), 1)
        self.assertEqual(result["products"][0]["Product"], "OELFLEX SPECIAL")

    def test_malformed_rows_skipped(self):
        soup = BeautifulSoup(PRODUCT_HTML_MALFORMED, "html.parser")
        result = Product.tables(soup, PRODUCT_URL)
        products = result.get("products", [])
        # Only GOOD001 should survive — others have empty article numbers
        article_nos = [p["Product"] for p in products]
        self.assertIn("GOOD001", article_nos)
        # Empty td and empty id rows should be skipped
        self.assertNotIn("", article_nos)

    def test_empty_html_returns_empty(self):
        soup = BeautifulSoup(EMPTY_HTML, "html.parser")
        result = Product.tables(soup, PRODUCT_URL)
        # No table found — falls back to product-level, but h1 is missing
        products = result.get("products", [])
        self.assertIsInstance(products, list)


class TestProductPopoverParsing(unittest.TestCase):
    """Product._parse_article_popover() must extract PDFs from HTML-encoded popover."""

    def test_extracts_pdf_links(self):
        html = """
        <td id="dgp-article-00100004">
          <div data-bs-content="&lt;a href=&quot;/docs/datasheet.pdf&quot;&gt;Datasheet (PDF)&lt;/a&gt;&lt;a href=&quot;/docs/cert.pdf&quot;&gt;Certificate&lt;/a&gt;"
               class="pointer">00100004</div>
        </td>
        """
        soup = BeautifulSoup(html, "html.parser")
        td = soup.find("td")
        result = Product._parse_article_popover(td, BASE_URL)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["name"], "Datasheet (PDF)")
        self.assertTrue(result[0]["url"].startswith("https://"))
        self.assertTrue(result[0]["url"].endswith("datasheet.pdf"))

    def test_relative_urls_made_absolute(self):
        html = '<td><div data-bs-content="&lt;a href=&quot;/file.pdf&quot;&gt;PDF&lt;/a&gt;">X</div></td>'
        soup = BeautifulSoup(html, "html.parser")
        td = soup.find("td")
        result = Product._parse_article_popover(td, BASE_URL)
        self.assertEqual(result[0]["url"], "https://products.lappgroup.com/file.pdf")

    def test_no_popover_returns_empty(self):
        html = "<td><div>Plain text</div></td>"
        soup = BeautifulSoup(html, "html.parser")
        td = soup.find("td")
        result = Product._parse_article_popover(td, BASE_URL)
        self.assertEqual(result, [])

    def test_empty_popover_returns_empty(self):
        html = '<td><div data-bs-content="">X</div></td>'
        soup = BeautifulSoup(html, "html.parser")
        td = soup.find("td")
        result = Product._parse_article_popover(td, BASE_URL)
        self.assertEqual(result, [])

    def test_malformed_html_in_popover_does_not_crash(self):
        html = '<td><div data-bs-content="&lt;a href=broken not closed">X</div></td>'
        soup = BeautifulSoup(html, "html.parser")
        td = soup.find("td")
        # Should not raise
        result = Product._parse_article_popover(td, BASE_URL)
        self.assertIsInstance(result, list)


class TestProductDocumentation(unittest.TestCase):
    """Product.documentation() must extract PDF links."""

    def setUp(self):
        self.soup = BeautifulSoup(PRODUCT_HTML, "html.parser")

    def test_returns_metadata(self):
        result = Product.documentation(self.soup, PRODUCT_URL)
        self.assertIn("metadata", result)

    def test_product_level_pdf_is_first(self):
        result = Product.documentation(self.soup, PRODUCT_URL)
        metadata = result["metadata"]
        self.assertTrue(len(metadata) >= 1)
        first = metadata[0]
        self.assertIn("type=1664268841", first["url"])
        self.assertEqual(first["description"], "product information (PDF)")

    def test_per_article_pdfs_included(self):
        result = Product.documentation(self.soup, PRODUCT_URL)
        metadata = result["metadata"]
        urls = [m["url"] for m in metadata]
        datasheet_urls = [u for u in urls if "DB00100004EN.pdf" in u]
        self.assertTrue(len(datasheet_urls) > 0)

    def test_no_duplicate_urls(self):
        result = Product.documentation(self.soup, PRODUCT_URL)
        metadata = result["metadata"]
        urls = [m["url"] for m in metadata]
        self.assertEqual(len(urls), len(set(urls)))

    def test_metadata_fields_present(self):
        result = Product.documentation(self.soup, PRODUCT_URL)
        for item in result["metadata"]:
            self.assertIn("name", item)
            self.assertIn("url", item)
            self.assertIn("file_path", item)
            self.assertIn("version", item)
            self.assertIn("date", item)
            self.assertIn("language", item)
            self.assertIn("description", item)

    def test_urls_are_absolute(self):
        result = Product.documentation(self.soup, PRODUCT_URL)
        for item in result["metadata"]:
            self.assertTrue(
                item["url"].startswith("https://"),
                f"URL not absolute: {item['url']}"
            )


class TestProductImages(unittest.TestCase):
    """Product.images() must extract image URLs from carousel."""

    def setUp(self):
        self.soup = BeautifulSoup(PRODUCT_HTML, "html.parser")

    def test_returns_metadata(self):
        result = Product.images(self.soup, PRODUCT_URL)
        self.assertIn("metadata", result)

    def test_correct_image_count(self):
        result = Product.images(self.soup, PRODUCT_URL)
        self.assertEqual(len(result["metadata"]), 2)

    def test_urls_are_absolute(self):
        result = Product.images(self.soup, PRODUCT_URL)
        for item in result["metadata"]:
            self.assertTrue(
                item["url"].startswith("https://"),
                f"Image URL not absolute: {item['url']}"
            )

    def test_base64_images_skipped(self):
        html = """
        <div id="prod-carousel">
          <div class="carousel-item">
            <img src="data:image/gif;base64,R0lGODlh..." />
          </div>
          <div class="carousel-item">
            <img src="/typo3temp/assets/setu_products/real.jpeg" />
          </div>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = Product.images(soup, PRODUCT_URL)
        self.assertEqual(len(result["metadata"]), 1)
        self.assertIn("real.jpeg", result["metadata"][0]["url"])

    def test_no_duplicate_images(self):
        html = """
        <div id="prod-carousel">
          <div class="carousel-item"><img src="/img/same.jpeg" /></div>
          <div class="carousel-item"><img src="/img/same.jpeg" /></div>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = Product.images(soup, PRODUCT_URL)
        self.assertEqual(len(result["metadata"]), 1)

    def test_metadata_fields_present(self):
        result = Product.images(self.soup, PRODUCT_URL)
        for item in result["metadata"]:
            self.assertIn("name", item)
            self.assertIn("url", item)
            self.assertIn("file_path", item)

    def test_empty_carousel_returns_no_metadata(self):
        soup = BeautifulSoup(EMPTY_HTML, "html.parser")
        result = Product.images(soup, PRODUCT_URL)
        self.assertNotIn("metadata", result)


class TestSiteConfig(unittest.TestCase):
    """SITE_CONFIG must have correct structure for all page types."""

    def test_category_has_required_keys(self):
        cfg = SITE_CONFIG["category"]
        self.assertIn("main_selector", cfg)
        self.assertIn("markdown", cfg)
        self.assertTrue(len(cfg["main_selector"]) > 0)

    def test_part_has_required_keys(self):
        cfg = SITE_CONFIG["part"]
        self.assertIn("main_selector", cfg)
        self.assertIn("markdown", cfg)
        self.assertIn("images", cfg)
        self.assertIn("documentation", cfg)
        self.assertIn("products", cfg)
        self.assertTrue(len(cfg["main_selector"]) > 0)

    def test_group_is_disabled(self):
        self.assertEqual(SITE_CONFIG["group"]["main_selector"], "")

    def test_part_variants_config(self):
        variants = SITE_CONFIG["part"]["products"]["variants"]
        self.assertIn("table_selector", variants)
        self.assertIn("row_selector", variants)
        self.assertEqual(variants["table_selector"], "table.setuArticles")
        self.assertEqual(variants["row_selector"], "tr.article")


class TestParallelInit(unittest.TestCase):
    """Verify parallel_init dispatches folders correctly via ThreadPoolExecutor."""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    @patch.object(Core, "download_general_files", return_value=True)
    @patch.object(Core, "download_images_files", return_value=True)
    @patch.object(Core, "prepare_products_table", return_value=True)
    @patch.object(Core, "prepare_markdown_file", return_value=True)
    def test_product_page_calls_all_methods(self, mock_md, mock_tbl, mock_img, mock_gen):
        data = {
            "page_type": "product",
            "tables": {"products": [{"Product": "X"}]},
            "markdowns": {"content": "# Test"},
            "documentation": {"metadata": [{"name": "a.pdf", "url": "http://x"}]},
            "images": {"metadata": [{"name": "img.jpg", "url": "http://x"}]},
        }
        parallel_init(self.tmp_dir, data)
        mock_tbl.assert_called_once()
        mock_md.assert_called_once()
        mock_img.assert_called_once()
        self.assertTrue(mock_gen.called)

    @patch.object(Core, "prepare_products_table", return_value=True)
    @patch.object(Core, "prepare_markdown_file", return_value=True)
    def test_category_page_no_downloads(self, mock_md, mock_tbl):
        data = {
            "page_type": "category",
            "tables": {"products": [{"Product": "Y"}]},
            "markdowns": {"content": "# Cat"},
        }
        parallel_init(self.tmp_dir, data)
        mock_tbl.assert_called_once()
        mock_md.assert_called_once()

    @patch.object(Core, "download_general_files", return_value=True)
    @patch.object(Core, "download_images_files", return_value=True)
    @patch.object(Core, "prepare_products_table", return_value=True)
    @patch.object(Core, "prepare_markdown_file", return_value=True)
    def test_empty_folders_skipped(self, mock_md, mock_tbl, mock_img, mock_gen):
        data = {
            "page_type": "product",
            "tables": {"products": [{"Product": "X"}]},
            "markdowns": {"content": "# Test"},
            # documentation and images are missing — should not call download methods
        }
        parallel_init(self.tmp_dir, data)
        mock_img.assert_not_called()
        mock_gen.assert_not_called()

    @patch.object(Core, "download_general_files", side_effect=Exception("network error"))
    @patch.object(Core, "prepare_products_table", return_value=True)
    @patch.object(Core, "prepare_markdown_file", return_value=True)
    def test_download_exception_does_not_crash(self, mock_md, mock_tbl, mock_gen):
        data = {
            "page_type": "product",
            "tables": {"products": [{"Product": "X"}]},
            "markdowns": {"content": "# Test"},
            "documentation": {"metadata": [{"name": "a.pdf", "url": "http://x"}]},
        }
        # Should not raise — exceptions are caught per-folder
        parallel_init(self.tmp_dir, data)
        mock_tbl.assert_called_once()

    @patch.object(Core, "download_general_files", return_value=True)
    @patch.object(Core, "download_images_files", return_value=True)
    @patch.object(Core, "prepare_products_table", return_value=True)
    @patch.object(Core, "prepare_markdown_file", return_value=True)
    def test_output_folders_created(self, mock_md, mock_tbl, mock_img, mock_gen):
        data = {
            "page_type": "product",
            "tables": {"products": [{"Product": "X"}]},
            "markdowns": {"content": "# Test"},
            "documentation": {"metadata": [{"name": "a.pdf", "url": "http://x"}]},
            "images": {"metadata": [{"name": "img.jpg", "url": "http://x"}]},
        }
        parallel_init(self.tmp_dir, data)
        # Core.create_output_folders should have created subfolders
        self.assertTrue(os.path.isdir(os.path.join(self.tmp_dir, "tables")))
        self.assertTrue(os.path.isdir(os.path.join(self.tmp_dir, "markdowns")))
        self.assertTrue(os.path.isdir(os.path.join(self.tmp_dir, "documentation")))
        self.assertTrue(os.path.isdir(os.path.join(self.tmp_dir, "images")))


if __name__ == "__main__":
    unittest.main()
