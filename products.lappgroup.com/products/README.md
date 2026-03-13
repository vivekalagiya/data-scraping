# products.lappgroup.com - Web Data Extraction Tool

A Python-based web crawler that extracts structured product data from [products.lappgroup.com](https://products.lappgroup.com) (LAPP Group online cable catalogue).

The script automatically detects page type and generates organized output with product tables, downloadable assets, and markdown documentation.

---

## Setup

**Requirements:** Python 3.8+

```bash
cd products.lappgroup.com/products

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Dependencies

| Package | Purpose |
|---------|---------|
| `beautifulsoup4` | HTML parsing and element selection |
| `curl-cffi` | HTTP requests with browser impersonation |
| `markdownify` | HTML to Markdown conversion |
| `filetype` | Downloaded file type detection |
| `PyPDF2` | PDF metadata extraction (date, version) |
| `jsonschema` | JSON structure validation |

---

## Usage

```bash
python3 crawl.py --url <PAGE_URL> --out <OUTPUT_DIRECTORY>
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--url` | Yes | URL of the page to scrape (category or product) |
| `--out` | Yes | Output directory path |
| `--update-only-prices` | No | Only update pricing data (skip file downloads) |

### Examples

**Scrape a category page:**

```bash
python3 crawl.py \
  --url "https://products.lappgroup.com/online-catalogue/power-and-control-cables.html" \
  --out output/category
```

**Scrape a product page:**

```bash
python3 crawl.py \
  --url "https://products.lappgroup.com/online-catalogue/power-and-control-cables/various-applications/pvc-outer-sheath-and-coloured-cores/oelflex-classic-100-300500-v.html" \
  --out output/product
```

---

## Page Type Detection

The crawler automatically detects page type using **HTML structure** (not URL patterns):

| Page Type | Detection Method | HTML Marker |
|-----------|-----------------|-------------|
| **Product** | Checked first | Presence of `<table class="setuArticles">` (article variant table) |
| **Category** | Checked second | Presence of `<ul class="nav nav-tree">` (nested link tree) |

This works for any category, subcategory, or product page on the site.

---

## Output Structure

### Category Page

```
output/category/
├── tables/
│   ├── products.json        # All subcategory/product links
│   └── metadata.json
└── markdowns/
    └── overview.md          # Full category tree as Markdown
```

### Product Page

```
output/product/
├── documentation/           # Downloaded PDFs (datasheets, certificates)
│   ├── LAPP_PRO100890EN.pdf
│   ├── DB00100004EN.pdf
│   └── metadata.json
├── images/                  # Product images
│   ├── product.jpg          # Main product image (always first)
│   ├── *.jpeg               # Additional images
│   └── metadata.json
├── block_diagrams/
│   └── metadata.json
├── design_resources/
│   └── metadata.json
├── software_tools/
│   └── metadata.json
├── tables/
│   ├── products.json        # Article variants keyed by article number
│   └── metadata.json
├── markdowns/
│   └── overview.md          # Product description + technical data
├── trainings/
│   └── metadata.json
└── other/
    └── metadata.json
```

---

## Output Formats

### products.json (Product Page)

Each article variant is keyed by its article number:

```json
{
  "00100004": {
    "Product": "00100004",
    "name": "OELFLEX CLASSIC 100 300/500 V",
    "description": "Colour-coded PVC control cable",
    "product_page_link": "https://products.lappgroup.com/...",
    "pdf_link": "https://products.lappgroup.com/.../DB00100004EN.pdf",
    "pdf_filename": "DB00100004EN.pdf",
    "image_url": "https://products.lappgroup.com/.../OELFLEX_00100004_PRODUCT_1_1.jpeg",
    "Number of cores and mm2 per conductor": "2 X 0.5",
    "Outer diameter [mm]": "4.8",
    "Copper index (kg/km)": "9.6",
    "Weight (kg/km)": "35"
  }
}
```

### products.json (Category Page)

Each entry links to a subcategory or product page:

```json
{
  "OELFLEX CLASSIC 100 300/500 V": {
    "Product": "OELFLEX CLASSIC 100 300/500 V",
    "name": "OELFLEX CLASSIC 100 300/500 V",
    "product_page_link": "https://products.lappgroup.com/.../oelflex-classic-100-300500-v.html"
  }
}
```

### metadata.json

Every folder containing downloaded files includes a metadata file:

```json
[
  {
    "name": "DB00100004EN.pdf",
    "file_path": "output/product/documentation/DB00100004EN.pdf",
    "version": null,
    "date": "2024-05-23",
    "url": "https://products.lappgroup.com/.../DB00100004EN.pdf",
    "language": null,
    "description": "Datasheet (PDF)"
  }
]
```

---

## How It Works

```
URL Input
    │
    ▼
Fetch HTML (curl-cffi with browser impersonation)
    │
    ▼
Parse with BeautifulSoup
    │
    ▼
Detect Page Type (product table? category tree?)
    │
    ├─ Product Page
    │   ├─ Extract article variant table (94 articles)
    │   ├─ Extract product description + technical data
    │   ├─ Download PDFs (datasheets, certificates)
    │   └─ Download product images
    │
    └─ Category Page
        ├─ Extract nested link tree
        └─ Generate overview markdown
    │
    ▼
Generate Output (JSON, Markdown, downloaded files)
```

### Key Implementation Details

- **SITE_CONFIG driven** - All CSS selectors are centralized in a single config dictionary, making it easy to adapt for different page structures
- **Per-article PDF extraction** - Datasheets are parsed from Bootstrap popover `data-bs-content` attributes embedded in each article table row
- **Product-level PDF** - Generated via `?type=1664268841` URL parameter (TYPO3 PDF export)
- **Image handling** - First product image is always saved as `product.jpg`; duplicates and base64 placeholders are skipped
- **Parallel downloads** - `ThreadPoolExecutor` runs documentation and image downloads concurrently (up to 4 workers), while local processing (tables, markdowns) completes first synchronously

### Error Handling

- Automatic retry with exponential backoff (3 attempts per request)
- Zyte API fallback when direct requests fail
- Graceful handling of missing HTML elements (empty fields default to `null`)
- Defensive table parsing (malformed rows are skipped)
- PDF metadata extraction with fallback (date, version from PDF properties)

---

## Sample Output

Pre-generated output is included in `sample_output/` so the tool's results can be reviewed without running it:

```
sample_output/
├── category/                # From: power-and-control-cables.html
│   ├── tables/
│   │   ├── products.json    # 600+ subcategory/product links
│   │   └── metadata.json
│   └── markdowns/
│       └── overview.md      # Full category tree as Markdown
└── product/                 # From: oelflex-classic-100-300500-v.html
    ├── documentation/       # 5 PDFs (product sheet, datasheets, certificates)
    │   ├── LAPP_PRO100890EN.pdf
    │   ├── DB00100004EN.pdf
    │   └── ...
    ├── images/
    │   ├── product.jpg      # Main product image
    │   └── *.jpeg
    ├── tables/
    │   └── products.json    # 94 article variants keyed by article number
    └── markdowns/
        └── overview.md
```

---

## Project Structure

```
products.lappgroup.com/
└── products/
    ├── crawl.py             # Main crawler script
    ├── test_crawl.py        # Unit tests (59 tests)
    ├── requirements.txt     # Python dependencies
    ├── README.md            # This file
    └── sample_output/       # Pre-generated output (category + product)
```
