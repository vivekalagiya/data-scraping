# Acacia Inc Web Scraper

A Python-based web data extraction tool that scrapes product data from [acacia-inc.com](https://acacia-inc.com) and generates structured output data with organized file assets.

## Setup

```bash
cd acacia-inc.com/products
pip install -r requirements.txt
```

### Requirements
- Python 3.8+
- `beautifulsoup4` – HTML parsing
- `curl-cffi` – HTTP requests with browser impersonation
- `markdownify` – HTML to Markdown conversion
- `filetype` – File type detection
- `PyPDF2` – PDF metadata extraction
- `pytest` – Testing framework
- `pytest-mock` – Mocking for tests

## Usage

```bash
# Scrape a category page (product listing)
python3 crawl.py --url "https://acacia-inc.com/product-category/performance-optimized-modules/" --out output/category

# Scrape a product detail page
python3 crawl.py --url "https://acacia-inc.com/product/ac1200/" --out output/part
```

### Arguments
| Argument | Required | Description |
|----------|----------|-------------|
| `--url`  | Yes      | URL of the page to scrape (category or product) |
| `--out`  | Yes      | Output directory path |
| `--update-only-prices` | No | Only update pricing data |
| `--parallel` | No | Enable parallel processing for downloads |
| `--max-workers` | No | Maximum number of concurrent workers (default: 5) |

## Output Structure

### Category Page Output
```
output/category/
├── markdowns/
│   └── overview.md          # Converted page content
└── tables/
    ├── products.json        # Product listings keyed by product name
    └── metadata.json        # Table metadata
```

### Product Page Output
```
output/part/
├── documentation/           # Downloaded PDFs
│   └── metadata.json
├── images/                  # Downloaded product images
│   ├── product.png          # First image (renamed)
│   ├── *.jpg / *.png        # Additional images
│   └── metadata.json
├── block_diagrams/
│   └── metadata.json
├── design_resources/
│   └── metadata.json
├── software_tools/
│   └── metadata.json
├── tables/
│   ├── products.json        # Product data keyed by SKU
│   └── metadata.json
├── markdowns/
│   └── overview.md          # Full page content as Markdown
├── trainings/
│   └── metadata.json
└── other/
    └── metadata.json
```

### products.json Format
```json
{
  "AC1200": {
    "name": "AC1200 Product Family",
    "Product": "AC1200",
    "Description": ["Improve efficiency while reducing network costs", "..."],
    "Features": ["3D Shaping: ...", "Adaptive Baud Rate: ..."],
    "image_url": "https://acacia-inc.com/wp-content/uploads/.../ac1200-product-family.png",
    "product_page_link": "https://acacia-inc.com/product/ac1200/",
    "Pricing": null,
    "Related Resources": [{"name": "...", "url": "..."}],
    "pdf_link": null,
    "pdf_filename": null
  }
}

### metadata.json Format
```json
[
 {
   "name": "datasheet.pdf",
   "file_path": "path/to/file.pdf",
   "version": "1.0.0",
   "date": "2021-01-01",
   "url": "https://example.com/file.pdf",
   "language": "english",
   "description": null
 }
]
```

```

## How It Works

1. **Page Detection** – Determines page type (category/group vs product) using CSS selectors
2. **Data Extraction** – Parses HTML using BeautifulSoup with site-specific selectors
3. **File Downloads** – Downloads images and PDFs with retry logic and type detection
4. **Output Generation** – Creates structured JSON, Markdown, and metadata files

### Page Type Detection
- **Group** (category page): Detected by `.split-products` selector
- **Product** (detail page): Detected by `main.content-main` selector

### Error Handling
- Automatic retry with exponential backoff (3 attempts)
- Zyte API fallback for bot-protected pages
- Graceful handling of missing elements and empty fields
- Clean logging with timestamps and status indicators

## Testing

Run unit tests using:
```bash
python -m pytest tests/test_crawl.py
```
The tests verify:
- Data extraction logic for products.
- Image extraction from lazy-loaded attributes.
- Parallel processing behavior using mocks.
