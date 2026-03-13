# Web Data Extraction Tool — aflglobal.com

## Setup

```bash
pip install -r requirements.txt
```

## Usage

```bash
python crawl.py --url <URL> --out <OUTPUT_DIRECTORY>
```

### Category / Sub-category Page

```bash
python crawl.py --url "https://www.aflglobal.com/en/apac/Products/Fiber-Optic-Connectivity/Preterminated-Cable-Assemblies" --out output/category
```

**Output structure:**
```
output/category/
├── tables/
│   ├── products.json      ← All sub-products with links
│   └── metadata.json
└── markdowns/
    └── overview.md        ← Full category page in Markdown
```

### Product Page

```bash
python crawl.py --url "https://www.aflglobal.com/en/apac/Products/Fiber-Optic-Cleaning/Fiber-Optic-Cleaning-Fluids/FCC2-Enhanced-Fiber-Connector-Cleaner-and-Preparation-Fluid-10-oz-Can" --out output/part
```

**Output structure:**
```
output/part/
├── documentation/
│   ├── *.pdf / CDN files  ← Downloaded spec sheets
│   └── metadata.json
├── images/
│   ├── product.*          ← Main product image
│   └── metadata.json
├── tables/
│   ├── products.json      ← Product details (name, features, description, pdf_link, image_url, etc.)
│   └── metadata.json
└── markdowns/
    └── overview.md        ← Product description in Markdown
```

## Page Type Detection

The script automatically detects the page type by inspecting the HTML:
- **Product Page** — detected when the page has an `<h1>` AND a spec-sheet link
  (url containing `stylelabs` CDN or ending in `.pdf`)
- **Group / Category Page** — detected when it has anchor links pointing deeper into
  the `/Products/` URL hierarchy (≥ 4 path segments)
- **Category Overview** — fallback for top-level listing pages with no product cards

Detection is based on HTML content, not URL patterns.

## Additional Options

```bash
python crawl.py --url <URL> --out <OUTPUT_DIRECTORY> --update-only-prices
```

Updates only the pricing data without re-downloading images and documents.

## Notes

- `verify_ssl` is set to `False` in `CONFIG` to handle AFL Global's SSL certificate.
- The `SITE_CONFIG` dict at the top of `crawl.py` contains all CSS selectors.
  If AFL Global changes their HTML structure, only that dict needs updating.
- AFL Global product pages use named anchor sections (`#afl-feature-section`,
  `#afl-description-section`) — these are parsed directly for structured data.
- Spec-sheet downloads come from the Stylelabs CDN (`afl-delivery.stylelabs.cloud`),
  not `.pdf` URLs — they are handled as documents in the `documentation/` folder.
