# Web Data Extraction Tool — eecoswitch.com

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
python crawl.py --url "http://www.eecoswitch.com/product/electromechanical-switches/" --out output/category
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
python crawl.py --url "http://www.eecoswitch.com/product/2300-series/" --out output/part
```

**Output structure:**
```
output/part/
├── documentation/
│   ├── *.pdf              ← Downloaded datasheets
│   └── metadata.json
├── images/
│   ├── product.*          ← Main product image (first image)
│   ├── *.gif / *.png      ← Additional images
│   └── metadata.json
├── block_diagrams/
├── design_resources/
├── software_tools/
├── trainings/
├── other/
├── tables/
│   ├── products.json      ← Product details (SKU, pdf_link, image_url, etc.)
│   └── metadata.json
└── markdowns/
    └── overview.md        ← Product description in Markdown
```

## Page Type Detection

The script automatically detects the page type by inspecting the HTML structure:
- **Product Page** — detected when the page contains a `<a href="*.pdf">` link
- **Category/Group Page** — detected when the page has a `<body>` tag but no PDF links

Detection is based purely on HTML content, not URL patterns.

## Additional Options

```bash
python crawl.py --url <URL> --out <OUTPUT_DIRECTORY> --update-only-prices
```

Updates only the pricing data without re-downloading images and documents.