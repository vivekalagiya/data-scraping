# SongchuanUSA Web Scraper

This project is a Python-based web scraper to extract product data, PDFs, images, and metadata from songchuanusa.com.

---

## 🚀 Features

- Detects page type (Category / Product)
- Extracts:
  - Product details (name, description, specs, features)
  - Images
  - PDF datasheets (including Google Drive links)
- Saves:
  - JSON (products, metadata)
  - Markdown (overview)
- Downloads files

---

## 📁 Project Structure

data-scraping/
│
├── songchuanusa.com/
│   └── products/
│       ├── crawl.py
│       └── output/
│
└── venv/

---

## ⚙️ Setup Instructions

### 1. Create Virtual Environment

python3 -m venv venv
source venv/bin/activate   # Mac/Linux

---

### 2. Install Dependencies

pip install beautifulsoup4 markdownify curl_cffi filetype PyPDF2

---

## ▶️ How to Run

### 🔹 Category Page

python3 crawl.py --url "https://songchuanusa.com/industrial-automation-relays/" --out output/category

---

### 🔹 Product Page

python3 crawl.py --url "https://songchuanusa.com/all-relays/sclb-scld-5a-general-purpose-power-relay/" --out output/parts

---

## 📦 Output

output/
├── parts/
│   ├── tables/products.json
│   ├── markdowns/overview.md
│   ├── documentation/metadata.json
│   ├── images/metadata.json

---

## ⚠️ Common Issues

1. Module not found:
pip install beautifulsoup4

2. metadata.json empty:
- Fixed by handling:
  - .pdf links
  - Google Drive links
  - Datasheet text links

3. Import error (common module):
Remove:
from common.products.crawl import Core

---

## 👨‍💻 Author

Siddharth Dave

