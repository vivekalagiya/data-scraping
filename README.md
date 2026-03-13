Web Crawler Assignment

Author: Jay Alpeshbhai Chavda

Features:
- Product crawler
- Category crawler
- Downloads 3 images
- Generates markdown documentation

Python Web Data Extraction Tool

Install dependencies:

pip install -r requirements.txt

Run Category Page:

python crawl.py --url https://www.chogori-tech.com/product/details.aspx?lcid=32 --out output/category

Run Product Page:

python crawl.py --url https://www.chogori-tech.com/product/info.aspx?itemid=487&lcid=65 --out output/part