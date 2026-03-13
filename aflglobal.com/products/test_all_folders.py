import crawl
from bs4 import BeautifulSoup

url = "https://www.aflglobal.com/en/apac/Products/Fiber-Optic-Cleaning/Fiber-Optic-Cleaning-Fluids/FCC2-Enhanced-Fiber-Connector-Cleaner-and-Preparation-Fluid-10-oz-Can"
html = crawl.Core.fetch_html(url, "request")
soup = BeautifulSoup(html, "html.parser")
parsed_docs = crawl.Product.documentation(soup, url)
parsed_imgs = crawl.Product.images(soup, url)

print("Parsed DOCS:", parsed_docs)
print("Parsed IMGS:", parsed_imgs)

# Mock save
crawl.Core.init("output/test", {
    "page_type": "product",
    "markdowns": crawl.Product.markdown(soup, url),
    "tables": crawl.Product.tables(soup, url),
    "documentation": parsed_docs,
    "images": parsed_imgs
}, False)
