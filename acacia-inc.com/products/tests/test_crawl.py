import pytest
from bs4 import BeautifulSoup
import os
import json
from unittest.mock import MagicMock, patch
from crawl import Product, Group, Category, Core

# Sample HTML for testing extraction (Acacia-inc.com structure)
PRODUCT_HTML = """
<html>
    <body>
        <main class="content-main">
            <h1 class="product_title">AC1200 Coherent Module</h1>
            <h2>High performance solution</h2>
            <div class="show-animate">
                <h3>Key Features</h3>
                <p>Low power consumption and high density.</p>
            </div>
            <div class="figure">
                <img src="img1.jpg" data-src="https://acacia-inc.com/img1_high.jpg">
            </div>
            <div class="articles-list2">
                <a href="https://acacia-inc.com/spec.pdf">Datasheet</a>
            </div>
        </main>
    </body>
</html>
"""

@pytest.fixture
def product_soup():
    return BeautifulSoup(PRODUCT_HTML, "html.parser")

def test_product_tables_extraction(product_soup):
    # Product.tables uses hardcoded selectors for Acacia
    tables = Product.tables(product_soup, "https://acacia-inc.com/product/ac1200/")
    assert "products" in tables
    product = tables["products"][0]
    assert product["name"] == "AC1200 Coherent Module"
    assert product["Product"] == "AC1200"

def test_product_images_extraction(product_soup):
    # Product.images uses SITE_CONFIG["part"]["images"]
    images = Product.images(product_soup, "https://acacia-inc.com/product/ac1200/")
    assert "metadata" in images
    assert len(images["metadata"]) > 0
    # The sample HTML image has data-src="img1_high.jpg"
    assert images["metadata"][0]["url"].endswith("img1_high.jpg") or images["metadata"][0]["url"] == "img1_high.jpg"

def test_parallel_logic_mock():
    # Test that download_images_files uses ThreadPoolExecutor when parallel=True
    structure_data = {
        "metadata": [
            {"name": "test1.jpg", "url": "http://example.com/1.jpg"},
            {"name": "test2.jpg", "url": "http://example.com/2.jpg"}
        ]
    }
    
    with patch('concurrent.futures.ThreadPoolExecutor') as mock_executor:
        mock_executor.return_value.__enter__.return_value.map.return_value = []
        with patch('crawl.Core.get_requests') as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.iter_content.return_value = [b"fake content"]
            with patch('crawl.filetype.guess', return_value=None):
                with patch('builtins.open', MagicMock()):
                    with patch('os.path.exists', return_value=False):
                        with patch('os.makedirs', MagicMock()):
                            with patch('crawl.Core.save_metadata', MagicMock()):
                                Core.download_images_files(structure_data, parallel=True, max_workers=2)
                                
        # Check if submit or map was called
        assert mock_executor.called
