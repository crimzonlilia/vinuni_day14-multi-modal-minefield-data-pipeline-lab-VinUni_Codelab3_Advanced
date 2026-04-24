from bs4 import BeautifulSoup
from schema import UnifiedDocument
from datetime import datetime

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract product data from the HTML table, ignoring boilerplate.

def parse_html_catalog(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')
    # ------------------------------------------
    
    # Find the table with id 'main-catalog'
    table = soup.find('table', id='main-catalog')
    if not table:
        print("  No table with id 'main-catalog' found")
        return []
    
    # Extract rows (skip header)
    rows = table.find_all('tr')[1:]  # Skip header row
    
    documents = []
    for idx, row in enumerate(rows):
        try:
            cols = row.find_all('td')
            if len(cols) < 3:
                continue
            
            product_name = cols[0].get_text(strip=True)
            category = cols[1].get_text(strip=True)
            price_text = cols[2].get_text(strip=True)
            
            # Handle 'N/A' or 'Liên hệ' (Contact us)
            if price_text in ['N/A', 'Liên hệ', '', 'NULL', 'None']:
                price_text = "Not available"
            
            content = f"Product: {product_name} | Category: {category} | Price: {price_text}"
            
            if len(content) < 20:
                continue
            
            doc = {
                'document_id': f'html-product-{idx + 1}',
                'content': content,
                'source_type': 'HTML',
                'author': 'Catalog',
                'source_metadata': {
                    'product_name': product_name,
                    'category': category,
                    'price_raw': price_text
                }
            }
            
            documents.append(doc)
        
        except Exception as e:
            print(f"  Error parsing HTML row {idx}: {e}")
            continue
    
    print(f"  Extracted {len(documents)} products from HTML catalog")
    
    return documents
    # Use BeautifulSoup to find the table with id 'main-catalog'
    catalog_table = soup.find('table', {'id': 'main-catalog'})
    
    if not catalog_table:
        print("Warning: Could not find table with id 'main-catalog'")
        return []
    
    # Extract headers
    headers = []
    for th in catalog_table.find('thead').find_all('th'):
        headers.append(th.get_text(strip=True))
    
    # Extract rows
    result = []
    tbody = catalog_table.find('tbody')
    
    if not tbody:
        print("Warning: Could not find tbody in catalog table")
        return []
    
    for row_idx, tr in enumerate(tbody.find_all('tr')):
        cells = tr.find_all('td')
        if len(cells) == 0:
            continue
        
        # Extract cell data
        row_data = [cell.get_text(strip=True) for cell in cells]
        
        # Map to columns (handle 'N/A' or 'Liên hệ' in price)
        product_id = row_data[0] if len(row_data) > 0 else f"html-prod-{row_idx}"
        product_name = row_data[1] if len(row_data) > 1 else "Unknown"
        category = row_data[2] if len(row_data) > 2 else "Unknown"
        price_str = row_data[3] if len(row_data) > 3 else "N/A"
        stock_str = row_data[4] if len(row_data) > 4 else "0"
        rating = row_data[5] if len(row_data) > 5 else "N/A"
        
        # Handle price: skip if N/A or Liên hệ
        price_value = None
        if price_str not in ['N/A', 'Liên hệ', '']:
            # Try to extract numeric value from price string
            # Handle format like "28,500,000 VND" or "1,850,000 VND"
            try:
                price_numeric = price_str.replace(',', '').replace('VND', '').strip()
                price_value = float(price_numeric)
            except ValueError:
                price_value = None
        
        # Skip rows with invalid price or stock
        if price_value is None:
            continue
        
        try:
            stock_quantity = int(stock_str)
        except ValueError:
            stock_quantity = 0
        
        # Skip rows with negative stock
        if stock_quantity < 0:
            continue
        
        # Create UnifiedDocument
        doc = UnifiedDocument(
            document_id=f"html-product-{product_id}",
            content=f"Product: {product_name}, Category: {category}, Price: {price_str}, Stock: {stock_quantity}, Rating: {rating}",
            source_type="HTML",
            author="Product Catalog",
            timestamp=datetime.now(),
            source_metadata={
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "price": price_value,
                "stock_quantity": stock_quantity,
                "rating": rating
            }
        )
        result.append(doc.model_dump())
    
    return result

