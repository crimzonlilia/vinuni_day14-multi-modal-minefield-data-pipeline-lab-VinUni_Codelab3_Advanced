from bs4 import BeautifulSoup

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

