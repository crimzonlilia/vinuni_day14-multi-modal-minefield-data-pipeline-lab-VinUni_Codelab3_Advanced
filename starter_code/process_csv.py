import pandas as pd
import re
from datetime import datetime
from schema import UnifiedDocument

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Process sales records, handling type traps and duplicates.

def clean_price(price_str):
    """
    Convert various price formats to float.
    Handles: "$1200", "250000", "five dollars", "N/A", "NULL", etc.
    Returns None for invalid prices.
    """
    if pd.isna(price_str):
        return None
    
    price_str = str(price_str).strip()
    
    # Handle special cases
    if price_str.upper() in ['N/A', 'NULL', 'NAN', 'LIÊN HỆ', '']:
        return None
    
    # Remove currency symbols
    price_str = price_str.replace('$', '').strip()
    
    # Handle Vietnamese text numbers (e.g., "five dollars" -> already removed $)
    text_numbers = {
        'zero': 0, 'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
        'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10
    }
    
    for text, num in text_numbers.items():
        if price_str.lower() == text:
            return float(num)
    
    # Try to convert to float
    try:
        # Remove commas if present
        price_str = price_str.replace(',', '')
        return float(price_str)
    except ValueError:
        return None

def normalize_date(date_str):
    """
    Convert various date formats to YYYY-MM-DD string.
    Handles: "2026-01-15", "15/01/2026", "January 16th 2026", "19 Jan 2026", "2026/01/19"
    """
    if pd.isna(date_str):
        return None
    
    date_str = str(date_str).strip()
    
    # Try common date formats
    formats = [
        '%Y-%m-%d',      # 2026-01-15
        '%d/%m/%Y',      # 15/01/2026
        '%Y/%m/%d',      # 2026/01/15
        '%B %d %Y',      # January 15 2026
        '%d %b %Y',      # 15 Jan 2026
        '%d %B %Y',      # 15 January 2026
    ]
    
    # Remove ordinal suffixes (1st, 2nd, 3rd, etc.)
    date_str = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str)
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    return None

def process_sales_csv(file_path):
    # --- FILE READING (Handled for students) ---
    df = pd.read_csv(file_path)
    # ------------------------------------------
    
    # Remove duplicate rows based on 'id'
    df = df.drop_duplicates(subset=['id'], keep='first')
    
    # Clean 'price' column
    df['price'] = df['price'].apply(clean_price)
    
    # Normalize 'date_of_sale' into YYYY-MM-DD format
    df['date_of_sale'] = df['date_of_sale'].apply(normalize_date)
    
    # Filter out rows with missing critical data (price or date)
    df = df.dropna(subset=['price', 'date_of_sale'])
    
    # Also filter out rows with invalid stock_quantity (negative or NaN)
    df = df[df['stock_quantity'].notna()]
    df['stock_quantity'] = pd.to_numeric(df['stock_quantity'], errors='coerce')
    df = df.dropna(subset=['stock_quantity'])
    
    # Convert stock to int
    df['stock_quantity'] = df['stock_quantity'].astype(int)
    
    # Build UnifiedDocument list
    result = []
    for idx, row in df.iterrows():
        doc = UnifiedDocument(
            document_id=f"csv-sales-{row['id']}",
            content=f"Product: {row['product_name']}, Category: {row['category']}, Price: {row['price']} {row['currency']}, Stock: {row['stock_quantity']}",
            source_type="CSV",
            author=row.get('seller_id', 'Unknown'),
            timestamp=datetime.strptime(row['date_of_sale'], '%Y-%m-%d'),
            source_metadata={
                "product_name": row['product_name'],
                "category": row['category'],
                "price": row['price'],
                "currency": row['currency'],
                "stock_quantity": row['stock_quantity'],
                "seller_id": row['seller_id']
            }
        )
        result.append(doc.model_dump())
    
    return result

