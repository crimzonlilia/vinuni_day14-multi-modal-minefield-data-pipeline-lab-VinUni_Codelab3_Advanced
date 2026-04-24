import pandas as pd
import re
<<<<<<< HEAD
=======
from datetime import datetime
from schema import UnifiedDocument
>>>>>>> 731cb926a70ddecec678c5e277bf1d4f64f97c1a

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Process sales records, handling type traps and duplicates.

<<<<<<< HEAD
def _clean_price(price_str):
    """Convert various price formats to float."""
    if not price_str or price_str in ['N/A', 'Liên hệ', 'NULL', 'None']:
        return None
    
    price_str = str(price_str).strip()
    
    # Remove currency symbols
    price_str = price_str.replace('$', '').strip()
    
    # Handle "five dollars" -> convert to number
    text_numbers = {
        'five': '5', 'one': '1', 'two': '2', 'three': '3', 'four': '4',
        'six': '6', 'seven': '7', 'eight': '8', 'nine': '9', 'zero': '0'
    }
    
    for text, num in text_numbers.items():
        price_str = price_str.lower().replace(text, num)
    
    # Extract numbers and decimals
    match = re.search(r'-?\d+(?:\.\d+)?', price_str)
    if match:
        try:
            return float(match.group())
        except ValueError:
            return None
    
    return None


def _normalize_date(date_str):
    """Normalize various date formats to YYYY-MM-DD."""
    if not date_str or date_str in ['N/A', 'Liên hệ', 'NULL']:
        return None
    
    date_str = str(date_str).strip()
    
    # Already normalized
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str
    
    # Try different formats
    date_patterns = [
        (r'(\d{1,2})/(\d{1,2})/(\d{4})', lambda m: f"{m.group(3)}-{m.group(2):0>2}-{m.group(1):0>2}"),
        (r'(\d{1,2})-(\d{1,2})-(\d{4})', lambda m: f"{m.group(3)}-{m.group(2):0>2}-{m.group(1):0>2}"),
        (r'(\d{4})/(\d{1,2})/(\d{1,2})', lambda m: f"{m.group(1)}-{m.group(2):0>2}-{m.group(3):0>2}"),
        (r'(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})(?:st|nd|rd|th)?\s+(\d{4})', 
         lambda m: f"{m.group(2)}-01-{m.group(1):0>2}"),  # Simplified - would need full month mapping
    ]
    
    for pattern, formatter in date_patterns:
        match = re.search(pattern, date_str, re.IGNORECASE)
        if match:
            try:
                return formatter(match)
            except:
                pass
    
    # Fallback: try parsing common formats
    return None


def process_sales_csv(file_path):
    """
    Process sales records CSV, handling:
    - Duplicate rows (remove)
    - Type traps (various price formats, date formats)
    - Missing/Invalid values
    
    Returns a list of dictionaries for the UnifiedDocument schema.
    """
    
    df = pd.read_csv(file_path)
    
    # --- Remove duplicates based on 'id' ---
    # Keep only first occurrence of each id
    df = df.drop_duplicates(subset=['id'], keep='first')
    
    # --- Clean price column ---
    df['price_cleaned'] = df['price'].apply(_clean_price)
    
    # --- Normalize date column ---
    df['date_normalized'] = df['date_of_sale'].apply(_normalize_date)
    
    # --- Create UnifiedDocument entries ---
    documents = []
    
    for idx, row in df.iterrows():
        try:
            doc_id = f"csv-sales-{row['id']}"
            
            # Build content from record
            content = f"Product: {row['product_name']} | Category: {row['category']} | "
            if row['price_cleaned'] is not None:
                content += f"Price: {row['price_cleaned']} {row['currency']} | "
            else:
                content += f"Price: Not available | "
            
            if row['date_normalized']:
                content += f"Date: {row['date_normalized']}"
            
            if len(content) < 20:
                print(f"  Skipping record {row['id']}: content too short")
                continue
            
            doc = {
                'document_id': doc_id,
                'content': content,
                'source_type': 'CSV',
                'author': row['seller_id'],
                'source_metadata': {
                    'product_name': row['product_name'],
                    'category': row['category'],
                    'price_original': str(row['price']),
                    'price_cleaned': row['price_cleaned'],
                    'currency': row['currency'],
                    'date_original': str(row['date_of_sale']),
                    'date_normalized': row['date_normalized'],
                    'stock_quantity': row['stock_quantity'] if pd.notna(row['stock_quantity']) else None
                }
            }
            
            documents.append(doc)
        
        except Exception as e:
            print(f"  Error processing row {idx}: {e}")
            continue
    
    print(f"  Processed {len(documents)} valid CSV records (removed {len(df) - len(documents)} duplicates/invalid)")
    
    return documents
=======
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
>>>>>>> 731cb926a70ddecec678c5e277bf1d4f64f97c1a

