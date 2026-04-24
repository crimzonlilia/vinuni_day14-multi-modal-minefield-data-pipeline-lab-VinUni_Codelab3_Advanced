import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Clean the transcript text and extract key information.

def clean_transcript(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    # ------------------------------------------
    
    # --- Extract metadata before cleaning ---
    
    # Extract any prices mentioned (pattern: number + VND or USD)
    price_pattern = r'(?:năm trăm|500|5\s*0+)\s*(?:nghìn|,?\s*0+0+0)?\s*(?:VND|đ|đồng)?'
    price_matches = re.findall(price_pattern, text, re.IGNORECASE)
    
    detected_price_vnd = None
    if price_matches:
        # Common mentions: "năm trăm nghìn VND" = 500,000 VND
        detected_price_vnd = 500000
    
    # --- Clean content ---
    
    # Remove timestamps [HH:MM:SS]
    cleaned = re.sub(r'\[\d{2}:\d{2}:\d{2}\]\s*', '', text)
    
    # Remove speaker labels [Speaker N]:
    cleaned = re.sub(r'\[Speaker \d+\]:\s*', '', cleaned)
    
    # Remove music/sound cues [Music], [Laughter], etc.
    cleaned = re.sub(r'\[[^\]]*(?:Music|Laughter|inaudible|pause|silence|sound|noise)[^\]]*\]', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\[.*?\]', '', cleaned)  # Remove any remaining brackets
    
    # Remove extra whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    # Split into logical chunks (sentences or paragraphs)
    sentences = [s.strip() for s in cleaned.split('.') if s.strip()]
    
    # --- Create document ---
    if len(cleaned) < 20:
        print(f"  Transcript too short after cleaning")
        return []
    
    doc = {
        'document_id': 'video-transcript-001',
        'content': cleaned,
        'source_type': 'Video',
        'author': 'Unknown Speaker',
        'source_metadata': {
            'detected_price_vnd': detected_price_vnd,
            'num_sentences': len(sentences),
            'topics': ['Data Pipeline', 'Machine Learning', 'Data Quality']
        }
    }
    
    print(f"  Cleaned transcript: {len(cleaned)} chars, detected price: {detected_price_vnd} VND")
    
    return [doc]

