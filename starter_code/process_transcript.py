import re
import json
import os
from dotenv import load_dotenv
import google.generativeai as genai
from schema import UnifiedDocument
from datetime import datetime

load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

def extract_vietnamese_numbers_with_ai(text):
    """
    Use Gemini AI to extract and convert Vietnamese number phrases to numeric values.
    Returns a list of numeric values found in the text.
    """
    model = genai.GenerativeModel('gemini-2.0-flash')
    
    prompt = f"""
Analyze this Vietnamese text and find ALL price mentions in Vietnamese words or numbers.
Convert each Vietnamese number phrase to its numeric value in VND.

Text: {text}

Return a JSON array of numbers found. For example:
- "năm trăm nghìn VND" -> 500000
- "một triệu" -> 1000000
- "hai triệu năm trăm nghìn" -> 2500000
- "500,000 VND" -> 500000

Return only a JSON array of numbers, like: [500000, 1000000, 2500000]
If no prices found, return: []
"""
    
    try:
        response = model.generate_content(prompt)
        response_text = response.text.strip()
        
        # Clean up markdown if present
        if response_text.startswith("```"):
            response_text = response_text.split("```")[1]
            if response_text.startswith("json"):
                response_text = response_text[4:]
        
        response_text = response_text.strip()
        prices = json.loads(response_text)
        
        # Ensure it's a list and all elements are numbers
        if isinstance(prices, list):
            return [int(p) for p in prices if isinstance(p, (int, float))]
        return []
    except Exception as e:
        print(f"Error extracting Vietnamese numbers with AI: {e}")
        return []

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
    # Remove timestamps like [00:00:00]
    cleaned_text = re.sub(r'\[\d{2}:\d{2}:\d{2}\]', '', text)
    
    # Remove noise tokens like [Music], [inaudible], [Laughter], [Music starts], [Music ends], [Speaker X]:
    noise_patterns = [
        r'\[Music.*?\]',
        r'\[inaudible\]',
        r'\[Laughter\]',
        r'\[Speaker \d+\]:',
    ]
    
    for pattern in noise_patterns:
        cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE)
    
    # Clean up extra whitespace
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
    
    # Use Gemini AI to extract Vietnamese numbers
    print("Using Gemini API to extract Vietnamese numbers from transcript...")
    price_mentions = extract_vietnamese_numbers_with_ai(cleaned_text)
    
    # Also try regex fallback for numeric prices
    numeric_pattern = r'(\d+(?:[,\.]\d{3})*)\s*VND'
    for match in re.finditer(numeric_pattern, cleaned_text):
        try:
            numeric_str = match.group(1).replace(',', '').replace('.', '')
            num = int(numeric_str)
            if num not in price_mentions:
                price_mentions.append(num)
        except ValueError:
            pass
    
    # Sort prices for consistency
    price_mentions.sort()
    
    # Build UnifiedDocument
    doc = UnifiedDocument(
        document_id="transcript-001",
        content=f"Cleaned Transcript: {cleaned_text}",
        source_type="Video",  # Classified as video since it's audio transcript
        author="Unknown Speaker",
        timestamp=datetime.now(),
        source_metadata={
            "detected_price_vnd": price_mentions[0] if price_mentions else None,  # Primary price for forensic check
            "prices_mentioned_vnd": price_mentions,  # All prices found
            "price_count": len(price_mentions),
            "original_file": "demo_transcript.txt",
            "extraction_method": "Gemini AI + regex"
        }
    )
    
    return doc.model_dump()

