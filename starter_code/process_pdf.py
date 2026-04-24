import os
import json
<<<<<<< HEAD
=======
from dotenv import load_dotenv
from schema import UnifiedDocument
from datetime import datetime
>>>>>>> 731cb926a70ddecec678c5e277bf1d4f64f97c1a

try:
    import google.generativeai as genai
    from dotenv import load_dotenv
    load_dotenv()
    GENAI_AVAILABLE = os.getenv("GEMINI_API_KEY") is not None
    if GENAI_AVAILABLE:
        genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
except (ImportError, Exception):
    GENAI_AVAILABLE = False

def extract_pdf_data(file_path):
    """Extract data from PDF using Gemini API and return UnifiedDocument."""
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return None
<<<<<<< HEAD
=======
        
    # Use gemini-2.0-flash or latest available model
    model = genai.GenerativeModel('gemini-2.0-flash')
>>>>>>> 731cb926a70ddecec678c5e277bf1d4f64f97c1a
    
    if not GENAI_AVAILABLE:
        print(f"  Skipping PDF processing: Gemini API not available")
        return None
    
    try:
        # Thay đổi model name để tránh lỗi 404 trên các phiên bản API cũ
        model = genai.GenerativeModel('gemini-2.5-flash')
        
<<<<<<< HEAD
        print(f"Uploading {file_path} to Gemini...")
        pdf_file = genai.upload_file(path=file_path)
        
        prompt = """
Analyze this document and extract a summary and the author. 
Output exactly as a JSON object matching this exact format:
=======
    prompt = """
Analyze this PDF document and extract the following information:
1. Title
2. Author (if available)
3. A 3-sentence summary of the main content

Provide your response as a JSON object with this exact structure:
>>>>>>> 731cb926a70ddecec678c5e277bf1d4f64f97c1a
{
    "title": "[Document Title]",
    "author": "[Author Name or 'Unknown']",
    "summary": "[3-sentence summary]"
}
"""
<<<<<<< HEAD
        
        print("Generating content from PDF using Gemini...")
        response = model.generate_content([pdf_file, prompt])
        content_text = response.text
        
        # Simple cleanup if the response is wrapped in markdown json block
        if content_text.startswith("```json"):
            content_text = content_text[7:]
        if content_text.endswith("```"):
            content_text = content_text[:-3]
        if content_text.startswith("```"):
            content_text = content_text[3:]
            
        extracted_data = json.loads(content_text.strip())
        return extracted_data
    
    except Exception as e:
        print(f"  Error processing PDF with Gemini: {e}")
        return None
=======
    
    print("Generating content from PDF using Gemini...")
    try:
        response = model.generate_content([pdf_file, prompt])
        content_text = response.text
    except Exception as e:
        print(f"Error generating content: {e}")
        return None
    
    # Clean up markdown code blocks if present
    if content_text.startswith("```json"):
        content_text = content_text[7:]
    if content_text.startswith("```"):
        content_text = content_text[3:]
    if content_text.endswith("```"):
        content_text = content_text[:-3]
    
    content_text = content_text.strip()
    
    try:
        pdf_info = json.loads(content_text)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return None
    
    # Build UnifiedDocument
    doc = UnifiedDocument(
        document_id="pdf-lecture-001",
        content=f"Title: {pdf_info.get('title', 'Unknown')}\n\nSummary: {pdf_info.get('summary', 'No summary available')}",
        source_type="PDF",
        author=pdf_info.get('author', 'Unknown'),
        timestamp=datetime.now(),
        source_metadata={
            "title": pdf_info.get('title', 'Unknown'),
            "original_file": "lecture_notes.pdf"
        }
    )
    
    return doc.model_dump()
>>>>>>> 731cb926a70ddecec678c5e277bf1d4f64f97c1a
