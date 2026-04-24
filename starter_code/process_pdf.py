import google.generativeai as genai
import os
import json
from dotenv import load_dotenv
from schema import UnifiedDocument
from datetime import datetime

load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

def extract_pdf_data(file_path):
    """Extract data from PDF using Gemini API and return UnifiedDocument."""
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return None
        
    # Use gemini-2.0-flash or latest available model
    model = genai.GenerativeModel('gemini-2.0-flash')
    
    print(f"Uploading {file_path} to Gemini...")
    try:
        pdf_file = genai.upload_file(path=file_path)
    except Exception as e:
        print(f"Failed to upload file to Gemini: {e}")
        return None
        
    prompt = """
Analyze this PDF document and extract the following information:
1. Title
2. Author (if available)
3. A 3-sentence summary of the main content

Provide your response as a JSON object with this exact structure:
{
    "title": "[Document Title]",
    "author": "[Author Name or 'Unknown']",
    "summary": "[3-sentence summary]"
}
"""
    
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
