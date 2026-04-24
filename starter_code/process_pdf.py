import os
import json

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
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return None
    
    if not GENAI_AVAILABLE:
        print(f"  Skipping PDF processing: Gemini API not available")
        return None
    
    try:
        # Thay đổi model name để tránh lỗi 404 trên các phiên bản API cũ
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        print(f"Uploading {file_path} to Gemini...")
        pdf_file = genai.upload_file(path=file_path)
        
        prompt = """
Analyze this document and extract a summary and the author. 
Output exactly as a JSON object matching this exact format:
{
    "document_id": "pdf-doc-001",
    "content": "Summary: [Insert your 3-sentence summary here]",
    "source_type": "PDF",
    "author": "[Insert author name here]",
    "timestamp": null,
    "source_metadata": {"original_file": "lecture_notes.pdf"}
}
"""
        
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
