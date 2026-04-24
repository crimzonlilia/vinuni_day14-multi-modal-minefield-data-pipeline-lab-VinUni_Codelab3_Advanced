import json
import time
import os

# Robust path handling
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "raw_data")


# Import role-specific modules
from schema import UnifiedDocument
from process_pdf import extract_pdf_data
from process_transcript import clean_transcript
from process_html import parse_html_catalog
from process_csv import process_sales_csv
from process_legacy_code import extract_logic_from_code
from quality_check import run_quality_gate

# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================
# Task: Orchestrate the ingestion pipeline and handle errors/SLA.

def main():
    start_time = time.time()
    final_kb = []
    
    # --- FILE PATH SETUP (Handled for students) ---
    pdf_path = os.path.join(RAW_DATA_DIR, "lecture_notes.pdf")
    trans_path = os.path.join(RAW_DATA_DIR, "demo_transcript.txt")
    html_path = os.path.join(RAW_DATA_DIR, "product_catalog.html")
    csv_path = os.path.join(RAW_DATA_DIR, "sales_records.csv")
    code_path = os.path.join(RAW_DATA_DIR, "legacy_pipeline.py")
    
    output_path = os.path.join(os.path.dirname(SCRIPT_DIR), "processed_knowledge_base.json")
    # ----------------------------------------------

    print("[ORCHESTRATOR] Starting pipeline...")
    
    # Process 1: PDF (if exists)
    if os.path.exists(pdf_path):
        print("\n[STEP 1] Processing PDF...")
        try:
            doc = extract_pdf_data(pdf_path)
            if doc and run_quality_gate(doc):
                final_kb.append(doc)
                print(f"  [PASS] PDF added to KB")
            else:
                print(f"  [SKIP] PDF rejected by quality gate")
        except Exception as e:
            print(f"  [ERROR] Error processing PDF: {e}")
    
    # Process 2: Transcript
    if os.path.exists(trans_path):
        print("\n[STEP 2] Processing Transcript...")
        try:
            docs = clean_transcript(trans_path)
            if docs:
                for doc in docs:
                    if run_quality_gate(doc):
                        final_kb.append(doc)
                        print(f"  [PASS] Transcript doc '{doc.get('document_id')}' added to KB")
                    else:
                        print(f"  [SKIP] Transcript doc rejected by quality gate")
        except Exception as e:
            print(f"  [ERROR] Error processing transcript: {e}")
    
    # Process 3: HTML
    if os.path.exists(html_path):
        print("\n[STEP 3] Processing HTML...")
        try:
            docs = parse_html_catalog(html_path)
            if docs:
                for doc in docs:
                    if run_quality_gate(doc):
                        final_kb.append(doc)
                        print(f"  [PASS] HTML doc '{doc.get('document_id')}' added to KB")
                    else:
                        print(f"  [SKIP] HTML doc rejected by quality gate")
        except Exception as e:
            print(f"  [ERROR] Error processing HTML: {e}")
    
    # Process 4: CSV
    if os.path.exists(csv_path):
        print("\n[STEP 4] Processing CSV...")
        try:
            docs = process_sales_csv(csv_path)
            if docs:
                for doc in docs:
                    if run_quality_gate(doc):
                        final_kb.append(doc)
                        print(f"  [PASS] CSV doc '{doc.get('document_id')}' added to KB")
                    else:
                        print(f"  [SKIP] CSV doc rejected by quality gate")
        except Exception as e:
            print(f"  [ERROR] Error processing CSV: {e}")
    
    # Process 5: Legacy Code
    if os.path.exists(code_path):
        print("\n[STEP 5] Processing Legacy Code...")
        try:
            doc = extract_logic_from_code(code_path)
            if doc and run_quality_gate(doc):
                final_kb.append(doc)
                print(f"  [PASS] Legacy code doc added to KB")
            else:
                print(f"  [SKIP] Legacy code doc rejected by quality gate")
        except Exception as e:
            print(f"  [ERROR] Error processing legacy code: {e}")
    
    # --- SAVE RESULTS ---
    print(f"\n[ORCHESTRATOR] Saving {len(final_kb)} documents to {output_path}...")
    with open(output_path, 'w', encoding='utf-8') as f:
        # Convert UnifiedDocument objects to dicts if needed
        kb_data = []
        for doc in final_kb:
            if isinstance(doc, UnifiedDocument):
                kb_data.append(doc.dict())
            elif isinstance(doc, dict):
                kb_data.append(doc)
            else:
                kb_data.append(doc)
        json.dump(kb_data, f, ensure_ascii=False, indent=2)
    
<<<<<<< HEAD
=======
    # Example:
    # doc = extract_pdf_data(pdf_path)
    # if doc and run_quality_gate(doc):
    #     final_kb.append(doc)

    for file_path, processor in [
        (pdf_path, extract_pdf_data),
        (trans_path, clean_transcript),
        (html_path, parse_html_catalog),
        (csv_path, process_sales_csv),
        (code_path, extract_logic_from_code)
    ]:
        print(f"Processing {file_path} with {processor.__name__}...")
        try:
            doc = processor(file_path)
            if doc and run_quality_gate(doc):
                final_kb.append(doc)
            else:
                print(f"Document from {file_path} failed quality gate.")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

>>>>>>> 731cb926a70ddecec678c5e277bf1d4f64f97c1a
    end_time = time.time()
    print(f"\n[ORCHESTRATOR] Pipeline finished in {end_time - start_time:.2f} seconds.")
    print(f"[ORCHESTRATOR] Total valid documents stored: {len(final_kb)}")


if __name__ == "__main__":
    main()
