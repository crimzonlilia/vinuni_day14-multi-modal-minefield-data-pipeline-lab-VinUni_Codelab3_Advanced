import ast
import re
<<<<<<< HEAD
=======
from schema import UnifiedDocument
from datetime import datetime
>>>>>>> 731cb926a70ddecec678c5e277bf1d4f64f97c1a

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract docstrings and comments from legacy Python code.

def extract_logic_from_code(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        source_code = f.read()
    # ------------------------------------------
    
<<<<<<< HEAD
    try:
        tree = ast.parse(source_code)
    except SyntaxError:
        print("  Failed to parse Python file")
        return None
    
    # Extract module docstring
    module_docstring = ast.get_docstring(tree)
    
    # Extract function docstrings and business rules
    business_rules = []
    function_docs = []
    
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            docstring = ast.get_docstring(node)
            if docstring:
                function_docs.append(f"Function '{node.name}': {docstring}")
    
    # Extract business rules from comments (regex)
    business_rule_pattern = r'#.*(?:Business Logic Rule|IMPORTANT|WARNING).*'
    rules = re.findall(business_rule_pattern, source_code, re.IGNORECASE)
    business_rules = [rule.strip('# ').strip() for rule in rules]
    
    # Build content
    content = ""
    if module_docstring:
        content += module_docstring + "\n\n"
    
    if function_docs:
        content += "Functions:\n" + "\n".join(function_docs) + "\n\n"
    
    if business_rules:
        content += "Business Rules:\n" + "\n".join(business_rules)
    
    if len(content) < 20:
        print("  No meaningful content extracted from legacy code")
        return None
    
    doc = {
        'document_id': 'code-legacy-001',
        'content': content,
        'source_type': 'Code',
        'author': 'Senior Dev (retired)',
        'source_metadata': {
            'language': 'Python',
            'num_functions': sum(1 for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)),
            'business_rules_count': len(business_rules),
            'has_module_docstring': module_docstring is not None
        }
    }
    
    print(f"  Extracted {len(business_rules)} business rules from legacy code")
    
    return doc
=======
    # Use the 'ast' module to find docstrings for functions
    try:
        tree = ast.parse(source_code)
    except SyntaxError as e:
        print(f"Error parsing code: {e}")
        return {}
    
    # Extract module-level docstring
    module_docstring = ast.get_docstring(tree) or ""
    
    # Extract function docstrings and business logic rules
    functions_logic = []
    
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            func_docstring = ast.get_docstring(node) or ""
            functions_logic.append(f"Function: {node.name}\nDocstring: {func_docstring}")
    
    # Use regex to find business rules in comments
    business_rules = []
    business_rule_pattern = r'#.*?Business Logic Rule.*'
    
    for match in re.finditer(business_rule_pattern, source_code, re.IGNORECASE):
        business_rules.append(match.group().strip())
    
    # Also look for explicit rule lines
    for line in source_code.split('\n'):
        if 'Business Logic Rule' in line or 'IMPORTANT:' in line:
            business_rules.append(line.strip())
    
    # Compile all extracted content
    all_content = f"Module: {module_docstring}\n\nFunctions:\n{chr(10).join(functions_logic)}\n\nBusiness Rules:\n{chr(10).join(business_rules)}"
    
    # Build UnifiedDocument
    doc = UnifiedDocument(
        document_id="legacy-code-001",
        content=all_content,
        source_type="Code",
        author="Senior Dev (retired)",
        timestamp=datetime.now(),
        source_metadata={
            "functions_count": len([n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]),
            "business_rules_found": len(business_rules),
            "original_file": "legacy_pipeline.py"
        }
    )
    
    return doc.model_dump()
>>>>>>> 731cb926a70ddecec678c5e277bf1d4f64f97c1a

