# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================
# Task: Implement quality gates to reject corrupt data or logic discrepancies.

import re
from typing import Dict, Tuple, List

class QualityGateResult:
    """Kết quả chi tiết từ quality gate checks."""
    def __init__(self, passed: bool, reason: str = "", details: Dict = None):
        self.passed = passed
        self.reason = reason
        self.details = details or {}


def _check_content_length(content: str, min_length: int = 20) -> QualityGateResult:
    """
    GATE 1: Kiểm tra độ dài tối thiểu
    Mục tiêu: Loại bỏ các snippet quá ngắn, không đủ ngữ cảnh tri thức.
    """
    if len(content) < min_length:
        return QualityGateResult(
            passed=False,
            reason=f"Content too short ({len(content)} chars, minimum: {min_length})",
            details={"actual_length": len(content), "minimum_length": min_length}
        )
    return QualityGateResult(passed=True)


def _check_toxic_patterns(content: str) -> QualityGateResult:
    """
    GATE 2: Loại bỏ dữ liệu "độc hại" hoặc lỗi hệ thống
    Mục tiêu: Ngăn chặn log lỗi, stack trace, hoặc thông báo kỹ thuật lọt vào Knowledge Base.
    """
    toxic_error_patterns = [
        r'null pointer exception',
        r'stack trace',
        r'access denied',
        r'segmentation fault',
        r'404 not found',
        r'undefined\s+is\s+not',
        r'error:\s*',
        r'exception\s+at\s+',
        r'failed to\s+',
    ]
    
    content_lower = content.lower()
    for pattern in toxic_error_patterns:
        if re.search(pattern, content_lower):
            return QualityGateResult(
                passed=False,
                reason=f"Contains suspicious error/log pattern: {pattern}",
                details={"pattern_matched": pattern}
            )
    return QualityGateResult(passed=True)


def _check_logic_discrepancies(content: str) -> QualityGateResult:
    """
    GATE 3: Kiểm tra sai lệch logic (Logic Discrepancies)
    Mục tiêu: Phát hiện khi tài liệu nói một đằng, code thực hiện một nẻo.
    Ví dụ: Comment ghi thuế 8% nhưng code set giá trị 10% (0.1)
    """
    # Tìm giá trị phần trăm trong comment (ví dụ: "tax 8%")
    comment_match = re.search(r'#.*tax.*?(\d+(?:\.\d+)?)\s*%', content, re.IGNORECASE)
    # Tìm giá trị gán trong code (ví dụ: "tax_rate = 0.1")
    code_match = re.search(r'tax_rate\s*=\s*(0\.\d+|\d+(?:\.\d+)?)', content, re.IGNORECASE)

    if comment_match and code_match:
        try:
            comment_value = float(comment_match.group(1))
            code_value = float(code_match.group(1))
            
            # Nếu code_value < 1, giả định nó là decimal (e.g., 0.08 = 8%)
            code_percent = code_value * 100 if code_value < 1 else code_value
            
            tolerance = 0.01  # 0.01% tolerance
            if abs(comment_value - code_percent) > tolerance:
                return QualityGateResult(
                    passed=False,
                    reason=f"Logic Discrepancy detected: Comment says {comment_value}%, but code says {code_percent}%",
                    details={
                        "comment_value": comment_value,
                        "code_value": code_percent,
                        "difference": abs(comment_value - code_percent)
                    }
                )
        except (ValueError, IndexError):
            pass  # Không lỗi, chỉ không match được
    
    return QualityGateResult(passed=True)


def _check_schema_compliance(document_dict: Dict) -> QualityGateResult:
    """
    GATE 4: Kiểm tra schema compliance
    Mục tiêu: Đảm bảo tài liệu có các field bắt buộc từ UnifiedDocument.
    """
    required_fields = ['document_id', 'content', 'source_type']
    missing_fields = [field for field in required_fields if field not in document_dict]
    
    if missing_fields:
        return QualityGateResult(
            passed=False,
            reason=f"Missing required schema fields: {missing_fields}",
            details={"missing_fields": missing_fields}
        )
    
    return QualityGateResult(passed=True)


def _check_content_encoding(content: str) -> QualityGateResult:
    """
    GATE 5: Kiểm tra encoding issues
    Mục tiêu: Phát hiện ký tự lạ, control characters, hoặc dữ liệu bị hỏng.
    """
    # Kiểm tra null bytes
    if '\x00' in content:
        return QualityGateResult(
            passed=False,
            reason="Content contains null bytes (corruption detected)",
            details={"issue": "null_bytes"}
        )
    
    # Kiểm tra quá nhiều control characters
    control_char_count = sum(1 for c in content if ord(c) < 32 and c not in '\n\r\t')
    if control_char_count > len(content) * 0.05:  # > 5% control chars
        return QualityGateResult(
            passed=False,
            reason=f"Content has too many control characters ({control_char_count})",
            details={"control_char_count": control_char_count}
        )
    
    return QualityGateResult(passed=True)


def _check_content_uniqueness(content: str, document_id: str) -> QualityGateResult:
    """
    GATE 6: Kiểm tra repeated boilerplate
    Mục tiêu: Phát hiện nội dung giống nhau quá nhiều lần (copy-paste error).
    """
    # Nếu một chuỗi 10+ ký tự lặp lại quá 3 lần, đó là boilerplate
    for match in re.finditer(r'(.{10,})', content):
        substr = match.group(1)
        if content.count(substr) > 3:
            return QualityGateResult(
                passed=False,
                reason=f"Content appears to be mostly repeated boilerplate",
                details={"repeated_substring": substr[:50]}
            )
    
    return QualityGateResult(passed=True)


def run_quality_gate(document_dict: Dict) -> bool:
    """
    Hàm kiểm soát chất lượng dữ liệu đầu vào.
    
    Args:
        document_dict: Dictionary với keys 'document_id', 'content', 'source_type', v.v.
    
    Returns:
        bool: True nếu tài liệu đạt chuẩn, False nếu bị loại bỏ
    """
    
    rejection_reasons = []
    
    # Gate 1: Schema compliance (kiểm tra trước tiên)
    result = _check_schema_compliance(document_dict)
    if not result.passed:
        print(f"[QA GATE 1] FAIL - {result.reason}")
        return False
    
    content = document_dict.get('content', '')
    
    # Gate 2: Content length
    result = _check_content_length(content)
    if not result.passed:
        print(f"[QA GATE 2] WARN - {result.reason}")
        rejection_reasons.append(result.reason)
    
    # Gate 3: Toxic patterns (CRITICAL)
    result = _check_toxic_patterns(content)
    if not result.passed:
        print(f"[QA GATE 3] WARN - {result.reason}")
        rejection_reasons.append(result.reason)
    
    # Gate 4: Logic discrepancies
    result = _check_logic_discrepancies(content)
    if not result.passed:
        print(f"[QA GATE 4] WARN - {result.reason}")
        rejection_reasons.append(result.reason)
    
    # Gate 5: Encoding issues
    result = _check_content_encoding(content)
    if not result.passed:
        print(f"[QA GATE 5] WARN - {result.reason}")
        rejection_reasons.append(result.reason)
    
    # Gate 6: Uniqueness check
    result = _check_content_uniqueness(content, document_dict.get('document_id', 'unknown'))
    if not result.passed:
        print(f"[QA GATE 6] WARN - {result.reason}")
        rejection_reasons.append(result.reason)
    
    # Nếu có bất kỳ rejection reason nào, loại bỏ tài liệu
    if rejection_reasons:
        print(f"[QA REJECT] Document {document_dict.get('document_id')} rejected!")
        return False
    
    print(f"[QA PASS] Document {document_dict.get('document_id')} passed all quality gates!")
    return True
