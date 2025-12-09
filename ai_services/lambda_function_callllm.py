import json
import os
import base64
import logging
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any

import boto3
from botocore.exceptions import ClientError

# ----------------------------------------------------------------------------- 
# 1. Logging & Config
# ----------------------------------------------------------------------------- 

logger = logging.getLogger()
logger.setLevel(logging.INFO)

bedrock = boto3.client("bedrock-runtime")
lambda_client = boto3.client("lambda")

# ENV: tên Lambda RAG-search, ví dụ 'rag_search'
RAG_FUNCTION_NAME = os.getenv("RAG_FUNCTION_NAME")

class Config:
    MODEL_ID: str = os.getenv(
        "MODEL_ID",
        "anthropic.claude-3-haiku-20240307-v1:0"
    )
    ALLOWED_FORMATS = {"pdf", "doc", "docx", "txt", "md", "html"}
    MAX_FILE_SIZE_BYTES: int = int(10 * 1024 * 1024)  # ~10MB


SYSTEM_PROMPT = """
Bạn là một trợ lý pháp lý tự động, hỗ trợ người dùng phổ thông phân tích rủi ro trong hợp đồng tiếng Việt.

NGUYÊN TẮC:
- Chỉ phân tích dựa trên nội dung hợp đồng được cung cấp.
- Luôn nhấn mạnh rằng kết quả chỉ mang tính chất tham khảo, không thay thế luật sư.
- Nếu không chắc về điều luật cụ thể, hãy để trống các trường liên quan đến điều/khoản, KHÔNG tự bịa số điều luật.
- Ưu tiên ngôn ngữ dễ hiểu, súc tích, dùng tiếng Việt.

NHIỆM VỤ:
- Tóm tắt ngắn gọn nội dung chính của hợp đồng.
- Xác định các rủi ro chính (nếu có), phân loại rủi ro và đánh giá mức độ nghiêm trọng.
- Đề xuất cách chỉnh sửa/bo sung điều khoản để giảm rủi ro.
- Nếu có thể, chỉ ra các căn cứ pháp luật liên quan (tên luật, điều, khoản); nếu không chắc, hãy để trống.

YÊU CẦU QUAN TRỌNG VỀ OUTPUT:
- CHỈ trả về một JSON hợp lệ (valid JSON), không chứa bất kỳ giải thích, bình luận hay text nào ở bên ngoài.
- KHÔNG dùng markdown, KHÔNG dùng ```json.
- JSON phải có cấu trúc CHÍNH XÁC như sau:

{
  "summary": "string",
  "overall_risk_level": "LOW | MEDIUM | HIGH | CRITICAL",
  "risk_items": [
    {
      "id": "string, ví dụ R1, R2,...",
      "title": "string, tiêu đề ngắn cho rủi ro",
      "clause_excerpt": "string, trích đoạn điều khoản liên quan (nếu xác định được, nếu không thì chuỗi rỗng)",
      "risk_types": [
        "LegalCompliance | Financial | FraudScam | UnclearTerm | ImbalancedObligation"
      ],
      "severity": "LOW | MEDIUM | HIGH | CRITICAL",
      "description": "string, mô tả chi tiết rủi ro bằng tiếng Việt",
      "recommendation": "string, gợi ý chỉnh sửa cụ thể bằng tiếng Việt",
      "law_references": [
        {
          "law_name": "string, có thể để chuỗi rỗng nếu không biết chắc",
          "article": "string, có thể để chuỗi rỗng nếu không biết chắc",
          "clause": "string, có thể để chuỗi rỗng nếu không biết chắc",
          "note": "string, ghi chú thêm nếu cần, có thể để chuỗi rỗng"
        }
      ]
    }
  ],
  "disclaimer": "string, lời cảnh báo rằng đây không phải tư vấn pháp lý chính thức"
}

- Nếu không tìm thấy rủi ro nào đáng kể, hãy để risk_items là một mảng rỗng [] và overall_risk_level là "LOW".
- Đảm bảo JSON trả về là hợp lệ, không có dấu phẩy thừa, không có comment.
""".strip()


# ----------------------------------------------------------------------------- 
# 2. Data structures
# ----------------------------------------------------------------------------- 

@dataclass
class ContractInput:
    language: str
    contract_text: Optional[str] = None
    file_name: Optional[str] = None
    file_format: Optional[str] = None
    file_bytes: Optional[bytes] = None
    # context RAG do BE gửi lên (hoặc để None)
    rag_context: Optional[str] = None

    @property
    def has_file(self) -> bool:
        return self.file_bytes is not None and self.file_format is not None

    @property
    def has_text(self) -> bool:
        return self.contract_text is not None and self.contract_text.strip() != ""


# ----------------------------------------------------------------------------- 
# 3. HTTP event parsing
# ----------------------------------------------------------------------------- 

def parse_event_body(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Hỗ trợ cả trường hợp gọi qua API Gateway HTTP API (body là string)
    và invoke trực tiếp (event là dict).
    """
    if "body" not in event:
        # invoke trực tiếp
        return event

    body = event["body"]
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")

    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        logger.warning("Body is not valid JSON")
        raise ValueError("Request body must be valid JSON")

    return data


# ----------------------------------------------------------------------------- 
# 4. Request -> ContractInput (validation)
# ----------------------------------------------------------------------------- 

def parse_contract_input(data: Dict[str, Any]) -> ContractInput:
    language = (data.get("language") or "vi").lower()

    # Lấy context_rag (nếu BE gửi lên)
    rag_context = (data.get("context_rag") or "").strip() or None

    # File branch
    file_b64 = data.get("file_bytes_base64")
    file_format = (data.get("file_format") or "").lower()
    file_name = data.get("file_name") or None

    if file_b64 and file_format:
        if file_format not in Config.ALLOWED_FORMATS:
            raise ValueError(
                f"Unsupported file_format '{file_format}'. "
                f"Allowed: {sorted(Config.ALLOWED_FORMATS)}"
            )

        try:
            file_bytes = base64.b64decode(file_b64)
        except Exception as e:
            logger.warning("Invalid base64 for file_bytes_base64: %s", e)
            raise ValueError("file_bytes_base64 is not valid base64")

        if len(file_bytes) > Config.MAX_FILE_SIZE_BYTES:
            raise ValueError(
                "File is too large for Bedrock document input "
                "(limit khoảng 4.5 MB)."
            )

        return ContractInput(
            language=language,
            file_name=file_name,
            file_format=file_format,
            file_bytes=file_bytes,
            rag_context=rag_context,
        )

    # Text branch
    contract_text = (data.get("contract_text") or "").strip()
    if not contract_text:
        raise ValueError(
            "Either contract_text or (file_bytes_base64 + file_format) is required"
        )

    return ContractInput(
        language=language,
        contract_text=contract_text,
        rag_context=rag_context,
    )


# ----------------------------------------------------------------------------- 
# 5. RAG hook: dùng Lambda invoke thay vì API Gateway
# ----------------------------------------------------------------------------- 

def retrieve_legal_context(contract_text: str, language: str) -> str:
    """
    Gọi trực tiếp Lambda rag_search (invokeFunction), trả về text context để nhét vào prompt LLM.
    Nếu RAG lỗi hoặc chưa cấu hình, trả chuỗi rỗng để không chặn flow chính.

    Yêu cầu:
    - ENV: RAG_FUNCTION_NAME = tên hàm Lambda rag_search
    - IAM: lambda:InvokeFunction trên hàm đó
    """
    if not RAG_FUNCTION_NAME:
        return ""

    # Payload gửi sang rag_search
    payload = {
        "query": contract_text,
        "language": language,
        "top_k": 8,
        "filters": {
            "source_type": ["legal"],
        }
    }

    try:
        response = lambda_client.invoke(
            FunctionName=RAG_FUNCTION_NAME,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )
    except Exception as e:
        print(f"[WARN] RAG Lambda invoke failed: {e}")
        return ""

    try:
        raw_payload = response["Payload"].read()
        resp_payload = json.loads(raw_payload)
    except Exception as e:
        print(f"[WARN] Failed to parse RAG Lambda raw payload: {e}")
        return ""

    # Trường hợp rag_search đang trả theo format API (statusCode + body)
    if isinstance(resp_payload, dict) and "statusCode" in resp_payload:
        status = resp_payload.get("statusCode", 500)
        if status != 200:
            print(f"[WARN] RAG Lambda returned status {status}: {resp_payload.get('body')}")
            return ""
        body = resp_payload.get("body") or "{}"
        try:
            result = json.loads(body)
        except json.JSONDecodeError:
            print("[WARN] RAG Lambda body is not valid JSON")
            return ""
    else:
        # Nếu sau này rag_search trả raw dict, dùng luôn
        if isinstance(resp_payload, dict):
            result = resp_payload
        else:
            return ""

    chunks = result.get("results") or []
    if not chunks:
        return ""

    lines = []
    for i, c in enumerate(chunks, start=1):
        title = c.get("title") or ""
        article_no = c.get("article_no") or ""
        article_title = c.get("article_title") or ""
        text = c.get("text") or ""

        header = f"[Trích dẫn {i} – {title}"
        if article_no:
            header += f", {article_no}"
        if article_title:
            header += f": {article_title}"
        header += "]"

        lines.append(header)
        lines.append(text)
        lines.append("")

    context_str = "\n".join(lines)
    return context_str


# ----------------------------------------------------------------------------- 
# 6. Prompt builder & Bedrock client
# ----------------------------------------------------------------------------- 

def build_user_prompt_text(contract_text: str, context: str | None = None) -> str:
    """
    Prompt dùng cho mode TEXT.
    Có thêm context (RAG) nếu không rỗng.
    """
    base = (
        "Dưới đây là nội dung hợp đồng cần phân tích rủi ro. "
        "Hãy đọc kỹ và TRẢ VỀ DUY NHẤT một JSON hợp lệ theo đúng cấu trúc đã được mô tả trong hướng dẫn hệ thống.\n\n"
        "=== NỘI DUNG HỢP ĐỒNG ===\n"
        f"{contract_text}\n"
        "=== HẾT NỘI DUNG HỢP ĐỒNG ==="
    )

    if context:
        return (
            base
            + "\n\n=== THÔNG TIN THAM KHẢO TỪ THƯ VIỆN PHÁP LUẬT / MẪU HỢP ĐỒNG (RAG) ===\n"
            + context
            + "\n=== HẾT THÔNG TIN THAM KHẢO ==="
        )

    return base


def call_bedrock_text(contract_text: str, language: str) -> str:
    """
    Gọi Bedrock Converse API với TEXT và trả về raw text từ model.
    Có sẵn hook context từ RAG.
    """
    context = retrieve_legal_context(contract_text, language=language)
    user_prompt = build_user_prompt_text(contract_text, context=context)

    logger.info("Calling Bedrock model %s with TEXT ...", Config.MODEL_ID)

    try:
        response = bedrock.converse(
            modelId=Config.MODEL_ID,
            system=[{"text": SYSTEM_PROMPT}],
            messages=[
                {
                    "role": "user",
                    "content": [{"text": user_prompt}],
                }
            ],
            inferenceConfig={
                "maxTokens": 4096,
                "temperature": 0.2,
                "topP": 0.9,
            },
        )
        logger.info("Received response from Bedrock (text mode)")
    except ClientError as e:
        logger.error("Bedrock invocation failed (text mode): %s", e)
        raise

    try:
        output_message = response["output"]["message"]
        content_list = output_message.get("content", [])
        if not content_list:
            raise ValueError("Empty content from model (text mode)")
        model_text = content_list[0].get("text", "")
    except Exception as e:
        logger.error("Failed to extract text from Bedrock response (text mode): %s", e)
        raise

    return model_text


def call_bedrock_document(
    file_bytes: bytes,
    file_format: str,
    file_name: Optional[str],
    language: str,
    context: Optional[str] = None,
) -> str:
    """
    Gọi Bedrock Converse API với DOCUMENT (pdf/docx/...) và trả về raw text từ model.

    file_format: pdf, doc, docx, txt, md, html
    context: chuỗi RAG (luật, mẫu hợp đồng) đã được build ở BE (context_rag)
    """
    if not file_name:
        file_name = f"uploaded_contract.{file_format}"

    # Nhét context RAG (nếu có) vào user_text
    user_text = (
        "Tài liệu đính kèm là một hợp đồng cần phân tích rủi ro. "
        "Hãy đọc toàn bộ tài liệu và TRẢ VỀ DUY NHẤT một JSON hợp lệ theo đúng schema đã được mô tả trong system prompt."
    )

    if context:
        user_text += (
            "\n\n=== THÔNG TIN THAM KHẢO TỪ THƯ VIỆN PHÁP LUẬT / MẪU HỢP ĐỒNG (RAG) ===\n"
            f"{context}\n"
            "=== HẾT THÔNG TIN THAM KHẢO ==="
        )

    logger.info(
        "Calling Bedrock model %s with DOCUMENT (format=%s, name=%s, size=%d bytes, has_context=%s) ...",
        Config.MODEL_ID, file_format, file_name, len(file_bytes), bool(context)
    )

    try:
        response = bedrock.converse(
            modelId=Config.MODEL_ID,
            system=[{"text": SYSTEM_PROMPT}],
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"text": user_text},
                        {
                            "document": {
                                "format": file_format,
                                "name": file_name,
                                "source": {"bytes": file_bytes},
                            }
                        },
                    ],
                }
            ],
            inferenceConfig={
                "maxTokens": 4096,
                "temperature": 0.2,
                "topP": 0.9,
            },
        )
        logger.info("Received response from Bedrock (document mode)")
    except ClientError as e:
        logger.error("Bedrock invocation failed (document mode): %s", e)
        raise

    try:
        output_message = response["output"]["message"]
        content_list = output_message.get("content", [])
        if not content_list:
            raise ValueError("Empty content from model (document mode)")
        model_text = content_list[0].get("text", "")
    except Exception as e:
        logger.error(
            "Failed to extract text from Bedrock document response: %s", e
        )
        raise

    return model_text


# ----------------------------------------------------------------------------- 
# 7. Model JSON parsing
# ----------------------------------------------------------------------------- 

def parse_model_json(model_output_text: str) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Phiên bản robust: cố gắng trích JSON object đầu tiên trong output.
    """
    try:
        text = model_output_text.strip()
        
        # 1. Tìm vị trí dấu '{' đầu tiên
        start_idx = text.find("{")
        
        # 2. Tìm vị trí dấu '}' cuối cùng
        end_idx = text.rfind("}")
        
        if start_idx != -1 and end_idx != -1:
            text = text[start_idx : end_idx + 1]
        else:
            raise ValueError("No JSON object found in output")

        analysis = json.loads(text)
        return analysis, None

    except Exception as e:
        logger.warning("Model output parsing failed: %s", e)
        logger.info("Raw output causing error: %s", model_output_text) 
        
        analysis = {
            "summary": f"AI đã trả về kết quả nhưng không đúng định dạng JSON. (Lỗi: {str(e)})",
            "overall_risk_level": "UNKNOWN",
            "risk_items": [],
            "disclaimer": "Lỗi kỹ thuật từ phía AI Parser."
        }
        return analysis, model_output_text


# ----------------------------------------------------------------------------- 
# 8. Core use case: analyze_contract
# ----------------------------------------------------------------------------- 

def analyze_contract(contract_input: ContractInput) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Core logic:
    - Chọn mode TEXT hoặc DOCUMENT.
    - Gọi Bedrock tương ứng.
    - Parse JSON từ output.
    """
    if contract_input.has_file:
        model_output_text = call_bedrock_document(
            file_bytes=contract_input.file_bytes,
            file_format=contract_input.file_format,
            file_name=contract_input.file_name,
            language=contract_input.language,
            context=contract_input.rag_context,  # context RAG do BE truyền (nếu có)
        )
    elif contract_input.has_text:
        model_output_text = call_bedrock_text(
            contract_text=contract_input.contract_text,
            language=contract_input.language,
        )
    else:
        raise ValueError("No valid input provided")

    analysis, raw = parse_model_json(model_output_text)
    return analysis, raw


# ----------------------------------------------------------------------------- 
# 9. HTTP response helper
# ----------------------------------------------------------------------------- 

def make_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body, ensure_ascii=False),
    }


# ----------------------------------------------------------------------------- 
# 10. Lambda handler
# ----------------------------------------------------------------------------- 

def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event)[:1000])

    try:
        data = parse_event_body(event)
        contract_input = parse_contract_input(data)

        analysis, raw_model_output = analyze_contract(contract_input)

        response_body = {
            "analysis": analysis,
            "model": Config.MODEL_ID,
            "raw_model_output": raw_model_output,
            "language": contract_input.language,
        }
        return make_response(200, response_body)

    except ValueError as ve:
        logger.warning("Bad request: %s", ve)
        return make_response(400, {"error": str(ve)})

    except ClientError as ce:
        logger.error("Bedrock client error: %s", ce)
        return make_response(
            502,
            {"error": "Bedrock invocation failed", "details": str(ce)},
        )

    except Exception as e:
        logger.error("Unexpected error: %s", e)
        return make_response(
            500,
            {"error": "Internal server error", "details": str(e)},
        )