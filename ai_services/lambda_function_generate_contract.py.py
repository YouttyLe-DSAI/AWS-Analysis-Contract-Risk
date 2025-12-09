import json
import os
import uuid
import datetime
import logging
from typing import Dict, Any, List

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# -------------------------------------------------------------------
# Config & clients
# -------------------------------------------------------------------

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")

TEMPLATE_BUCKET = os.getenv("TEMPLATE_BUCKET")
TEMPLATE_METADATA_KEY = os.getenv("TEMPLATE_METADATA_KEY", "index/template_metadata.jsonl")

MODEL_ID = os.getenv("MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")

# Lambda RAG-search (đã triển khai ở giai đoạn 2.3)
RAG_FUNCTION_NAME = os.getenv("RAG_FUNCTION_NAME", "ragsearch")

if not TEMPLATE_BUCKET:
    raise RuntimeError("TEMPLATE_BUCKET env var is required")

s3 = boto3.client("s3", region_name=AWS_REGION)
bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)

# -------------------------------------------------------------------
# Global cache template metadata
# -------------------------------------------------------------------

TEMPLATE_CACHE = {
    "loaded": False,
    "by_id": {}  # dict[doc_id] = metadata dict
}


def load_template_metadata_if_needed():
    if TEMPLATE_CACHE["loaded"]:
        return

    logger.info("Loading template metadata from s3://%s/%s ...", TEMPLATE_BUCKET, TEMPLATE_METADATA_KEY)

    try:
        obj = s3.get_object(Bucket=TEMPLATE_BUCKET, Key=TEMPLATE_METADATA_KEY)
    except ClientError as e:
        logger.error("Failed to load template metadata: %s", e)
        raise

    by_id = {}

    for line in obj["Body"].iter_lines():
        if not line:
            continue
        try:
            rec = json.loads(line.decode("utf-8"))
        except json.JSONDecodeError:
            logger.warning("Invalid JSON line in template metadata, skipped")
            continue

        doc_id = rec.get("doc_id")
        if not doc_id:
            continue
        by_id[doc_id] = rec

    TEMPLATE_CACHE["by_id"] = by_id
    TEMPLATE_CACHE["loaded"] = True

    logger.info("Loaded %d template metadata records", len(by_id))


def parse_event_body(event: Dict[str, Any]) -> Dict[str, Any]:
    if "body" not in event:
        return event
    body = event["body"]
    if event.get("isBase64Encoded"):
        import base64
        body = base64.b64decode(body).decode("utf-8")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        raise ValueError("Request body must be valid JSON")
    return data


def load_template_raw_text(metadata: Dict[str, Any]) -> str:
    """
    MVP: thử đọc nội dung file template từ S3 như text (nhiều file .doc/.docx sẽ không đọc được,
    nhưng không sao, chỉ dùng tham khảo cấu trúc nếu decode được).
    Sau này nếu anh muốn xử lý chuẩn DOCX thì ta thêm python-docx.
    """
    source_raw_path = metadata.get("source_raw_path")
    if not source_raw_path:
        return ""

    try:
        obj = s3.get_object(Bucket=TEMPLATE_BUCKET, Key=source_raw_path)
    except ClientError as e:
        logger.warning("Failed to load template file %s: %s", source_raw_path, e)
        return ""

    try:
        content_bytes = obj["Body"].read()
        text = content_bytes.decode("utf-8", errors="ignore")
        return text
    except Exception:
        return ""


# -------------------------------------------------------------------
# RAG integration
# -------------------------------------------------------------------

def build_rag_query(template_metadata: Dict[str, Any], contract_info: Dict[str, Any]) -> str:
    """
    Xây dựng query gửi cho RAG-search:
    - Nêu loại hợp đồng (template_type, title).
    - Đính kèm JSON contract_info.
    Mục tiêu: tìm các điều luật liên quan đến loại hợp đồng và các nội dung chính.
    """
    template_type = template_metadata.get("template_type") or ""
    title = template_metadata.get("title") or ""

    header = f"Loại hợp đồng: {template_type}. Tiêu đề: {title}."
    body = json.dumps(contract_info, ensure_ascii=False, indent=2)

    query = (
        header
        + "\n\nDưới đây là thông tin chi tiết về hợp đồng (JSON):\n"
        + body
        + "\n\nHãy tìm các văn bản pháp luật Việt Nam liên quan trực tiếp tới loại hợp đồng này, "
          "đặc biệt là về: thời hạn thuê/mua, nghĩa vụ các bên, chấm dứt hợp đồng, phạt vi phạm, "
          "bồi thường thiệt hại, quyền sử dụng tài sản."
    )
    return query


def call_rag_lambda(query: str, language: str = "vi") -> Dict[str, Any]:
    """
    Gọi trực tiếp Lambda rag_search (invokeFunction).
    Lambda rag_search hiện tại trả về dạng:
      { "statusCode": 200, "body": "{\"query\":..., \"results\": [...]}" }
    """
    if not RAG_FUNCTION_NAME:
        return {}

    payload = {
        "query": query,
        "language": language,
        "top_k": 6,
        "filters": {
            "source_type": ["legal"]
            # Có thể thêm "field": ["Xây dựng - Đô thị"] nếu muốn tập trung BĐS
        }
    }

    try:
        response = lambda_client.invoke(
            FunctionName=RAG_FUNCTION_NAME,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )
    except Exception as e:
        logger.warning("RAG Lambda invoke failed: %s", e)
        return {}

    try:
        raw_payload = response["Payload"].read()
        resp_payload = json.loads(raw_payload)
    except Exception as e:
        logger.warning("Failed to parse RAG Lambda raw payload: %s", e)
        return {}

    # Trường hợp rag_search vẫn đang dùng make_response(statusCode, body)
    if isinstance(resp_payload, dict) and "statusCode" in resp_payload:
        status = resp_payload.get("statusCode", 500)
        if status != 200:
            logger.warning("RAG Lambda returned status %s: %s", status, resp_payload.get("body"))
            return {}
        body = resp_payload.get("body") or "{}"
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            logger.warning("RAG Lambda body is not valid JSON")
            return {}

    # Nếu sau này anh sửa rag_search trả raw dict, có thể rơi vào đây
    if isinstance(resp_payload, dict):
        return resp_payload

    return {}


def build_legal_context_text(rag_result: Dict[str, Any]) -> str:
    """
    Nhận kết quả từ RAG (rag_search) và build thành một block text để nhét vào prompt.
    """
    chunks = rag_result.get("results") or []
    if not chunks:
        return ""

    lines: List[str] = []
    for i, c in enumerate(chunks, start=1):
        title = c.get("title") or ""
        article_no = c.get("article_no") or ""
        article_title = c.get("article_title") or ""
        text = c.get("text") or ""

        header = f"[Trích dẫn pháp luật {i} – {title}"
        if article_no:
            header += f", {article_no}"
        if article_title:
            header += f": {article_title}"
        header += "]"

        lines.append(header)
        lines.append(text)
        lines.append("")

    return "\n".join(lines)


def retrieve_legal_context_for_template(
    template_metadata: Dict[str, Any],
    contract_info: Dict[str, Any],
    language: str
) -> str:
    """
    Hàm tổng: build query -> call RAG -> build text.
    Nếu có lỗi hoặc không có kết quả, trả về "" để không chặn flow chính.
    """
    try:
        query = build_rag_query(template_metadata, contract_info)
        rag_result = call_rag_lambda(query=query, language=language)
        context_text = build_legal_context_text(rag_result)
        return context_text
    except Exception as e:
        logger.warning("retrieve_legal_context_for_template failed: %s", e)
        return ""


# -------------------------------------------------------------------
# LLM: sinh hợp đồng
# -------------------------------------------------------------------

def build_system_prompt() -> str:
    return (
        "Bạn là một luật sư hợp đồng tại Việt Nam, chuyên soạn thảo hợp đồng thuê/mua bán/chuyển nhượng bất động sản.\n"
        "- Ngôn ngữ: tiếng Việt, văn phong rõ ràng, chặt chẽ, nhưng dễ hiểu.\n"
        "- Hãy soạn thảo hợp đồng dựa trên thông tin đầu vào (contract_info), loại hợp đồng (template_type) "
        "và các trích dẫn pháp luật được cung cấp.\n"
        "- Nếu thông tin đầu vào chưa đầy đủ, hãy điền các điều khoản theo thông lệ phổ biến, "
        "nhưng không bịa số liệu quá cụ thể.\n"
        "- Luôn đảm bảo quyền và nghĩa vụ của các bên cân bằng, ưu tiên tuân thủ pháp luật Việt Nam.\n"
        "- Output CHỈ là nội dung hợp đồng hoàn chỉnh dạng text thuần, không thêm giải thích.\n"
    )


def build_user_prompt(
    template_metadata: Dict[str, Any],
    contract_info: Dict[str, Any],
    template_raw_text: str,
    legal_context: str
) -> str:
    """
    Prompt cho model: cung cấp
    - Thông tin template (type, title, ...).
    - contract_info (JSON).
    - context pháp luật từ RAG.
    - (Optional) snippet template_raw_text nếu đọc được.
    """
    title = template_metadata.get("title") or ""
    template_type = template_metadata.get("template_type") or ""
    template_desc = f"Loại hợp đồng: {template_type}. Tiêu đề mẫu: {title}."

    user_parts: List[str] = []
    user_parts.append(template_desc)

    # Thông tin pháp luật từ RAG
    if legal_context:
        user_parts.append(
            "\nDưới đây là một số trích dẫn pháp luật và điều khoản liên quan do hệ thống truy xuất được "
            "(hãy dùng làm căn cứ khi soạn thảo, nhưng không cần trích nguyên văn toàn bộ):\n"
        )
        user_parts.append(legal_context)

    # contract_info
    user_parts.append("\nDưới đây là thông tin đầu vào (contract_info) ở dạng JSON:\n")
    user_parts.append(json.dumps(contract_info, ensure_ascii=False, indent=2))

    # snippet từ template gốc (nếu có)
    if template_raw_text:
        max_chars = 3000
        snippet = template_raw_text[:max_chars]
        user_parts.append(
            "\nDưới đây là một phần nội dung gốc của mẫu hợp đồng (nếu đọc được, chỉ dùng tham khảo cấu trúc, "
            "không cần copy y nguyên):\n"
        )
        user_parts.append(snippet)

    user_parts.append(
        "\nYÊU CẦU:\n"
        "- Hãy soạn thảo TOÀN BỘ hợp đồng hoàn chỉnh, có đầy đủ phần mở đầu, điều khoản chi tiết, "
        "điều khoản chung, điều khoản về giải quyết tranh chấp, chữ ký.\n"
        "- Trả về nội dung hợp đồng ở dạng text thuần (plain text), mỗi điều khoản nên cách nhau ít nhất một dòng trống.\n"
        "- Không thêm bất kỳ giải thích nào ngoài nội dung hợp đồng."
    )

    return "\n".join(user_parts)


def call_bedrock_generate_contract(system_prompt: str, user_prompt: str) -> str:
    """
    Gọi Bedrock (Claude Haiku) để sinh hợp đồng, trả về text thuần.
    """
    logger.info("Calling Bedrock model %s for contract generation ...", MODEL_ID)

    try:
        response = bedrock.converse(
            modelId=MODEL_ID,
            system=[{"text": system_prompt}],
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
    except ClientError as e:
        logger.error("Bedrock invocation failed: %s", e)
        raise

    try:
        output_message = response["output"]["message"]
        content_list = output_message.get("content", [])
        if not content_list:
            raise ValueError("Empty content from model")
        model_text = content_list[0].get("text", "")
    except Exception as e:
        logger.error("Failed to extract text from Bedrock response: %s", e)
        raise

    return model_text


def to_html_from_text(contract_text: str) -> str:
    """
    Đơn giản: mỗi dòng -> <p>, dòng trống -> <br>.
    """
    lines = contract_text.splitlines()
    html_lines: List[str] = []
    html_lines.append("<html><body>")
    for line in lines:
        line = line.strip()
        if not line:
            html_lines.append("<br>")
        else:
            esc = (
                line.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )
            html_lines.append(f"<p>{esc}</p>")
    html_lines.append("</body></html>")
    return "\n".join(html_lines)


def save_generated_to_s3(contract_text: str, contract_html: str) -> Dict[str, str]:
    """
    Lưu contract_text và contract_html lên S3, trả về paths.
    """
    now = datetime.datetime.utcnow()
    y = now.year
    m = now.month
    contract_id = str(uuid.uuid4())

    base_prefix = f"generated/contracts/{y}/{m:02d}/{contract_id}"
    text_key = f"{base_prefix}.txt"
    html_key = f"{base_prefix}.html"

    try:
        s3.put_object(
            Bucket=TEMPLATE_BUCKET,
            Key=text_key,
            Body=contract_text.encode("utf-8"),
            ContentType="text/plain; charset=utf-8",
        )
        s3.put_object(
            Bucket=TEMPLATE_BUCKET,
            Key=html_key,
            Body=contract_html.encode("utf-8"),
            ContentType="text/html; charset=utf-8",
        )
    except ClientError as e:
        logger.warning("Failed to upload generated contract to S3: %s", e)
        return {}

    return {
        "contract_text_path": text_key,
        "contract_html_path": html_key,
    }


def make_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",  # demo
        },
        "body": json.dumps(body, ensure_ascii=False),
    }


# -------------------------------------------------------------------
# Lambda handler
# -------------------------------------------------------------------

def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event)[:1000])

    try:
        data = parse_event_body(event)

        template_id = (data.get("template_id") or "").strip()
        if not template_id:
            return make_response(400, {"error": "template_id is required"})

        contract_info = data.get("contract_info") or {}
        if not isinstance(contract_info, dict):
            return make_response(400, {"error": "contract_info must be an object"})

        language = (data.get("language") or "vi").lower()

        # 1. Load template metadata
        load_template_metadata_if_needed()
        metadata = TEMPLATE_CACHE["by_id"].get(template_id)
        if not metadata:
            return make_response(404, {"error": f"Template not found for template_id={template_id}"})

        # 2. (Optional) load raw text của file template từ S3
        template_raw_text = load_template_raw_text(metadata)

        # 3. Lấy context pháp luật từ RAG
        legal_context = retrieve_legal_context_for_template(metadata, contract_info, language)

        # 4. Build prompts
        system_prompt = build_system_prompt()
        user_prompt = build_user_prompt(metadata, contract_info, template_raw_text, legal_context)

        # 5. Gọi Bedrock để sinh hợp đồng
        contract_text = call_bedrock_generate_contract(system_prompt, user_prompt)

        # 6. Convert sang HTML
        contract_html = to_html_from_text(contract_text)

        # 7. Lưu lên S3
        s3_paths = save_generated_to_s3(contract_text, contract_html)

        # 8. Build response
        resp_body = {
            "template_id": template_id,
            "template_title": metadata.get("title"),
            "template_type": metadata.get("template_type"),
            "language": language,
            "contract_text": contract_text,
            "contract_html": contract_html,
            "s3_paths": s3_paths,
            "debug": {
                "used_template_file": metadata.get("source_raw_path"),
                "source_type": metadata.get("source_type"),
                "rag_used": bool(legal_context),
            },
        }

        return make_response(200, resp_body)

    except ValueError as ve:
        logger.warning("Bad request: %s", ve)
        return make_response(400, {"error": str(ve)})

    except ClientError as ce:
        logger.error("AWS client error: %s", ce)
        return make_response(502, {"error": "Upstream AWS error", "details": str(ce)})

    except Exception as e:
        logger.error("Unexpected error: %s", e)
        return make_response(500, {"error": "Internal server error", "details": str(e)})
