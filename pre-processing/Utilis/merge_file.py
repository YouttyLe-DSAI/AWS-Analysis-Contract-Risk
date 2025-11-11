# merge_file.py
# -*- coding: utf-8 -*-
"""
Hợp nhất file DOC/DOCX với file JSON (metadata) do nhiều thành viên cào.
Kết quả: 1 file JSONL, mỗi dòng là 1 chunk (theo Điều) + metadata đầy đủ.
"""

import os
import re
import json
from glob import glob

# ========= 1. HÀM ĐỌC DOC/DOCX BẰNG WINDOWS WORD / PYTHON-DOCX =========
# yêu cầu: pip install pywin32 python-docx
import win32com.client
from docx import Document


def load_doc_text(doc_path: str) -> str:
    """
    Đọc nội dung văn bản:
    - .docx -> dùng python-docx
    - .doc  -> dùng Word COM
    - .pdf  -> tạm bỏ qua (return "")
    """
    ext = os.path.splitext(doc_path)[1].lower()

    # đọc .docx
    if ext == ".docx":
        doc = Document(doc_path)
        paras = [p.text for p in doc.paragraphs]
        return "\n".join(paras)

    # đọc .doc bằng Word COM
    if ext == ".doc":
        word = win32com.client.Dispatch("Word.Application")
        word.Visible = False
        try:
            doc = word.Documents.Open(doc_path)
            text = doc.Content.Text
            doc.Close()
        finally:
            word.Quit()
        return text

    # nếu là pdf hoặc format khác thì bỏ qua
    if ext == ".pdf":
        print(f"[WARN] bỏ qua PDF: {doc_path}")
        return ""

    raise ValueError(f"Không đọc được định dạng: {doc_path}")


# ========= 2. TÁCH THEO "ĐIỀU ..." =========
def split_by_dieu(full_text: str):
    """
    Tách văn bản thành các đoạn theo mẫu 'Điều <số>'
    Trả về list[(heading, body)]
    """
    text = full_text.replace("\r\n", "\n").replace("\r", "\n")
    pattern = r"(?=^Điều\s+\d+[.\s])"
    parts = re.split(pattern, text, flags=re.MULTILINE)

    chunks = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        first_nl = part.find("\n")
        if first_nl != -1:
            heading = part[:first_nl].strip()
            body = part[first_nl + 1:].strip()
        else:
            heading = part
            body = ""
        chunks.append((heading, body))
    return chunks


# ========= 3. CHUẨN HÓA DOC_ID =========
def normalize_doc_id(symbol: str, fallback: str) -> str:
    """
    Ưu tiên dùng Số hiệu làm doc_id.
    Nếu không có thì dùng tên file.
    """
    if symbol:
        s = symbol.strip().upper()
        s = s.replace("–", "-").replace("—", "-")
        s = s.replace("NÐ", "NĐ")
        return s
    return fallback


# ========= 4. HÀM CHÍNH =========
def build_chunks(root_raw_dir: str, out_path: str):
    """
    root_raw_dir: thư mục 'raw' chứa các thư mục thành viên
    out_path: file jsonl đầu ra
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    out_f = open(out_path, "w", encoding="utf-8")

    # lấy danh sách các folder thành viên
    members = [
        d for d in os.listdir(root_raw_dir)
        if os.path.isdir(os.path.join(root_raw_dir, d))
    ]

    for member in members:
        member_dir = os.path.join(root_raw_dir, member)
        doc_dir = os.path.join(member_dir, "doc")
        json_dir = os.path.join(member_dir, "json")

        if not os.path.isdir(doc_dir) or not os.path.isdir(json_dir):
            print(f"[WARN] Bỏ qua {member} vì không có doc/ hoặc json/")
            continue

        # gom tất cả doc/docx/pdf
        doc_files = []
        doc_files += glob(os.path.join(doc_dir, "*.doc"))
        doc_files += glob(os.path.join(doc_dir, "*.docx"))
        doc_files += glob(os.path.join(doc_dir, "*.pdf"))

        print(f"[INFO] {member}: tìm thấy {len(doc_files)} file văn bản")

        for doc_path in doc_files:
            base_name = os.path.splitext(os.path.basename(doc_path))[0]
            json_path = os.path.join(json_dir, base_name + ".json")

            if not os.path.exists(json_path):
                print(f"[WARN] {member}: không tìm thấy JSON cho {base_name}, bỏ qua")
                continue

            # đọc metadata
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    json_obj = json.load(f)
            except Exception as e:
                print(f"[WARN] {member}: lỗi đọc {json_path}: {e}")
                continue

            meta = json_obj.get("meta", {})
            source_url = json_obj.get("source_url")
            relations_sections = json_obj.get("relations_sections") or {}
            content_connection = json_obj.get("content_connection") or []

            # đọc nội dung văn bản
            try:
                full_text = load_doc_text(doc_path)
            except Exception as e:
                print(f"[WARN] {member}: lỗi đọc DOC {doc_path}: {e}")
                continue

            if not full_text.strip():
                print(f"[WARN] {member}: file rỗng hoặc bỏ qua {doc_path}")
                continue

            # tách theo điều
            dieu_chunks = split_by_dieu(full_text)

            symbol = meta.get("Số hiệu") or meta.get("So hieu")
            doc_id = normalize_doc_id(symbol, base_name)

            for idx, (heading, body) in enumerate(dieu_chunks):
                text = (heading + "\n" + body).strip()

                record = {
                    "id": f"{doc_id}:{idx}",
                    "doc_id": doc_id,
                    "chunk_index": idx,
                    "section_title": heading,
                    "text": text,
                    "title": meta.get("Tiêu đề") or meta.get("Tieu de"),
                    "symbol": symbol,
                    "doc_type": meta.get("Loại văn bản") or meta.get("Loai van ban"),
                    "field": meta.get("Lĩnh vực, ngành"),
                    "issued_by": meta.get("Nơi ban hành"),
                    "signer": meta.get("Người ký"),
                    "issued_date": meta.get("Ngày ban hành"),
                    "effective_date": meta.get("Ngày hiệu lực"),
                    "published_date": meta.get("Ngày đăng"),
                    "status": meta.get("Tình trạng"),
                    "source_url": source_url,
                    "relations_sections": relations_sections,
                    "content_connection": content_connection,
                    "original_doc_path": doc_path,
                }

                out_f.write(json.dumps(record, ensure_ascii=False) + "\n")

            print(f"[OK] {member}/{base_name}: {len(dieu_chunks)} chunks")

    out_f.close()
    print(f"[DONE] đã tạo file: {out_path}")


# ========= 5. CHẠY =========
if __name__ == "__main__":
    # ⚠️ NHỚ sửa lại 2 dòng này cho đúng đường dẫn của bạn
    ROOT_RAW = r"D:\crawl_web\out_luocdo\raw"
    OUT_FILE = r"D:\crawl_web\out_luocdo\processed\chunks_with_meta.jsonl"

    build_chunks(ROOT_RAW, OUT_FILE)
