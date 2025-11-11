import os
import json
import shutil
import unicodedata
import re
from pathlib import Path

# ====== CẤU HÌNH Ở ĐÂY ======
DOC_DIR = r"D:\crawl_web\out_luocdo\raw\Tứng\doc"      # folder chứa .doc/.docx
JSON_DIR = r"D:\crawl_web\out_luocdo\raw\Tứng\json"    # folder chứa .json
OUT_DIR = r"D:\crawl_web\pre-processing\classified_docs"  # folder sẽ bỏ doc đã phân loại
MOVE_FILE = True  # True = move, False = copy
# ============================

def slugify(text: str) -> str:
    # bỏ dấu + lowercase + thay khoảng trắng bằng _
    text = unicodedata.normalize('NFKD', text)
    text = "".join([c for c in text if not unicodedata.combining(c)])
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text or "khac"

def main():
    DOC_DIR_PATH = Path(DOC_DIR)
    JSON_DIR_PATH = Path(JSON_DIR)
    OUT_DIR_PATH = Path(OUT_DIR)
    OUT_DIR_PATH.mkdir(parents=True, exist_ok=True)

    # index các file doc theo tên gốc
    doc_map = {}
    for p in DOC_DIR_PATH.iterdir():
        if p.is_file() and p.suffix.lower() in [".doc", ".docx"]:
            stem = p.stem  # ví dụ 01_2017_N_-CP
            doc_map[stem] = p

    missing_doc = []
    missing_json = []

    # duyệt json
    for json_file in JSON_DIR_PATH.iterdir():
        if not json_file.is_file() or json_file.suffix.lower() != ".json":
            continue

        stem = json_file.stem  # ví dụ 01_2017_N_-CP

        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[ERR] Không đọc được JSON {json_file}: {e}")
            continue

        # cố gắng lấy trường Loại văn bản
        loai = None
        meta = data.get("meta") or {}
        # các key có thể gặp
        for key in ["Loại văn bản", "Loai van ban", "loai_van_ban", "Loại VB"]:
            if key in meta:
                loai = meta[key]
                break

        if not loai:
            loai = "khac"

        loai_slug = slugify(loai)

        # tìm doc tương ứng
        doc_path = doc_map.get(stem)
        if not doc_path:
            missing_doc.append(stem)
            print(f"[MISS DOC] Không thấy DOC cho JSON: {stem}")
            continue

        # tạo folder loại
        target_dir = OUT_DIR_PATH / loai_slug
        target_dir.mkdir(parents=True, exist_ok=True)

        target_path = target_dir / doc_path.name

        if MOVE_FILE:
            shutil.move(str(doc_path), str(target_path))
            action = "MOVE"
        else:
            shutil.copy2(str(doc_path), str(target_path))
            action = "COPY"

        print(f"[{action}] {doc_path.name} -> {target_dir}")

        # xóa khỏi map để lát nữa biết doc nào không có json
        doc_map.pop(stem, None)

    # những doc còn lại trong doc_map là doc không có json
    if doc_map:
        print("\n[WARN] Các DOC không tìm thấy JSON tương ứng:")
        for stem, path in doc_map.items():
            print(" -", path.name)
            missing_json.append(path.name)

    # ghi log ra file
    (OUT_DIR_PATH / "missing_doc.txt").write_text(
        "\n".join(missing_doc), encoding="utf-8"
    )
    (OUT_DIR_PATH / "missing_json.txt").write_text(
        "\n".join(missing_json), encoding="utf-8"
    )

    print("\nDone.")

if __name__ == "__main__":
    main()
