import json
import shutil
from pathlib import Path

# ====== CẤU HÌNH Ở ĐÂY ======
CLASSIFIED_DOC_ROOT = r"D:\crawl_web\pre-processing\classified_docs"  # nơi bạn đã phân loại DOC theo loại
JSON_ROOT = r"D:\crawl_web\out_luocdo\raw\Tứng\json"                 # nơi đang chứa toàn bộ JSON ban đầu
MOVE_FILE = False  # True = move, False = copy
# ============================

def main():
    classified_root = Path(CLASSIFIED_DOC_ROOT)
    json_root = Path(JSON_ROOT)

    missing_json = []
    done = 0

    # duyệt toàn bộ subfolder trong classified (nghi_dinh, thong_tu, quyet_dinh, ...)
    for doc_type_dir in classified_root.rglob("*"):
        if not doc_type_dir.is_dir():
            continue

        # trong từng folder đó, tìm .doc/.docx
        for doc_file in doc_type_dir.iterdir():
            if not doc_file.is_file() or doc_file.suffix.lower() not in [".doc", ".docx"]:
                continue

            stem = doc_file.stem  # tên gốc
            json_path = json_root / f"{stem}.json"

            if json_path.exists():
                target = doc_type_dir / json_path.name
                if MOVE_FILE:
                    shutil.move(str(json_path), str(target))
                    action = "MOVE"
                else:
                    shutil.copy2(str(json_path), str(target))
                    action = "COPY"
                print(f"[{action}] {json_path.name} -> {doc_type_dir}")
                done += 1
            else:
                print(f"[MISS JSON] Không thấy {stem}.json")
                missing_json.append(stem)

    # ghi log
    log_path = classified_root / "missing_json_from_docs.txt"
    log_path.write_text("\n".join(missing_json), encoding="utf-8")

    print(f"\nXong. Đã ghép được {done} JSON.")
    print(f"File thiếu JSON được ghi ở: {log_path}")

if __name__ == "__main__":
    main()
