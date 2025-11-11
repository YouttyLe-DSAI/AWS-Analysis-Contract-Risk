import json
import re

in_path = "D:\crawl_web\out_luocdo\processed\chunks_clean.jsonl"
out_path = "D:\crawl_web\out_luocdo\processed\chunks_clean_unicode.jsonl"

# bảng chuyển từ TCVN3 sang Unicode (rút gọn, thêm dần nếu gặp)
TCVN3_MAP = {
    "µ": "ư", "¶": "ứ", "·": "ừ", "¸": "ữ", "¹": "ự",
    "¨": "ă", "©": "ắ", "ª": "ằ", "«": "ẳ", "¬": "ẵ", "­": "ặ",
    "¸": "á", "µ": "ú",
    "Ò": "Ó",  # bạn bổ sung dần
}

# bảng chuyển từ VNI sang Unicode (rút gọn)
VNI_MAP = {
    "õ": "õ",  # ví dụ, bạn thêm dần
}

def looks_like_tcvn3(s: str) -> bool:
    # nếu chứa nhiều ký tự trong dải này thì đoán là TCVN3
    return bool(re.search(r"[µ¶·¸¨©ª«¬­]", s))

def convert_tcvn3(s: str) -> str:
    for k, v in TCVN3_MAP.items():
        s = s.replace(k, v)
    return s

def clean_text(s: str) -> str:
    # dọn rác chung
    s = s.replace("\u00a0", " ").replace("\ufeff", "")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

with open(in_path, "r", encoding="utf-8") as fin, \
     open(out_path, "w", encoding="utf-8") as fout:
    for line in fin:
        obj = json.loads(line)
        text = obj.get("text", "")

        # nếu nhìn giống TCVN3 thì convert
        if looks_like_tcvn3(text):
            text = convert_tcvn3(text)

        text = clean_text(text)
        obj["text"] = text

        # tiêu đề cũng xử lý
        if "title" in obj and obj["title"]:
            t2 = obj["title"]
            if looks_like_tcvn3(t2):
                t2 = convert_tcvn3(t2)
            obj["title"] = clean_text(t2)

        fout.write(json.dumps(obj, ensure_ascii=False) + "\n")

print("Đã ghi ra:", out_path)
