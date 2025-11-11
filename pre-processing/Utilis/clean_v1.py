# clean_chunks.py
# -*- coding: utf-8 -*-
import json
import os
import argparse

def clean_file(input_path, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    removed = 0
    total = 0

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(output_path, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            total += 1
            if "crawler_owner" in obj:
                obj.pop("crawler_owner")
                removed += 1
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")

    print(f"[DONE] cleaned: {output_path}")
    print(f"[INFO] tổng dòng: {total}, dòng bỏ crawler_owner: {removed}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="input_path", required=True)
    parser.add_argument("--out", dest="output_path", required=True)
    args = parser.parse_args()

    clean_file(args.input_path, args.output_path)
