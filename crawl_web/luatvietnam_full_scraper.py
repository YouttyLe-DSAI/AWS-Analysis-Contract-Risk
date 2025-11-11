# -*- coding: utf-8 -*-
"""
luatvietnam_full_scraper.py

Flow:
1. Vào trang list (URL bạn đưa)
2. Lặp PageIndex=1..max → lấy tất cả link văn bản (kết thúc bằng -d1.html)
3. Với mỗi link:
    - mở trang
    - đảm bảo đang ở tab "Tóm tắt" (click nếu có)
    - bóc các trường thuộc tính:
        + tên văn bản
        + Cơ quan ban hành
        + Số hiệu
        + Loại văn bản
        + Ngày ban hành
        + Áp dụng
        + Lĩnh vực
        + Người ký
    - bóc phần "TÓM TẮT VĂN BẢN"
    - tìm link .doc/.docx → tải về
    - lưu 1 file JSON

Chạy:
    python luatvietnam_full_scraper.py \
      --list-url "https://luatvietnam.vn/van-ban/tim-van-ban.html?..." \
      --max-pages 3 \
      --out-dir out_luat
"""

import argparse
import json
import os
import re
import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}


# ----------------- tiện ích ----------------- #

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def clean_text(s):
    if not s:
        return None
    return " ".join(s.split())


def slugify(text):
    text = text.strip().lower()
    trans = str.maketrans(
        "àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ",
        "aaaaaaaaaaaaaaaaaeeeeeeeeeeeiiiiiooooooooooooooooouuuuuuuuuuuyyyyyd",
    )
    text = text.translate(trans)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "van-ban"


def download_doc(doc_url, out_dir):
    ensure_dir(out_dir)
    filename = os.path.basename(urlparse(doc_url).path)
    if not filename:
        filename = "van-ban.docx"
    out_path = os.path.join(out_dir, filename)
    if os.path.exists(out_path):
        return out_path
    try:
        r = requests.get(doc_url, headers=HEADERS, stream=True, timeout=30)
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        return out_path
    except Exception as e:
        print(f"[WARN] tải doc thất bại {doc_url}: {e}")
        return None


def extract_attr_by_label(soup: BeautifulSoup, label: str):
    """
    Bảng thuộc tính có cấu trúc <td>label</td><td>value</td>
    """
    el = soup.find(lambda tag: tag.name in ["td", "th", "span"] and label in tag.get_text(strip=True))
    if not el:
        return None
    td = el.find_next("td")
    if td:
        return clean_text(td.get_text(" ", strip=True))
    span = el.find_next("span")
    if span:
        return clean_text(span.get_text(" ", strip=True))
    return None


# ----------------- phần lấy detail ----------------- #

def parse_detail_html(url, html, out_dir):
    soup = BeautifulSoup(html, "html.parser")

    # tên văn bản
    h1 = soup.find("h1")
    ten_van_ban = clean_text(h1.get_text(strip=True)) if h1 else None

    co_quan_ban_hanh = extract_attr_by_label(soup, "Cơ quan ban hành")
    so_hieu = extract_attr_by_label(soup, "Số hiệu")
    loai_van_ban = extract_attr_by_label(soup, "Loại văn bản")
    ngay_ban_hanh = extract_attr_by_label(soup, "Ngày ban hành")
    ap_dung = extract_attr_by_label(soup, "Áp dụng")
    linh_vuc = extract_attr_by_label(soup, "Lĩnh vực")
    nguoi_ky = extract_attr_by_label(soup, "Người ký")

    # tóm tắt văn bản
    tom_tat = None
    for h in soup.find_all(["h2", "h3", "h4"]):
        if "tóm tắt" in h.get_text(strip=True).lower():
            parts = []
            sib = h.find_next_sibling()
            while sib and sib.name not in ["h2", "h3", "h4"]:
                txt = sib.get_text(" ", strip=True)
                if txt:
                    parts.append(txt)
                sib = sib.find_next_sibling()
            tom_tat = clean_text(" ".join(parts))
            break

    # tìm link doc
    doc_url = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".doc") or href.lower().endswith(".docx"):
            doc_url = urljoin(url, href)
            break

    local_doc = None
    if doc_url:
        local_doc = download_doc(doc_url, os.path.join(out_dir, "docs"))

    data = {
        "source_url": url,
        "ten_van_ban": ten_van_ban,
        "tom_tat_van_ban": tom_tat,
        "co_quan_ban_hanh": co_quan_ban_hanh,
        "so_hieu": so_hieu,
        "loai_van_ban": loai_van_ban,
        "ngay_ban_hanh": ngay_ban_hanh,
        "ap_dung": ap_dung,
        "linh_vuc": linh_vuc,
        "nguoi_ky": nguoi_ky,
        "doc_file_url": doc_url,
        "doc_file_local": local_doc,
    }

    base = data["so_hieu"] or data["ten_van_ban"] or "van-ban"
    base = slugify(base)
    json_path = os.path.join(out_dir, base + ".json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"[OK] Lưu metadata -> {json_path}")
    if local_doc:
        print(f"[OK] Tải file doc -> {local_doc}")

    return data


def scrape_detail_with_playwright(page, url, out_dir):
    print(f"    [detail] mở: {url}")
    try:
        page.goto(url, wait_until="load", timeout=60000)
    except PWTimeout:
        print("    [WARN] load chậm, lấy HTML hiện tại")

    # cố bấm tab "Tóm tắt" nếu có (để đúng phần bạn cần)
    try:
        tab = page.query_selector("text=Tóm tắt")
        if tab:
            tab.click()
            page.wait_for_timeout(400)
    except Exception:
        pass

    # đợi render xíu
    page.wait_for_timeout(1000)
    html = page.content()
    return parse_detail_html(url, html, out_dir)


# ----------------- phần list ----------------- #

def collect_links_from_list(page):
    links = set()
    for a in page.query_selector_all("a[href]"):
        href = a.get_attribute("href")
        if not href:
            continue
        if href.endswith("-d1.html"):
            full = urljoin(page.url, href)
            links.add(full)
    return list(links)


def build_page_url(base_url, page_index: int):
    # đơn giản nhất: thay chuỗi PageIndex=xx trong URL
    if "PageIndex=" in base_url:
        return re.sub(r"PageIndex=\d+", f"PageIndex={page_index}", base_url)
    # nếu không có, thêm vào
    joiner = "&" if "?" in base_url else "?"
    return f"{base_url}{joiner}PageIndex={page_index}"


def crawl_all(list_url, max_pages, out_dir, headless=True, sleep_sec=1.0):
    ensure_dir(out_dir)
    ensure_dir(os.path.join(out_dir, "docs"))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page(user_agent=HEADERS["User-Agent"])

        all_detail_links = []

        for i in range(1, max_pages + 1):
            page_url = build_page_url(list_url, i)
            print(f"[LIST] mở trang {i}: {page_url}")
            try:
                page.goto(page_url, wait_until="load", timeout=60000)
            except PWTimeout:
                print("[WARN] list load chậm, vẫn lấy link trong DOM hiện tại")
            page.wait_for_timeout(int(sleep_sec * 1000))
            links = collect_links_from_list(page)
            print(f"[LIST] trang {i} lấy được {len(links)} link chi tiết")
            if not links:
                # có thể là hết trang
                pass
            all_detail_links.extend(links)

            # duyệt từng link chi tiết ngay tại đây
            for link in links:
                # để tránh lỗi trùng JSON nếu chạy lại
                try:
                    scrape_detail_with_playwright(page, link, out_dir)
                except Exception as e:
                    print(f"[ERROR] detail lỗi {link}: {e}")

        browser.close()

    # lưu lại list link để lần sau khỏi crawl
    list_path = os.path.join(out_dir, "detail_links.txt")
    with open(list_path, "w", encoding="utf-8") as f:
        for link in dict.fromkeys(all_detail_links):
            f.write(link + "\n")
    print(f"[DONE] Đã duyệt xong. Tổng link: {len(all_detail_links)}. Lưu tại {list_path}")


# ----------------- main ----------------- #

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--list-url", required=True, help="URL trang danh sách (có PageIndex=1)")
    parser.add_argument("--max-pages", type=int, default=1, help="Số trang list muốn đi")
    parser.add_argument("--out-dir", default="out_luat", help="Nơi lưu json + docs")
    parser.add_argument("--headless", action="store_true", help="Chạy ẩn (headless)")
    args = parser.parse_args()

    crawl_all(
        list_url=args.list_url,
        max_pages=args.max_pages,
        out_dir=args.out_dir,
        headless=args.headless,
        sleep_sec=1.0,
    )


if __name__ == "__main__":
    main()
