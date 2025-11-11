import asyncio, json, re, hashlib
from collections import deque, OrderedDict
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin
from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeoutError

# ================== CẤU HÌNH NHANH ==================
SEED_URL   = "https://thuvienphapluat.vn/page/tim-van-ban.aspx?keyword=quy%e1%ba%bft+%c4%91%e1%bb%8bnh+%c4%91%e1%ba%a5t+%c4%91ai&area=0&match=True&type=17&status=0&signer=0&bdate=01/11/1986&sort=1&lan=1&scan=0&org=0&fields=&page=6"
OUTPUT_DIR = Path("out_luocdo")  # thư mục chứa docs + checkpoint + downloads
HEADLESS   = False               # để bạn nhìn và đăng nhập
VIEWPORT   = {"width": 1366, "height": 880}
DOMAIN_OK  = "thuvienphapluat.vn"
BASE_URL   = "https://thuvienphapluat.vn"
AUTH_STATE_PATH = OUTPUT_DIR / "auth_state.json"

# ================== HẰNG SỐ / SELECTORS ==================
FIELDS_ORDER = [
    "Tiêu đề",
    "Số hiệu",
    "Loại văn bản",
    "Lĩnh vực, ngành",
    "Nơi ban hành",
    "Người ký",
    "Ngày ban hành",
    "Ngày hiệu lực",
    "Ngày đăng",
    "Số công báo",
    "Tình trạng",
]

DIAGRAM_TAB       = 'a[href="#tab4"]'
VIEWING_DOCUMENT  = '#viewingDocument.ct'
SECTION_HEADER    = '.ghd, .ghda'
SECTION_BOX       = '.ct'
LOAD_MORE         = '.dgcvm'
LINKS_IN_SECTION  = '.dgc a[href]'
CONTENT_CONN_WRAP = '#contentConnection .dgcParent .dgc a[href]'

# ================== TIỆN ÍCH LƯU FILE ==================
def ensure_dirs():
    (OUTPUT_DIR / "docs").mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "checkpoints").mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "downloads").mkdir(parents=True, exist_ok=True)

def save_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def safe_name(s: str) -> str:
    return re.sub(r"[^0-9A-Za-z._-]+", "_", s).strip("_") or "unknown"

# ================== HỖ TRỢ URL ==================
def url_in_domain(url: str) -> bool:
    return DOMAIN_OK in url.lower()

def tail_numeric_id(url: str) -> Optional[str]:
    m = re.search(r"-(\d+)\.aspx$", url)
    return m.group(1) if m else None

def make_fallback_id(url: str) -> str:
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
    return f"u_{h}"

# ================== PLAYWRIGHT HELPERS ==================
async def ensure_tab4(page: Page):
    try:
        if await page.locator(DIAGRAM_TAB).count() > 0:
            await page.click(DIAGRAM_TAB)
            await page.wait_for_timeout(400)
    except Exception:
        pass

async def expand_all_in(page: Page, section_id: str, max_clicks: int = 20):
    container = page.locator(f"#{section_id}")
    for _ in range(max_clicks):
        btns = container.locator(LOAD_MORE)
        cnt = await btns.count()
        if cnt == 0:
            break
        clicked = False
        for i in range(cnt):
            b = btns.nth(i)
            if await b.is_visible():
                try:
                    await b.click()
                    await page.wait_for_timeout(300)
                    clicked = True
                    break
                except Exception:
                    continue
        if not clicked:
            break

async def collect_links_in_section(page: Page, section_id: str) -> List[Dict[str, str]]:
    try:
        await page.wait_for_selector(f'#{section_id}{SECTION_BOX}', state="visible", timeout=6000)
    except PWTimeoutError:
        return []
    await expand_all_in(page, section_id)

    links = page.locator(f'#{section_id} {LINKS_IN_SECTION}')
    items: List[Dict[str, str]] = []
    for i in range(await links.count()):
        a = links.nth(i)
        try:
            name = " ".join((await a.inner_text()).split())
            url  = (await a.get_attribute("href")) or ""
            if name and url:
                items.append({"name": name, "url": url})
        except Exception:
            continue
    return items

async def collect_content_connection(page: Page) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    if await page.locator("#contentConnection").count() == 0:
        return items
    links = page.locator(CONTENT_CONN_WRAP)
    for i in range(await links.count()):
        a = links.nth(i)
        try:
            name = " ".join((await a.inner_text()).split())
            url  = (await a.get_attribute("href")) or ""
            if name and url:
                items.append({"name": name, "url": url})
        except Exception:
            continue
    return items

async def collect_column_sections(page: Page, container_selector: str) -> Dict[str, List[Dict[str, str]]]:
    result: Dict[str, List[Dict[str, str]]] = {}
    container = page.locator(container_selector)
    if await container.count() == 0:
        return result

    headers = container.locator(SECTION_HEADER)
    for i in range(await headers.count()):
        h = headers.nth(i)
        title = (await h.inner_text()).strip()
        onclick = await h.get_attribute("onclick")
        if not onclick:
            continue
        m = re.search(r"toggle\('([^']+)'\)", onclick)
        if not m:
            continue
        section_id = m.group(1)
        try:
            await h.click()
            await page.wait_for_timeout(250)
        except Exception:
            pass
        clean_title = re.sub(r"\s*\[\s*\d+\s*\]\s*", "", title).strip()
        key = f"{section_id} | {clean_title}"
        result[key] = await collect_links_in_section(page, section_id)
    return result

async def scrape_viewing_document(page: Page) -> OrderedDict:
    await page.wait_for_selector(VIEWING_DOCUMENT, state="visible", timeout=8000)
    box = page.locator(VIEWING_DOCUMENT)

    data = OrderedDict((k, "") for k in FIELDS_ORDER)

    # Tiêu đề
    titles = box.locator(".tt")
    for i in range(await titles.count()):
        t = (await titles.nth(i).inner_text()).strip()
        if t:
            data["Tiêu đề"] = " ".join(t.split())
            break

    # Các cặp label/value
    rows = box.locator(".att")
    for i in range(await rows.count()):
        row = rows.nth(i)
        key = (await row.locator(".hd.fl").inner_text()).strip() if await row.locator(".hd.fl").count() else ""
        val = (await row.locator(".ds.fl").inner_text()).strip() if await row.locator(".ds.fl").count() else ""
        if not key:
            continue
        key = key[:-1].strip() if key.endswith(":") else key
        key = " ".join(key.split())
        val = " ".join(val.split())
        if key in data:
            data[key] = val
    return data

# ================== TAB 4 ==================
async def scrape_full_tab4(page: Page) -> Tuple[OrderedDict, Dict[str, List[Dict[str, str]]], List[Dict[str, str]]]:
    await ensure_tab4(page)
    meta_view = await scrape_viewing_document(page)
    left_sections  = await collect_column_sections(page, ".left.fl")
    right_sections = await collect_column_sections(page, ".rr.fl")
    sections = {**left_sections, **right_sections}
    content_conn = await collect_content_connection(page)
    return meta_view, sections, content_conn

# ================== MỞ TAB 'Tải về' + LẤY HREF ==================
async def open_download_tab(page: Page) -> bool:
    # ưu tiên theo text
    tabs = page.locator('a:has-text("Tải về")')
    if await tabs.count() > 0:
        try:
            await tabs.nth(0).click()
            await page.wait_for_timeout(500)
            return True
        except Exception:
            pass

    # fallback nếu nó xài href dạng #tab7
    for maybe in ["#tab7", "#tab5", "#tab6"]:
        loc = page.locator(f'a[href="{maybe}"]')
        if await loc.count() > 0:
            try:
                await loc.nth(0).click()
                await page.wait_for_timeout(500)
                return True
            except Exception:
                pass
    return False

async def download_vietnamese_doc(page: Page, doc_id: str):
    """
    Mở tab 'Tải về', lấy href của 'Tải Văn bản tiếng Việt', request trực tiếp và lưu file.
    """
    opened = await open_download_tab(page)
    if not opened:
        return False

    # id mà bạn gửi
    sel = '#ctl00_Content_ThongTinVB_vietnameseHyperLink'
    if await page.locator(sel).count() == 0:
        # fallback text
        link_loc = page.locator('a:has-text("Tải Văn bản tiếng Việt"), a:has-text("Văn bản tiếng Việt")')
        if await link_loc.count() == 0:
            return False
        link = link_loc.nth(0)
    else:
        link = page.locator(sel)

    href = await link.get_attribute("href")
    if not href:
        return False

    # build absolute url
    abs_url = urljoin(BASE_URL, href)

    # dùng request của context để tải
    try:
        resp = await page.context.request.get(abs_url)
        if resp.status != 200:
            print(f"[DL] {doc_id} tải thất bại, HTTP {resp.status}")
            return False

        content = await resp.body()

        # đoán đuôi
        ctype = resp.headers.get("content-type", "").lower()
        if ".docx" in abs_url or "vnd.openxmlformats-officedocument.wordprocessingml.document" in ctype:
            ext = "docx"
        elif ".doc" in abs_url or "msword" in ctype:
            ext = "doc"
        else:
            # mặc định doc
            ext = "doc"

        out_path = OUTPUT_DIR / "downloads" / f"{doc_id}.{ext}"
        out_path.write_bytes(content)
        print(f"[DL] saved download for {doc_id} -> {out_path.name}")
        return True
    except Exception as e:
        print(f"[DL] fail {doc_id}: {e}")
        return False

# ================== LƯU MỘT VĂN BẢN ==================
def doc_id_from_meta(meta: Dict[str, str], url: str) -> str:
    so_hieu = (meta.get("Số hiệu") or "").strip()
    if so_hieu:
        return safe_name(so_hieu)
    tid = tail_numeric_id(url)
    if tid:
        return f"id_{tid}"
    return make_fallback_id(url)

def build_doc_json(meta: OrderedDict, sections: Dict[str, List[Dict[str, str]]], content_conn: List[Dict[str, str]], url: str) -> dict:
    return {
        "source_url": url,
        "meta": meta,
        "relations_sections": sections,
        "content_connection": content_conn,
    }

def save_document_record(out_dir: Path, doc_json: dict, doc_id: str):
    path = out_dir / "docs" / f"{doc_id}.json"
    save_json(path, doc_json)
    return path

# ================== THU URL MỚI ==================
def harvest_new_urls(sections: Dict[str, List[Dict[str, str]]], content_conn: List[Dict[str, str]]) -> List[str]:
    urls = []
    for _, arr in sections.items():
        for it in arr:
            u = (it.get("url") or "").strip()
            if u and url_in_domain(u):
                urls.append(u)
    for it in content_conn:
        u = (it.get("url") or "").strip()
        if u and url_in_domain(u):
            urls.append(u)

    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

# ================== CHECKPOINT ==================
def load_checkpoint():
    ensure_dirs()
    cp_seen  = OUTPUT_DIR / "checkpoints" / "seen_ids.json"
    cp_queue = OUTPUT_DIR / "checkpoints" / "queue.json"
    if cp_seen.exists():
        seen_ids = set(json.loads(cp_seen.read_text(encoding="utf-8")))
    else:
        seen_ids = set()
    if cp_queue.exists():
        queue = deque(json.loads(cp_queue.read_text(encoding="utf-8")))
    else:
        queue = deque()
    return seen_ids, queue

def save_checkpoint(seen_ids: set, queue: deque):
    cp_seen  = OUTPUT_DIR / "checkpoints" / "seen_ids.json"
    cp_queue = OUTPUT_DIR / "checkpoints" / "queue.json"
    cp_seen.write_text(json.dumps(sorted(list(seen_ids)), ensure_ascii=False, indent=2), encoding="utf-8")
    cp_queue.write_text(json.dumps(list(queue), ensure_ascii=False, indent=2), encoding="utf-8")

# ================== CHỜ ĐĂNG NHẬP ==================
async def wait_for_manual_login(page: Page):
    print("\n=== MỞ TRÌNH DUYỆT ĐỂ BẠN ĐĂNG NHẬP ===")
    print("Đăng nhập trên cửa sổ Chromium vừa mở.")
    print("Xong quay lại terminal và nhấn Enter để tiếp tục...\n")
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, input, ">> Nhấn Enter khi đã đăng nhập xong: ")

# ================== MAIN CRAWL LOOP ==================
async def crawl_bfs(seed_url: str):
    ensure_dirs()
    seen_ids, queue = load_checkpoint()
    if not queue:
        queue.append(seed_url)

    async with async_playwright() as p:
        if AUTH_STATE_PATH.exists():
            browser = await p.chromium.launch(headless=HEADLESS)
            context = await browser.new_context(
                viewport=VIEWPORT,
                storage_state=str(AUTH_STATE_PATH),
                accept_downloads=True,
            )
        else:
            browser = await p.chromium.launch(headless=HEADLESS)
            context = await browser.new_context(
                viewport=VIEWPORT,
                accept_downloads=True,
            )

        page = await context.new_page()

        if not AUTH_STATE_PATH.exists():
            await page.goto(seed_url, wait_until="domcontentloaded", timeout=60000)
            await wait_for_manual_login(page)
            state = await context.storage_state()
            AUTH_STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[AUTH] Đã lưu phiên vào {AUTH_STATE_PATH}")

        while queue:
            url = queue.popleft()
            if not url_in_domain(url):
                continue

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            except Exception as e:
                print(f"[SKIP] goto fail: {url} — {e}")
                save_checkpoint(seen_ids, queue)
                continue

            try:
                meta, sections, content_conn = await scrape_full_tab4(page)
            except Exception as e:
                print(f"[WARN] scrape fail: {url} — {e}")
                save_checkpoint(seen_ids, queue)
                continue

            doc_id = doc_id_from_meta(meta, url)
            if doc_id in seen_ids:
                print(f"[DUP] {doc_id} -> {url}")
                save_checkpoint(seen_ids, queue)
                continue

            record = build_doc_json(meta, sections, content_conn, url)
            path = save_document_record(OUTPUT_DIR, record, doc_id)
            seen_ids.add(doc_id)
            print(f"[OK] saved {doc_id} -> {path.name}")

            # tải .doc
            await download_vietnamese_doc(page, doc_id)

            # enqueue URL mới
            new_urls = harvest_new_urls(sections, content_conn)
            for u in new_urls:
                if u not in queue:
                    queue.append(u)

            save_checkpoint(seen_ids, queue)

        await browser.close()

# ================== CHẠY ==================
if __name__ == "__main__":
    asyncio.run(crawl_bfs(SEED_URL))
