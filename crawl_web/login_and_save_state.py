# login_and_save_state.py
# Chạy file này TRƯỚC để đăng nhập tay và lưu state.json

from playwright.sync_api import sync_playwright

LOGIN_URL = "https://luatvietnam.vn/"
STATE_FILE = "state.json"

def main():
    with sync_playwright() as p:
        # mở có giao diện để bạn bấm
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        print("[INFO] Mở trang để bạn đăng nhập ...")
        page.goto(LOGIN_URL)

        print("[ACTION] Hãy tự đăng nhập trong cửa sổ vừa mở (bấm Đăng nhập, nhập tài khoản...).")
        input("[ACTION] Đăng nhập xong thì quay lại cửa sổ này và bấm Enter để lưu state: ")

        # lưu cookie + localStorage
        context.storage_state(path=STATE_FILE)
        print(f"[OK] Đã lưu phiên đăng nhập vào {STATE_FILE}")

        browser.close()

if __name__ == "__main__":
    main()
