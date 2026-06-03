import socket
import subprocess
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

TARGET_URL = "https://jp.mercari.com/mypage/purchases"


def _is_debug_port_open(host: str = "127.0.0.1", port: int = 9222) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex((host, port)) == 0


def start_chrome():
    if _is_debug_port_open():
        # Chrome がすでにデバッグポート付きで起動済みならそのまま使う
        print("Chrome debug port 9222 already open, reusing existing instance.")
        return

    # 同じプロファイルで Chrome が起動中だとシングルインスタンス機構により
    # --remote-debugging-port が無視されるため、先に終了させる
    subprocess.run(["taskkill", "/F", "/IM", "chrome.exe"], capture_output=True)
    time.sleep(1)

    subprocess.Popen([
        CHROME_PATH,
        "--remote-debugging-port=9222",
        r"--user-data-dir=C:\Users\stani\AppData\Local\Google\Chrome\User Data",
        "--profile-directory=Default",
        "--no-first-run",
        "--no-default-browser-check",
    ])

    # ポートが開くまで最大10秒待機
    for _ in range(10):
        time.sleep(1)
        if _is_debug_port_open():
            return
    raise RuntimeError("Chrome debug port 9222 did not open within 10 seconds.")


def main():
    
    print("0")
    start_chrome()
    print("0.5")

    options = Options()
    options.debugger_address = "127.0.0.1:9222"

    print("1")
    driver = webdriver.Chrome(options=options)

    print("2")

    driver.get(TARGET_URL)

    print("3")

    time.sleep(5)

    print("4")

    print(driver.current_url)
    print(driver.title)


if __name__ == "__main__":
    main()