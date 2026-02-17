from pathlib import Path
import sys
import subprocess

PYTHON = sys.executable
BASE_DIR = Path("D:/apps_nostock")

DELETE_SCRIPT = BASE_DIR / "apps" / "publish" / "delete_ebay_daily.py"

module_path = str(DELETE_SCRIPT.relative_to(BASE_DIR)) \
    .replace("\\", ".") \
    .replace("/", ".")
module_path = module_path[:-3]  # .py を削除

result = subprocess.run(
    [PYTHON, "-m", module_path],
    cwd=str(BASE_DIR),
    capture_output=True,
    text=True,
    encoding="utf-8",   # ★ これを追加
)

print("RETURN CODE:", result.returncode)
print("STDOUT:\n", result.stdout)
print("STDERR:\n", result.stderr)
