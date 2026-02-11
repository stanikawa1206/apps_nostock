import subprocess
import os

def launch():
    # 1. ローカル側の起動コマンド
    # 「start」を使うことで、新しいウィンドウでバッチを実行します
    local_cmd = 'start "LOCAL_CHECK" /d "D:\\apps_nostock\\apps\\inventory" check_remaining_ebay.bat'

    # 2. VPS側の起動コマンド (先ほど成功したsshコマンド)
    # こちらも「start」で別ウィンドウにします
    vps_cmd = (
        'start "VPS_CHECK" ssh -tt root@162.43.42.135 '
        '"cd /opt/apps_nostock && git pull && cd /opt/apps_nostock/apps/inventory && '
        'chmod +x check_remaining_ebay.sh && ./check_remaining_ebay.sh"'
    )

    print("ローカルとVPSの両方で在庫チェックを開始します...")

    # 同時に実行
    subprocess.Popen(local_cmd, shell=True)
    subprocess.Popen(vps_cmd, shell=True)

    print("2つのウィンドウが立ち上がりました。")

if __name__ == "__main__":
    launch()