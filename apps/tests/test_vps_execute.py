import subprocess
import os

def run_remote_sh():
    # VPSの接続情報
    vps_user = "root"
    vps_ip = "162.43.42.135"
    remote_dir = "/opt/apps_nostock/apps/inventory"
    sh_filename = "check_remaining_ebay.sh"

    # 実行するコマンドの組み立て
    # 1. ディレクトリへ移動
    # 2. 最新コードをプル (念のためルートで実行)
    # 3. .sh を実行
    remote_cmd = (
        f"cd /opt/apps_nostock && git pull && "
        f"cd {remote_dir} && chmod +x {sh_filename} && ./{sh_filename}"
    )

    # SSHコマンドの構成
    # -tt: 擬似ターミナルを強制割り当て（ログをリアルタイム表示するため）
    ssh_cmd = [
        "ssh", "-tt", f"{vps_user}@{vps_ip}",
        remote_cmd
    ]

    print(f"--- 接続開始: {vps_ip} ---")
    
    try:
        # プロセスを実行し、標準出力をそのまま表示
        process = subprocess.Popen(ssh_cmd, shell=True)
        process.wait()
    except KeyboardInterrupt:
        print("\n--- ローカル側で中断されました ---")
    except Exception as e:
        print(f"--- エラー発生: {e} ---")

if __name__ == "__main__":
    run_remote_sh()