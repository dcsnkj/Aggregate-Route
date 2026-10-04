#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""确保 Codex 聚合路由在跑 —— 给 Codex 的 SessionStart 钩子用。

行为：先探端口。已经在跑 → 立刻退出（0，什么都不做）。
      没在跑 → 后台拉起 router.py（脱离父进程，不占住钩子），等它起来后退出。

所以它是幂等的：Codex 每次开会话都调一次，成本只有一次 0.2 秒的端口探测。
"""
import socket
import subprocess
import sys
import time
from pathlib import Path

HOST, PORT = "127.0.0.1", 8788
HERE = Path(__file__).resolve().parent
ROUTER = HERE / "codex_router.py"


def up():
    s = socket.socket()
    s.settimeout(0.4)
    try:
        s.connect((HOST, PORT))
        return True
    except OSError:
        return False
    finally:
        try:
            s.close()
        except Exception:
            pass


def log(msg):
    try:
        p = Path.home() / ".codex" / "logs" / "model-router.log"
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "a", encoding="utf-8") as f:
            f.write(f'{{"ts": "{time.strftime("%Y-%m-%d %H:%M:%S")}", '
                    f'"event": "ensure", "msg": "{msg}"}}\n')
    except Exception:
        pass


def main():
    if up():
        return 0
    if not ROUTER.exists():
        log(f"router.py 不存在: {ROUTER}")
        return 0
    # 用 pythonw 起，避免弹控制台；DETACHED 让它跟钩子进程完全脱钩
    py = sys.executable
    if py.lower().endswith("python.exe"):
        cand = py[:-len("python.exe")] + "pythonw.exe"
        if Path(cand).exists():
            py = cand
    flags = 0x00000008 | 0x00000200          # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
    try:
        subprocess.Popen([py, str(ROUTER)], cwd=str(HERE), creationflags=flags,
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, close_fds=True)
    except Exception as e:
        log(f"启动失败: {type(e).__name__}: {e}")
        return 0
    for _ in range(24):                       # 最多等 6 秒
        if up():
            log("已自动拉起路由")
            return 0
        time.sleep(0.25)
    log("拉起了但端口一直没通")
    return 0


if __name__ == "__main__":
    sys.exit(main())
