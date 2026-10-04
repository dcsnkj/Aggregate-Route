#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Codex 聚合路由的看门狗 —— 路由挂了就自动拉回来，用户不用管。

每 60 秒探一次 127.0.0.1:8788：
  通  -> 什么都不做
  不通 -> 后台拉起 router.py（脱离父进程），并写一行日志

由 Startup 目录里的 `Codex模型路由.cmd` 在登录时启动，之后一直活着。
成本：每 60 秒一次 socket 探测（0.2 秒），几乎为零。
"""
import socket
import subprocess
import sys
import time
from pathlib import Path

HOST, PORT = "127.0.0.1", 8788
INTERVAL = 60
HERE = Path(__file__).resolve().parent
ROUTER = HERE / "codex_router.py"
LOG = Path.home() / ".codex" / "logs" / "model-router.log"
HEARTBEAT = Path.home() / ".codex" / "logs" / "watchdog.heartbeat"

if sys.stdout is None:
    import os
    sys.stdout = open(os.devnull, "w", encoding="utf-8")


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


def log(msg, event="watchdog"):
    try:
        LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(f'{{"ts": "{time.strftime("%Y-%m-%d %H:%M:%S")}", '
                    f'"event": "{event}", "msg": "{msg}"}}\n')
    except Exception:
        pass


def start_router():
    if not ROUTER.exists():
        log(f"router.py 不存在: {ROUTER}")
        return False
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
        return False
    for _ in range(24):
        if up():
            log("路由掉线，已自动拉起")
            return True
        time.sleep(0.25)
    log("拉起了但端口一直不通")
    return False


def main():
    log(f"看门狗启动（每 {INTERVAL}s 探一次）", event="watchdog_start")
    if not up():
        start_router()
    while True:
        # 心跳文件：router.py --doctor 靠它的更新时间判断看门狗是否还活着
        try:
            HEARTBEAT.parent.mkdir(parents=True, exist_ok=True)
            HEARTBEAT.write_text(str(int(time.time())), encoding="utf-8")
        except Exception:
            pass
        time.sleep(INTERVAL)
        if not up():
            start_router()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
