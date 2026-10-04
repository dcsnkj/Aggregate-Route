#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Codex 本地模型路由 —— 一个 base_url 走遍多个网关与多个 key。

Codex 只认一个 provider（指向本机 http://127.0.0.1:8788/v1），
本服务按请求里的 model 名把请求转发到对应上游，并在上游失败时依次尝试：
    同一上游的下一个 key  ->  路由里的下一个候选上游

用法：
    python codex_router.py                 # 起服务（默认 127.0.0.1:8788）
    python codex_router.py --port 8788
    python codex_router.py --check         # 只做自检：逐个候选发一次真实请求，不常驻
    python codex_router.py --routes        # 打印路由表
    python codex_router.py --keys          # 打印各上游的 key 数量与冷却状态

配置：~/.codex/codex-model-router.json（由 setup_codex_router.py 生成）
日志：~/.codex/logs/model-router.log（每次请求一行 JSON，含跳过了谁、用了哪个 key）
"""
import argparse
import collections
import itertools
import json
import os
import queue
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

CODEX_HOME = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex"))
CONFIG_PATH = Path(os.environ.get("CODEX_ROUTER_CONFIG", CODEX_HOME / "codex-model-router.json"))
LOG_PATH = CODEX_HOME / "logs" / "model-router.log"

# pythonw.exe 起服务时没有 stdout，print() 会炸 —— 换成黑洞
if sys.stdout is None:
    sys.stdout = open(os.devnull, "w", encoding="utf-8")
if sys.stderr is None:
    sys.stderr = open(os.devnull, "w", encoding="utf-8")

# 这些状态码说明「这个 key / 这个上游现在服务不了我」，值得换下一个。
# 把 401/403/404 也算进来：额度耗尽(403)、key 失效/被封(401)、
# 模型在这家不支持但另一家支持(404)，都属于"换一个试试"的典型场景。
RETRYABLE_STATUS = {401, 403, 404, 408, 409, 425, 429,
                    500, 502, 503, 504, 520, 521, 522, 523, 524}
# 这些是「请求本身有问题」，换哪家都一样，直接原样回给 Codex
PASSTHROUGH_STATUS = {400, 413, 422}
# 上游返回的正文里带这些字样，也说明这家当前不可用
RETRYABLE_TEXT = ("get_channel_failed", "负载已经达到上限", "rate limit", "overloaded",
                  "额度不足", "余额不足", "insufficient", "quota", "balance", "暂时不可用",
                  "用户已被封禁", "无效的令牌")

STREAM_TIMEOUT = 180           # 单次 socket 读超时（秒）——上游长时间不说话就放弃
DEFAULT_HEADER_TIMEOUT = 30    # 等响应头的上限；超了就当这个候选不行，换下一个
                               # （anyrouter 满载有时要 81 秒才回 500，靠这个跳走）

# key 冷却：某个 key 刚失败过就先别用它，避免每个请求都白撞一次
COOLDOWN_SECONDS = {
    "quota": 900,      # 额度不足 / 被封禁 —— 短时间内不会自愈
    "ratelimit": 120,  # 429 / rate limit
    "other": 60,       # 5xx、超时等
}
_QUOTA_MARKERS = ("额度不足", "余额不足", "insufficient", "quota", "balance", "封禁", "无效的令牌")
_RATE_MARKERS = ("rate limit", "429", "too many requests")

_CFG = {"upstreams": {}, "routes": {}, "listen": "127.0.0.1:8788", "default": None}
_CFG_MTIME = 0
_RR = collections.defaultdict(itertools.count)      # 每个上游的轮询起点
_COOLDOWN = {}                                      # (upstream, key) -> 解除时间戳


def log(event, **kw):
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps({"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "event": event, **kw},
                          ensure_ascii=False)
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def load_config(force=False):
    """读配置；文件变了就热加载（改完不用重启）。"""
    global _CFG, _CFG_MTIME
    try:
        mt = CONFIG_PATH.stat().st_mtime
    except OSError:
        return _CFG
    if not force and mt == _CFG_MTIME and _CFG["routes"]:
        return _CFG
    try:
        _CFG = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        _CFG_MTIME = mt
        log("config_loaded", path=str(CONFIG_PATH), routes=len(_CFG.get("routes", {})),
            upstreams={k: len(keys_of(v)) for k, v in _CFG.get("upstreams", {}).items()})
    except Exception as e:
        log("config_error", error=f"{type(e).__name__}: {e}")
    return _CFG


def keys_of(up):
    """兼容两种写法：keys 数组（多 key）或单个 key。"""
    ks = up.get("keys")
    if isinstance(ks, list) and ks:
        return [k for k in ks if k]
    k = up.get("key")
    return [k] if k else []


def short(k):
    return "…" + k[-6:] if k else "-"


def opener_for(up):
    """按上游配置造 opener。

    注意：必须显式给 ProxyHandler —— 不传会继承环境变量里的 http_proxy，
    那样"直连"其实走了代理，行为跟预期不一致。
    """
    proxy = up.get("proxy")
    if proxy:
        return urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
    return urllib.request.build_opener(urllib.request.ProxyHandler({}))


def protocol_of(up):
    """上游协议：responses（默认，原样透传）或 chat（需要翻译层）。"""
    return (up.get("protocol") or "responses").lower()


def build_request(up, model, body, key):
    """按上游协议造请求。

    - responses：请求体原样转发到 <base>/responses
    - chat：把 Responses 形状翻成 Chat 形状，打到 <base>/chat/completions
      （Codex 只认 responses，但很多网关只有 chat —— 见 chat_bridge.py）
    """
    proto = protocol_of(up)
    if proto == "chat":
        from chat_bridge import responses_to_chat
        payload = responses_to_chat(dict(body, model=model))
        path = "/chat/completions"
    else:
        payload = dict(body)
        payload["model"] = model
        path = "/responses"
    headers = {
        "content-type": "application/json",
        "accept": "text/event-stream",
        "authorization": "Bearer " + key,
        "originator": "codex_exec",
        "user-agent": "codex_cli_rs/0.160.0 (Windows 10.0.19045; x86_64) codex_exec",
    }
    url = up["base_url"].rstrip("/") + path
    return urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"),
                                 headers=headers, method="POST")


def call_upstream(up, model, body, key, header_timeout):
    """发一次请求，返回 (kind, obj)。

    kind: 'ok'(拿到响应头) / 'http'(HTTPError) / 'net'(连接层错误) / 'slow'(等响应头超时)
    在后台线程里等响应头，主线程最多等 header_timeout —— 超时就直接放弃这个候选，
    不用陪着上游傻等 80 秒。
    """
    req = build_request(up, model, body, key)
    q = queue.Queue()

    def work():
        try:
            q.put(("ok", opener_for(up).open(req, timeout=STREAM_TIMEOUT)))
        except urllib.error.HTTPError as e:
            q.put(("http", e))
        except Exception as e:
            q.put(("net", e))

    threading.Thread(target=work, daemon=True).start()
    try:
        return q.get(timeout=header_timeout)
    except queue.Empty:
        return ("slow", None)


def peek_error_body(resp):
    try:
        return resp.read(2000).decode("utf-8", "replace")
    except Exception:
        return ""


def cool_down_kind(status, text):
    if any(s in text for s in _QUOTA_MARKERS):
        return "quota"
    if status == 403:
        return "quota"
    if status == 429 or any(s in text for s in _RATE_MARKERS):
        return "ratelimit"
    return "other"


def order_keys(up_short, keys):
    """轮询起点 + 冷却优先：正常的排前面，冷却中的垫底（全冷却时仍会逐个试）。"""
    if len(keys) == 1:
        return list(keys)
    start = next(_RR[up_short]) % len(keys)
    rotated = keys[start:] + keys[:start]
    now = time.time()
    fresh = [k for k in rotated if _COOLDOWN.get((up_short, k), 0) <= now]
    cooling = [k for k in rotated if _COOLDOWN.get((up_short, k), 0) > now]
    return fresh + cooling


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "codex-router/1"

    def log_message(self, *a):
        pass

    # ---------- 工具 ----------
    def _json(self, status, obj):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json; charset=utf-8")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _chunk(self, data):
        self.wfile.write(b"%X\r\n" % len(data) + data + b"\r\n")

    # ---------- 路由 ----------
    def do_GET(self):
        path = self.path.split("?")[0].rstrip("/")
        cfg = load_config()
        if path in ("/healthz", "/v1/healthz"):
            self._json(200, {
                "ok": True,
                "routes": len(cfg.get("routes", {})),
                "upstreams": {k: {"keys": len(keys_of(v)), "protocol": protocol_of(v)}
                              for k, v in cfg.get("upstreams", {}).items()},
            })
            return
        if path.endswith("/models"):
            models = [{"id": s, "object": "model", "owned_by": (r["candidates"][0]["upstream"]
                      if r.get("candidates") else "router")}
                      for s, r in cfg.get("routes", {}).items()]
            self._json(200, {"object": "list", "data": models})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self):
        path = self.path.split("?")[0].rstrip("/")
        if not path.endswith("/responses"):
            self._json(404, {"error": f"unsupported path {self.path}"})
            return
        n = int(self.headers.get("content-length") or 0)
        raw = self.rfile.read(n) if n else b""
        try:
            body = json.loads(raw.decode("utf-8"))
        except Exception as e:
            self._json(400, {"error": {"message": f"bad json: {e}", "type": "router_error"}})
            return

        cfg = load_config()
        slug = body.get("model") or cfg.get("default")
        route = cfg.get("routes", {}).get(slug)
        if not route:
            # 没登记过的模型：按 default 的路由兜底，但把模型名原样传下去
            route = cfg.get("routes", {}).get(cfg.get("default") or "")
            if route:
                route = {"candidates": [dict(c, model=slug) for c in route["candidates"]]}
        if not route:
            self._json(404, {"error": {"message": f"模型 '{slug}' 没有路由规则",
                                       "type": "router_error"}})
            return

        header_timeout = float(route.get("header_timeout") or cfg.get("header_timeout")
                               or DEFAULT_HEADER_TIMEOUT)
        t0 = time.time()
        tried = []
        last_err = None       # 记最后一个 HTTP 错误，全都失败时把它原样透给 Codex

        for cand in route["candidates"]:
            up_short = cand["upstream"]
            up = cfg.get("upstreams", {}).get(up_short)
            if not up:
                tried.append({"upstream": up_short, "error": "上游未配置"})
                continue
            keys = keys_of(up)
            if not keys:
                tried.append({"upstream": up_short, "error": "没有可用 key"})
                continue

            for key in order_keys(up_short, keys):
                kind, obj = call_upstream(up, cand["model"], body, key, header_timeout)
                tag = f"{up_short}/{cand['model']}@{short(key)}"

                if kind == "ok":
                    status = obj.status
                    if status != 200:
                        txt = peek_error_body(obj)
                        obj.close()
                        tried.append({"cand": tag, "status": status, "body": txt[:200],
                                      "elapsed": round(time.time() - t0, 1)})
                        if status in PASSTHROUGH_STATUS:
                            self._json(status, self._safe_json(txt))
                            log("passthrough_error", model=slug, tried=tried)
                            return
                        self._cool(up_short, key, status, txt)
                        last_err = (status, txt)
                        continue
                    # 200 也要闻一下头几个字节：有些网关用 200 + SSE 错误事件报满载
                    first = b""
                    try:
                        first = obj.read1(8192) if hasattr(obj, "read1") else obj.read(8192)
                    except Exception:
                        first = b""
                    head_txt = first.decode("utf-8", "replace")
                    if any(s in head_txt for s in RETRYABLE_TEXT):
                        tried.append({"cand": tag, "status": 200, "body": head_txt[:200],
                                      "elapsed": round(time.time() - t0, 1)})
                        obj.close()
                        self._cool(up_short, key, 200, head_txt)
                        continue
                    _COOLDOWN.pop((up_short, key), None)     # 这个 key 是好的
                    if protocol_of(up) == "chat":
                        self._stream_bridge(obj, tag, tried, t0, first, body)
                    else:
                        self._stream_back(obj, tag, tried, t0, first)
                    return

                if kind == "http":
                    status = obj.code
                    txt = peek_error_body(obj)
                    tried.append({"cand": tag, "status": status, "body": txt[:200],
                                  "elapsed": round(time.time() - t0, 1)})
                    if status in PASSTHROUGH_STATUS:
                        self._json(status, self._safe_json(txt))
                        log("passthrough_error", model=slug, tried=tried)
                        return
                    self._cool(up_short, key, status, txt)
                    last_err = (status, txt)
                    continue

                if kind == "slow":
                    tried.append({"cand": tag, "error": f"等响应头超过 {header_timeout:.0f}s",
                                  "elapsed": round(time.time() - t0, 1)})
                    self._cool(up_short, key, 504, "timeout")
                    continue

                tried.append({"cand": tag, "error": f"{type(obj).__name__}: {obj}",
                              "elapsed": round(time.time() - t0, 1)})
                self._cool(up_short, key, 502, "network")
                continue

        log("all_failed", model=slug, tried=tried)
        if last_err:
            # 把上游最后那个真实错误透出去，比笼统的 502 有用得多
            status, txt = last_err
            body = self._safe_json(txt)
            if isinstance(body, dict) and "error" in body:
                body.setdefault("router", {})["tried"] = [t["cand"] for t in tried if "cand" in t]
            self._json(status, body)
            return
        self._json(502, {"error": {
            "message": f"模型 '{slug}' 的所有候选上游都失败了",
            "type": "router_error", "detail": tried}})

    # ---------- 输出 ----------
    @staticmethod
    def _cool(up_short, key, status, text):
        kind = cool_down_kind(status, text)
        _COOLDOWN[(up_short, key)] = time.time() + COOLDOWN_SECONDS[kind]
        log("cooldown", upstream=up_short, key=short(key), kind=kind,
            seconds=COOLDOWN_SECONDS[kind])

    @staticmethod
    def _safe_json(txt):
        try:
            return json.loads(txt)
        except Exception:
            return {"error": {"message": txt[:500], "type": "upstream_error"}}

    def _stream_bridge(self, resp, tag, tried, t0, first, body):
        """上游是 chat 协议：把它的 SSE 逐块翻成 Responses 事件再发给 Codex。"""
        from chat_bridge import ChatStreamBridge
        br = ChatStreamBridge(body.get("model") or "",
                              instructions=body.get("instructions"),
                              parallel_tool_calls=bool(body.get("parallel_tool_calls", True)))
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("cache-control", "no-cache")
        self.send_header("transfer-encoding", "chunked")
        self.end_headers()

        def feed_line(line):
            line = line.strip()
            if not line.startswith(b"data:"):
                return b""
            payload = line[5:].strip()
            if payload == b"[DONE]":
                return br.finish()
            try:
                d = json.loads(payload)
            except Exception:
                return b""
            return br.feed(d)

        total = 0
        try:
            out0 = br.start()
            self._chunk(out0)
            self.wfile.flush()
            total += len(out0)

            buf = first or b""
            done = False
            while not done:
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    out = feed_line(line)
                    if out:
                        self._chunk(out)
                        self.wfile.flush()
                        total += len(out)
                chunk = resp.read1(65536) if hasattr(resp, "read1") else resp.read(65536)
                if not chunk:
                    if buf.strip():
                        out = feed_line(buf)
                        if out:
                            self._chunk(out)
                            self.wfile.flush()
                            total += len(out)
                    done = True
                else:
                    buf += chunk

            out = br.finish()
            self._chunk(out)
            self.wfile.flush()
            total += len(out)
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
            log("ok", via=tag, protocol="chat", bytes=total,
                elapsed=round(time.time() - t0, 1), finish=br.finish_reason,
                reasoning_chars=br.reasoning_chars,
                skipped=[t.get("cand") or t.get("upstream") for t in tried] or None)
        except (BrokenPipeError, ConnectionResetError):
            log("client_gone", via=tag, protocol="chat", bytes=total)
        except Exception as e:
            log("stream_error", via=tag, protocol="chat",
                error=f"{type(e).__name__}: {e}", bytes=total)
        finally:
            try:
                resp.close()
            except Exception:
                pass

    def _stream_back(self, resp, tag, tried, t0, first=b""):
        ctype = resp.headers.get("content-type") or "text/event-stream"
        self.send_response(200)
        self.send_header("content-type", ctype)
        self.send_header("cache-control", "no-cache")
        self.send_header("transfer-encoding", "chunked")
        self.end_headers()
        total = 0
        try:
            if first:
                total += len(first)
                self._chunk(first)
                self.wfile.flush()
            while True:
                chunk = resp.read1(65536) if hasattr(resp, "read1") else resp.read(65536)
                if not chunk:
                    break
                total += len(chunk)
                self._chunk(chunk)
                self.wfile.flush()
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
            log("ok", via=tag, bytes=total, elapsed=round(time.time() - t0, 1),
                skipped=[t.get("cand") or t.get("upstream") for t in tried] or None)
        except (BrokenPipeError, ConnectionResetError):
            log("client_gone", via=tag, bytes=total)
        except Exception as e:
            log("stream_error", via=tag, error=f"{type(e).__name__}: {e}", bytes=total)
        finally:
            try:
                resp.close()
            except Exception:
                pass


# ---------- 自检 ----------
CHECK_BODY = {"instructions": "be brief",
              "input": [{"type": "message", "role": "user",
                         "content": [{"type": "input_text", "text": "hi"}]}],
              "tools": [], "tool_choice": "auto", "parallel_tool_calls": True,
              "store": False, "stream": True, "include": ["reasoning.encrypted_content"],
              "prompt_cache_key": "router-check", "text": {"verbosity": "medium"},
              "reasoning": {"effort": "low"}}


def probe_once(up, model, key, timeout=DEFAULT_HEADER_TIMEOUT):
    kind, obj = call_upstream(up, model, CHECK_BODY, key, timeout)
    if kind == "ok":
        txt = ""
        if obj.status != 200:
            txt = peek_error_body(obj)[:90]
        code = obj.status
        obj.close()
        return code, txt
    if kind == "http":
        return obj.code, peek_error_body(obj)[:90]
    if kind == "slow":
        return "TIMEOUT", f"等响应头超时 {timeout}s"
    return "NET", f"{type(obj).__name__}: {obj}"


def check(cfg):
    print(f"{'模型':<24} {'候选':<34} 结果")
    print("-" * 92)
    ok = True
    for slug, route in cfg.get("routes", {}).items():
        for i, cand in enumerate(route["candidates"]):
            up = cfg["upstreams"].get(cand["upstream"])
            if not up:
                continue
            for j, key in enumerate(keys_of(up)):
                code, txt = probe_once(up, cand["model"], key,
                                       float(route.get("header_timeout") or DEFAULT_HEADER_TIMEOUT))
                good = code == 200
                if not good:
                    ok = False
                head = f"{slug:<24} " if (i == 0 and j == 0) else " " * 25
                ptag = "" if protocol_of(up) == "responses" else " (chat 桥接)"
                print(f"{head}{cand['upstream']}/{cand['model']}@{short(key):<10} "
                      f"{'OK  ' if good else '!!  '}HTTP {code} {txt}{ptag}")
    print()
    print("全部可用 ✅" if ok else "有候选不可用（上面带 !! 的）—— 可能是临时满载/欠费，稍后再试")
    return 0 if ok else 1


def doctor(cfg):
    """一键体检：该看的都看一遍，不用记数字。"""
    ok = True
    host, _, port = (cfg.get("listen") or "127.0.0.1:8788").rpartition(":")
    port = int(port or 8788)

    s = socket.socket()
    s.settimeout(0.6)
    try:
        s.connect((host or "127.0.0.1", port))
        print(f"  路由服务      ✅ 在跑（{host}:{port}）")
    except OSError:
        print(f"  路由服务      ❌ 没在跑 —— 双击 Startup 目录里的 Codex模型路由.cmd")
        ok = False
    finally:
        try:
            s.close()
        except Exception:
            pass

    hb = CODEX_HOME / "logs" / "watchdog.heartbeat"
    if hb.exists():
        age = time.time() - hb.stat().st_mtime
        if age < 180:
            print(f"  看门狗        ✅ 在跑（{age:.0f}s 前的心跳）")
        else:
            print(f"  看门狗        ⚠️ 心跳停在 {age / 60:.0f} 分钟前 —— 它挂了路由就没人看着了")
            ok = False
    else:
        print("  看门狗        ⚠️ 没有心跳文件 —— 从没以独立方式启动过看门狗")
        ok = False

    expected = cfg.get("expected_keys") or {}
    for name, up in cfg.get("upstreams", {}).items():
        n = len(keys_of(up))
        exp = expected.get(name)
        proto = protocol_of(up)
        ptag = "" if proto == "responses" else f"[{proto} 桥接]"
        if exp and n < exp:
            print(f"  上游 {name:<9} ⚠️ {n} 把 key（上次装配时是 {exp} 把，少了 {exp - n} 把）{ptag}")
            ok = False
        else:
            tail = f"（上次装配 {exp} 把）" if exp else ""
            print(f"  上游 {name:<9} ✅ {n} 把 key {tail}{ptag}")

    cat = CODEX_HOME / "codex-router-catalog.json"
    if cat.exists():
        try:
            models = json.loads(cat.read_text(encoding="utf-8")).get("models", [])
            print(f"  模型选择器    ✅ {len(models)} 个：{[m['slug'] for m in models]}")
        except Exception as e:
            print(f"  模型选择器    ⚠️ 读不出来: {e}")
            ok = False
    else:
        print("  模型选择器    ⚠️ 目录文件不存在")
        ok = False

    if LOG_PATH.exists():
        recent = []
        for line in LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()[-400:]:
            try:
                d = json.loads(line)
            except Exception:
                continue
            if d.get("event") == "all_failed":
                try:
                    if time.time() - time.mktime(time.strptime(d["ts"], "%Y-%m-%d %H:%M:%S")) < 3600:
                        recent.append(d)
                except Exception:
                    pass
        if recent:
            print(f"  近 1 小时      ⚠️ 有 {len(recent)} 次「所有候选都失败」：")
            for d in recent[-2:]:
                print(f"      {d['ts']} {d.get('model')} 试过 "
                      f"{[t.get('cand') for t in d.get('tried', []) if t.get('cand')]}")
            ok = False
        else:
            print("  近 1 小时      ✅ 没有「所有候选都失败」的记录")

    print()
    print("体检通过 ✅" if ok else "上面带 ⚠️ / ❌ 的项需要处理")
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--check", action="store_true", help="逐个候选+key 自检，不常驻")
    ap.add_argument("--routes", action="store_true", help="打印路由表")
    ap.add_argument("--keys", action="store_true", help="打印各上游 key 数与冷却状态")
    ap.add_argument("--doctor", action="store_true", help="体检：路由/看门狗/key 数/目录/近期失败")
    a = ap.parse_args()

    cfg = load_config(force=True)
    if not cfg.get("routes"):
        print(f"配置读不到或为空: {CONFIG_PATH}", file=sys.stderr)
        return 2

    if a.check:
        return check(cfg)

    if a.routes:
        for slug, r in cfg["routes"].items():
            chain = " -> ".join(f"{c['upstream']}/{c['model']}" for c in r["candidates"])
            print(f"  {slug:<24} {chain}")
        return 0

    if a.keys:
        now = time.time()
        for name, up in cfg["upstreams"].items():
            ks = keys_of(up)
            print(f"  {name:<11} {up['base_url']:<32} {len(ks)} 个 key")
            for k in ks:
                left = _COOLDOWN.get((name, k), 0) - now
                state = f"冷却中，还剩 {left:.0f}s" if left > 0 else "正常"
                print(f"      {short(k):<10} {state}")
        return 0

    if a.doctor:
        return doctor(cfg)

    host, _, port = (cfg.get("listen") or "127.0.0.1:8788").rpartition(":")
    port = a.port or int(port or 8788)
    host = host or "127.0.0.1"

    # 已经在跑就不再起第二个实例（让 start.cmd 可以随手双击）
    probe = socket.socket()
    probe.settimeout(0.4)
    try:
        probe.connect((host, port))
        print(f"路由已在运行: http://{host}:{port}/v1  （无需重复启动）")
        return 0
    except OSError:
        pass
    finally:
        try:
            probe.close()
        except Exception:
            pass

    ThreadingHTTPServer.allow_reuse_address = True
    srv = ThreadingHTTPServer((host, port), Handler)
    print(f"Codex 模型路由已启动: http://{host}:{port}/v1")
    print(f"  路由 {len(cfg['routes'])} 条")
    for name, up in cfg["upstreams"].items():
        print(f"  {name:<11} {up['base_url']:<32} {len(keys_of(up))} 个 key")
    print(f"  日志 {LOG_PATH}")
    print("  Ctrl+C 停止")
    log("started", listen=f"{host}:{port}", routes=len(cfg["routes"]),
        upstreams={k: len(keys_of(v)) for k, v in cfg["upstreams"].items()})
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")
    return 0


if __name__ == "__main__":
    sys.exit(main())
