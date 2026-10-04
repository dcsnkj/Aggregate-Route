#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""网关「模型 × 协议」可用性矩阵。

回答的问题：这个网关到底哪些模型能用、分别能用在**哪条协议**上。
- /v1/responses   —— Codex 走的路径
- /v1/messages    —— Claude Code 走的路径（Anthropic 格式；公告里说的 "messages 格式"）

对 /v1/messages 会带 `anthropic-beta: context-1m-2025-08-07`（**这是启用 1m 上文的正确方式**）。
不带这个头时网关一律回 400「1m 上下文已经全量可用，请启用 1m 上下文后重试」——
注意**不是**靠模型名加 `[1m]` 后缀（实测加后缀无效，起作用的是请求头）。

用法：
    python probe_model_matrix.py --base-url https://gw.example.com/v1 --key sk-xxx
    python probe_model_matrix.py                  # 不传则从 ~/.codex/codex-model-router.json 取
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROUTER_CFG = Path.home() / ".codex" / "codex-model-router.json"


def from_router_config():
    """从路由配置里取第一个上游的地址与第一把 key。"""
    if not ROUTER_CFG.exists():
        return None, None
    cfg = json.loads(ROUTER_CFG.read_text(encoding="utf-8"))
    for up in (cfg.get("upstreams") or {}).values():
        keys = up.get("keys") or ([up["key"]] if up.get("key") else [])
        if up.get("base_url") and keys:
            return up["base_url"], keys[0]
    return None, None


def opener(proxy=None):
    # 空 dict = 真直连；不传 ProxyHandler 会继承 shell 里的 http_proxy
    if proxy:
        return urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
    return urllib.request.build_opener(urllib.request.ProxyHandler({}))


CODEX_BODY = {
    "instructions": "be brief",
    "input": [{"type": "message", "role": "user",
               "content": [{"type": "input_text", "text": "hi"}]}],
    "tools": [], "tool_choice": "auto", "parallel_tool_calls": True,
    "store": False, "stream": True, "include": ["reasoning.encrypted_content"],
    "prompt_cache_key": "matrix", "text": {"verbosity": "medium"},
    "reasoning": {"effort": "low"},
}


def call(base, path, body, key, extra_headers=None, timeout=30):
    hd = {"content-type": "application/json", "accept": "text/event-stream",
          "authorization": "Bearer " + key,
          "originator": "codex_exec", "user-agent": "codex_cli_rs/0.160.0"}
    if extra_headers:
        hd.update(extra_headers)
    rq = urllib.request.Request(base + path, data=json.dumps(body).encode("utf-8"),
                                headers=hd, method="POST")
    t0 = time.time()
    try:
        with opener().open(rq, timeout=timeout) as r:
            r.read(80)
            return r.status, "", time.time() - t0
    except urllib.error.HTTPError as e:
        return e.code, e.read(220).decode("utf-8", "replace"), time.time() - t0
    except Exception as e:
        return None, f"{type(e).__name__}: {e}", time.time() - t0


def brief(code, detail):
    if code == 200:
        return "✅ 可用"
    if code is None:
        return "❌ 连不上"
    try:
        d = json.loads(detail)
        msg = (d.get("error") or {}).get("message") if isinstance(d.get("error"), dict) else d.get("error")
        msg = msg or d.get("message") or detail
    except Exception:
        msg = detail
    msg = str(msg)[:60]
    return f"HTTP {code} {msg}"


def test(model, key, base):
    out = {}
    c, d, t = call(base, "/responses", dict(CODEX_BODY, model=model), key)
    out["responses"] = (c, brief(c, d), t)

    mbody = {"model": model, "max_tokens": 16,
             "messages": [{"role": "user", "content": "hi"}]}
    # 不带 beta 头 → 一律 400「请启用 1m」
    c, d, t = call(base, "/messages", mbody, key, {"anthropic-version": "2023-06-01"})
    out["messages"] = (c, brief(c, d), t)
    # 带上 1m 的 beta 头 → 才是真正的可用性
    c, d, t = call(base, "/messages", mbody, key,
                   {"anthropic-version": "2023-06-01",
                    "anthropic-beta": "context-1m-2025-08-07"})
    out["messages+1m"] = (c, brief(c, d), t)

    # OpenAI 的 chat 协议：Codex 自己不用（只认 responses），
    # 但网关只有 chat 时可以靠路由的桥接层翻译进来
    cbody = {"model": model, "max_tokens": 512, "temperature": 0,
             "messages": [{"role": "user", "content": "hi"}]}
    c, d, t = call(base, "/chat/completions", cbody, key)
    out["chat"] = (c, brief(c, d), t)
    return model, out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default=None,
                    help="网关地址（带路径前缀，如 https://gw.example.com/v1）")
    ap.add_argument("--key", default=None)
    ap.add_argument("--proxy", default=None, help="走代理，如 http://127.0.0.1:7897（默认直连）")
    a = ap.parse_args()

    base, cfg_key = from_router_config()
    base = (a.base_url or base or "").rstrip("/")
    if not base:
        sys.exit("请用 --base-url 指定网关地址（或先跑 setup_router.py 生成路由配置）")
    key = a.key or cfg_key
    if not key:
        sys.exit("请用 --key 指定密钥")

    rq = urllib.request.Request(base + "/models",
                                headers={"authorization": "Bearer " + key})
    with opener(a.proxy).open(rq, timeout=20) as r:
        models = [x["id"] for x in json.loads(r.read().decode("utf-8")).get("data", [])]
    print(f"{base}/models 列出 {len(models)} 个模型，逐个真打三条路径...\n")

    with ThreadPoolExecutor(max_workers=6) as p:
        results = dict(p.map(lambda m: test(m, key, base), models))

    protos = ["responses", "chat", "messages", "messages+1m"]
    print(f"{'模型':<34} " + " ".join(f"{p:<14}" for p in protos))
    print("-" * 92)
    for m in models:
        row = f"{m:<34} "
        for p in protos:
            cell = results[m].get(p)
            row += f"{(cell[1] if cell else '—'):<14} "
        print(row)

    print("\n可用的：")
    for p in protos:
        ok = [m for m in models if results[m].get(p) and results[m][p][0] == 200]
        print(f"  {p:<16} {len(ok)} 个: {ok if ok else '（无）'}")

    codex_ok = [m for m in models if results[m]["responses"][0] == 200]
    print(f"\n结论：Codex（/responses）能用的只有 {len(codex_ok)} 个 —— {codex_ok}")


if __name__ == "__main__":
    sys.exit(main())
