#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""网关可用性探针 —— 判断某个 OpenAI 兼容网关现在能不能被 Codex 使用。

它发的不是随便一个请求，而是**照抄 Codex CLI 的真实请求体形状**，
所以能拿到网关转发的**上游真实错误**，而不是网关自己的参数校验错误。

用法:
    python probe_gateway.py --base-url https://your-gateway.example.com/v1 --key sk-xxx
    python probe_gateway.py                       # 不传则从 ~/.codex/codex-model-router.json 取第一个上游
    python probe_gateway.py --direct              # 不走代理（默认也不走，除非 --proxy）
    python probe_gateway.py --proxy http://127.0.0.1:7897
    python probe_gateway.py --model gpt-6-astra   # 换模型

判读:
    OK          -> 上游可用，Codex 能跑
    OVERLOAD    -> 500 get_channel_failed / 负载已达上限，上游通道满了（等它恢复，改配置没用）
    BADMODEL    -> 404 不支持所选模型（该模型不在这条协议上）
    AUTH        -> 401/403 密钥无效或无权限
    BADREQ      -> 400 invalid codex request，请求体不被认作 Codex 请求
    LINK        -> 连不上（TLS/DNS/代理问题），先修网络
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROUTER_CFG = Path.home() / ".codex" / "codex-model-router.json"
DEFAULT_AUTH = Path.home() / ".codex" / "auth.json"


def from_router_config():
    """没显式传参数时，从路由配置里捞第一个上游的地址与第一把 key。"""
    if not ROUTER_CFG.exists():
        return None, None
    cfg = json.loads(ROUTER_CFG.read_text(encoding="utf-8"))
    for up in (cfg.get("upstreams") or {}).values():
        keys = up.get("keys") or ([up["key"]] if up.get("key") else [])
        if up.get("base_url") and keys:
            return up["base_url"], keys[0]
    return None, None


def load_key(explicit=None):
    if explicit:
        return explicit
    _, k = from_router_config()
    if k:
        return k
    if DEFAULT_AUTH.exists():
        d = json.loads(DEFAULT_AUTH.read_text(encoding="utf-8"))
        for name in ("OPENAI_API_KEY", "api_key", "key"):
            if isinstance(d.get(name), str) and d[name].startswith("sk-"):
                return d[name]
    raise SystemExit("找不到密钥，请用 --key 指定")


def codex_body(model, text="say hi", summary="auto"):
    """Codex CLI 真实请求体形状。

    - 缺字段会被 anyrouter 判为 400 invalid codex request
    - `reasoning.summary` 只能是 auto / concise / detailed；
      `"none"` 会被上游拒（ice API 与 anyrouter 都拒），
      而 Codex 目录里的 `default_reasoning_summary` 恰好是 "none" —— 见 SKILL §7b
    """
    return {
        "model": model,
        "instructions": "be brief",
        "input": [
            {"type": "message", "role": "user",
             "content": [{"type": "input_text", "text": text}]}
        ],
        "tools": [],
        "tool_choice": "auto",
        "parallel_tool_calls": True,
        "reasoning": {"effort": "low", "summary": summary},
        "store": False,
        "stream": True,
        "include": ["reasoning.encrypted_content"],
        "prompt_cache_key": "probe",
        "text": {"verbosity": "medium"},
    }


def opener(proxy):
    if not proxy:
        # 空 dict 才是真直连；不传 ProxyHandler 会继承 shell 里的 http_proxy 环境变量
        return urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": proxy, "https": proxy})
    )


def probe_models(op, key, base):
    rq = urllib.request.Request(base + "/models", headers={"Authorization": "Bearer " + key})
    t0 = time.time()
    try:
        with op.open(rq, timeout=20) as r:
            n = len(json.loads(r.read().decode("utf-8", "replace")).get("data", []))
            return f"HTTP 200  模型 {n} 个  {time.time() - t0:.2f}s"
    except urllib.error.HTTPError as e:
        return f"HTTP {e.code}  {time.time() - t0:.2f}s"
    except Exception as e:
        return f"{type(e).__name__}  {time.time() - t0:.2f}s"


def probe_responses(op, key, model, base):
    hd = {
        "Authorization": "Bearer " + key,
        "content-type": "application/json",
        "accept": "text/event-stream",
        "originator": "codex_exec",
        "user-agent": "codex_cli_rs probe",
    }
    rq = urllib.request.Request(
        base + "/responses",
        data=json.dumps(codex_body(model)).encode("utf-8"),
        headers=hd, method="POST",
    )
    t0 = time.time()
    try:
        with op.open(rq, timeout=130) as r:
            head = r.read(200).decode("utf-8", "replace")
            return 200, time.time() - t0, head
    except urllib.error.HTTPError as e:
        return e.code, time.time() - t0, e.read(300).decode("utf-8", "replace")
    except Exception as e:
        return None, time.time() - t0, f"{type(e).__name__}: {e}"


def verdict(status, elapsed, body):
    if status == 200:
        return "OK", "上游可用 —— Codex 现在能跑"
    if status == 500 and "get_channel_failed" in body:
        return "OVERLOAD", f"上游通道满载（{elapsed:.0f}s 内轮询完所有渠道）→ 等恢复，改配置无用"
    if status == 500:
        return "OVERLOAD", "上游 500 → 换模型或等恢复"
    if status == 404 and "不支持所选模型" in body:
        return "BADMODEL", "该模型不在此 API 分组里"
    if status == 400 and "invalid codex request" in body:
        return "BADREQ", "请求体不符合 Codex 格式"
    if status == 401:
        return "AUTH", "密钥无效"
    if status is None:
        return "LINK", "连不上 → 检查 Clash 节点/代理端口/TUN"
    return "OTHER", "见原始响应"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default=None,
                    help="网关地址（要带路径前缀，如 https://gw.example.com/v1）；"
                         "不传则取 ~/.codex/codex-model-router.json 的第一个上游")
    ap.add_argument("--model", default="gpt-6-astra")
    ap.add_argument("--proxy", default=None, help="走代理，如 http://127.0.0.1:7897（默认直连）")
    ap.add_argument("--key", default=None)
    a = ap.parse_args()

    base = (a.base_url or from_router_config()[0] or "").rstrip("/")
    if not base:
        sys.exit("请用 --base-url 指定网关地址（或先跑 setup_router.py 生成路由配置）")
    key = load_key(a.key)
    op = opener(a.proxy)

    print(f"网关: {base}")
    print(f"路径: {'代理 ' + a.proxy if a.proxy else '直连'}   模型: {a.model}")
    print(f"  /models      {probe_models(op, key, base)}")

    status, elapsed, body = probe_responses(op, key, a.model, base)
    code, msg = verdict(status, elapsed, body)
    print(f"  /responses   {status}  {elapsed:.1f}s")
    print(f"  原始响应: {body[:200]}")
    print(f"\n结论 [{code}] {msg}")
    return 0 if code == "OK" else 1


if __name__ == "__main__":
    sys.exit(main())
