#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""装配聚合路由：把多个中转网关收进一个 Codex provider。

做五件事（幂等，可反复跑）：
  1. 按 `~/.codex/router-sources.json` 的规则，从 cc-switch 库里取出各上游的
     base_url 与密钥（不硬编码密钥、不额外复制）
  2. 拉取各上游 /v1/models，**逐个真发一次请求**校验，生成路由表
     （按模型名分派 + 同名跨上游兜底）
  3. 写路由配置 `~/.codex/codex-model-router.json`
  4. 写合并模型目录 `~/.codex/codex-router-catalog.json`
  5. 在 cc-switch 库与 `~/.codex/config.toml` 里加/更新 `router` provider，并设为当前

用法：
    python setup_router.py --dry      # 只看会改什么
    python setup_router.py            # 执行（默认会装开机自启）
    python setup_router.py --no-activate    # 只加 provider，不切过去
    python setup_router.py --no-autostart   # 不装开机自启
    python setup_router.py --no-verify      # 跳过真发请求的校验（快，但可能混进用不了的模型）
"""
import argparse
import json
import os
import re
import shutil
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent          # 本文件所在目录（setup/）
REPO = HERE.parent                              # 仓库根
SRC_ROUTER_DIR = REPO / "router"                # router 脚本源目录

HOME = Path.home()
CC_DB = HOME / ".cc-switch" / "cc-switch.db"
CC_SETTINGS = HOME / ".cc-switch" / "settings.json"
CODEX = Path(os.environ.get("CODEX_HOME", HOME / ".codex"))
CONFIG_TOML = CODEX / "config.toml"
DISK_CATALOG = CODEX / "cc-switch-model-catalog.json"
ROUTER_CONFIG = CODEX / "codex-model-router.json"
ROUTER_CATALOG = CODEX / "codex-router-catalog.json"
EXTRA_KEYS = CODEX / "router-extra-keys.txt"      # 手动追加的 key（一行一把，# 注释）
ROUTER_DIR = CODEX / "router"                     # 装配后 router 脚本的落地目录
SOURCES_FILE = CODEX / "router-sources.json"      # 上游来源规则
BACKUP = CODEX / "router-backup" / f"router-{time.strftime('%Y%m%d-%H%M%S')}"
STARTUP = Path(os.environ.get("APPDATA") or (HOME / "AppData" / "Roaming")) / \
    "Microsoft/Windows/Start Menu/Programs/Startup"

PROVIDER_NAME = "聚合路由 (多网关聚合)"
PROVIDER_ID = "router-aggregate-codex"
SEGMENT = "router"          # [model_providers.router]
LISTEN = "127.0.0.1:8788"

# 上游来源规则模板：第一次运行时写到 ~/.codex/router-sources.json，之后按文件走。
#   name_match : cc-switch 里名字**含**该串的都收（同一个站的多把 key 会自动合并轮换）
#   name_exact : cc-switch 里名字**等于**该串的那一条
#   segment    : 读该条目的 [model_providers.<segment>] 段取 base_url
#   extra_keys : 额外把 ~/.codex/router-extra-keys.txt 里的 key 也并进来
SOURCES_TEMPLATE = {
    "upstreams": {
        "anyrouter": {"name_match": "anyrouter", "segment": "anyrouter", "extra_keys": True},
        "deepseek": {"name_exact": "DeepSeek", "segment": "custom"},
    },
    "order": ["anyrouter", "deepseek"],
    "auto_chain": [["anyrouter", "gpt-6-astra"], ["deepseek", "deepseek-flash"]],
}

UPSTREAM_SOURCES = {}      # 由 load_sources() 填充
FALLBACKS = {}             # 由 load_sources() 填充：模型 -> [[上游, 模型], ...] 跨模型兜底
UPSTREAM_ORDER = []
AUTO_CHAIN = []
# 这些模型 Codex 的 responses 路径用不了，别放进目录（Claude 走 /v1/messages，图片模型也不是对话模型）
SKIP_SUBSTR = ("-cc-format", "claude-", "gemini-", "image", "embedding", "rerank")


def load_sources(dry=False):
    """读 ~/.codex/router-sources.json；不存在就按模板生成一份（dry 模式只提示不写）。"""
    global UPSTREAM_SOURCES, UPSTREAM_ORDER, AUTO_CHAIN, PROVIDER_NAME
    if not SOURCES_FILE.exists():
        if dry:
            log(f"  [dry] 会生成上游来源模板: {SOURCES_FILE}")
            d = SOURCES_TEMPLATE
        else:
            SOURCES_FILE.parent.mkdir(parents=True, exist_ok=True)
            SOURCES_FILE.write_text(json.dumps(SOURCES_TEMPLATE, ensure_ascii=False, indent=2),
                                    encoding="utf-8", newline="\n")
            log(f"  已生成上游来源模板: {SOURCES_FILE}（可自行增删上游）")
            d = SOURCES_TEMPLATE
    else:
        d = json.loads(SOURCES_FILE.read_text(encoding="utf-8"))
    global FALLBACKS
    FALLBACKS = d.get("fallbacks") or {}
    if d.get("provider_name"):
        PROVIDER_NAME = d["provider_name"]
    UPSTREAM_SOURCES = d.get("upstreams") or {}
    names = list(UPSTREAM_SOURCES)
    UPSTREAM_ORDER = d.get("order") or names
    AUTO_CHAIN = [tuple(x) for x in (d.get("auto_chain") or [])]
    return d


def pythonw():
    """拿一个不带控制台的解释器路径（Windows 上 python.exe → pythonw.exe）。"""
    exe = sys.executable
    if exe.lower().endswith("python.exe"):
        cand = exe[:-len("python.exe")] + "pythonw.exe"
        if Path(cand).exists():
            return cand
    return exe


def log(*a):
    print(*a, flush=True)


# ---------------------------------------------------------------- 读取上游参数
def read_extra_keys():
    """手动追加的 key：一行一把，# 开头是注释。"""
    if not EXTRA_KEYS.exists():
        return []
    out = []
    for line in EXTRA_KEYS.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if s and not s.startswith("#") and s.startswith("sk-"):
            out.append(s)
    return out


def read_keys_file(path):
    """读一行一把 key 的文本文件（# 注释）。key 形态不限于 sk-（有的网关用 nvapi- 等）。"""
    p = Path(os.path.expandvars(os.path.expanduser(str(path))))
    if not p.exists():
        log(f"    !! keys_file 不存在: {p}")
        return []
    out = []
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip().strip('"').strip("'")
        if s and not s.startswith("#") and " " not in s and len(s) >= 16:
            out.append(s)
    return out


def detect_protocol(base, key, timeout=15):
    """探测网关是 responses 还是 chat 协议。

    Codex **只认 responses**（`wire_api="chat"` 已被官方移除），
    所以只有 chat 接口的网关必须靠桥接层翻译（见 router/chat_bridge.py）。
    判据：POST <base>/responses，拿到 `404 page not found` 这种"根本没这个路由"的
    响应就是 chat；返回 JSON 错误（400/404 模型不支持）说明端点存在。
    """
    op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    body = json.dumps({"model": "probe", "input": "hi", "messages": [{"role": "user", "content": "hi"}]}).encode()
    rq = urllib.request.Request(base.rstrip("/") + "/responses", data=body,
                                headers={"authorization": "Bearer " + key,
                                         "content-type": "application/json"}, method="POST")
    try:
        with op.open(rq, timeout=timeout) as r:
            r.read(64)
        return "responses"
    except urllib.error.HTTPError as e:
        txt = ""
        try:
            txt = e.read(200).decode("utf-8", "replace")
        except Exception:
            pass
        if e.code == 404 and "page not found" in txt.lower():
            return "chat"
        return "responses"
    except Exception:
        return "responses"


def read_upstreams():
    """把同一个站点的多把 key 合并成一个上游（keys 数组），供路由轮换。"""
    db = sqlite3.connect(str(CC_DB))
    out = {}
    extra = read_extra_keys()
    for short, src in UPSTREAM_SOURCES.items():
        base, keys, sources = None, [], []

        # A) 直接声明：base_url（+ 可选 keys_file / keys）
        if src.get("base_url"):
            base = str(src["base_url"]).rstrip("/")
            for k in (src.get("keys") or []):
                if k not in keys:
                    keys.append(k)
                    sources.append("sources 内联")
            if src.get("keys_file"):
                for k in read_keys_file(src["keys_file"]):
                    if k not in keys:
                        keys.append(k)
                        sources.append(Path(str(src["keys_file"])).name)

        # B) 从 cc-switch 收集
        for pname, sc in db.execute(
                "SELECT name,settings_config FROM providers WHERE app_type='codex'"):
            if src.get("name_exact"):
                if pname != src["name_exact"]:
                    continue
            elif src.get("name_match"):
                if src["name_match"] not in pname.lower():
                    continue
                if pname == PROVIDER_NAME:      # 别把路由自己收进来（名字里也可能含匹配词）
                    continue
            else:
                continue
            d = json.loads(sc or "{}")
            cfg = d.get("config", "")
            seg = src["segment"]
            m = re.search(r"(?m)^\[model_providers\." + re.escape(seg) + r"\]\s*\n"
                          r"((?:[^\[].*\n?)*)", cfg)
            if not m:                            # 没有这个段就不是这个上游，跳过
                continue
            body = m.group(1)
            bu = re.search(r'(?m)^\s*base_url\s*=\s*["\']([^"\']+)["\']', body)
            k = (d.get("auth") or {}).get("OPENAI_API_KEY")
            if bu:
                b = bu.group(1).rstrip("/")
                if base is None:
                    base = b
                elif b != base:
                    log(f"    ({pname}: base_url {b} 与已选 {base} 不同，忽略)")
            if k and k.startswith("sk-") and k not in keys:
                keys.append(k)
                sources.append(pname)
        if src.get("extra_keys"):
            for k in extra:
                if k not in keys:
                    keys.append(k)
                    sources.append(f"{EXTRA_KEYS.name}（手动追加）")
        if base and keys:
            proto = src.get("protocol") or detect_protocol(base, keys[0])
            up = {"base_url": base, "keys": keys, "proxy": src.get("proxy"), "protocol": proto}
            if src.get("models"):
                up["models"] = list(src["models"])
            out[short] = up
            ptag = "" if proto == "responses" else "  ⚠️ chat 协议（将经桥接层翻译）"
            log(f"  {short:<10} {base:<30} {len(keys)} 个 key  协议={proto}{ptag}")
            for k, s in zip(keys, sources):
                log(f"      …{k[-6:]:<8} 来自 {s}")
        else:
            log(f"  !! {short} 没取到 base_url / key，跳过")
    return out


def first_key(up):
    return (up.get("keys") or [up.get("key")])[0]


def fetch_models(up):
    rq = urllib.request.Request(up["base_url"] + "/models",
                                headers={"Authorization": "Bearer " + first_key(up)})
    op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with op.open(rq, timeout=20) as r:
        data = json.loads(r.read().decode("utf-8"))
    return [x.get("id") for x in data.get("data", []) if x.get("id")]


def build_base_routes(upstreams):
    """生成「模型名 -> 候选上游」。有 models 白名单就用它，否则拉 /v1/models 全量。"""
    per = {}
    for short, up in upstreams.items():
        allow = up.get("models") or []
        if allow:
            ids = list(allow)
            log(f"  {short:<10} 用白名单里的 {len(ids)} 个: {ids}")
        else:
            try:
                ids = [m for m in fetch_models(up) if not any(s in m.lower() for s in SKIP_SUBSTR)]
            except Exception as e:
                log(f"  !! {short} 拉模型列表失败: {type(e).__name__} {e}")
                ids = []
            log(f"  {short:<10} 列出 {len(ids)} 个: {ids}")
        per[short] = ids

    names = {}
    for short in UPSTREAM_ORDER:
        for m in per.get(short, []):
            names.setdefault(m, [])
            if short not in names[m]:
                names[m].append(short)

    routes = {}
    for m, ups in names.items():
        ordered = sorted(ups, key=UPSTREAM_ORDER.index)
        routes[m] = {"candidates": [{"upstream": u, "model": m} for u in ordered]}
    return routes, per


def finalize_routes(routes, per, verdicts):
    """剔除彻底用不了的候选，保留「暂时欠费/满载」的作为兜底，再补别名与一键兜底。

    区分很重要：
      unsupported（404 / key 无效）-> 删掉，留着只会每次白等
      nobalance（403 欠费）/ overloaded（500 满载）-> 保留，充值或错峰后就能用
    """
    HARD_DROP = {"unsupported"}
    dropped, weak = [], []
    for slug in list(routes):
        alive = []
        for c in routes[slug]["candidates"]:
            v, detail = verdicts.get((c["upstream"], c["model"]), ("unknown", ""))
            c["probe"] = v
            if v in HARD_DROP:
                dropped.append((slug, c["upstream"], c["model"], detail))
                continue
            if v in ("nobalance", "overloaded", "timeout", "net", "other", "empty"):
                weak.append((slug, c["upstream"], c["model"], v, detail))
            alive.append(c)
        if alive:
            routes[slug] = {"candidates": alive}
        else:
            del routes[slug]

    if dropped:
        log("\n  已剔除（彻底不可用）：")
        for slug, u, m, d in dropped:
            log(f"    - {u}/{m}  ({d})")
    if weak:
        log("\n  暂时不可用（保留作兜底，充值/错峰后可用）：")
        for slug, u, m, v, d in weak:
            log(f"    - {u}/{m}  [{v}] {d}")

    # 同名模型强制指定上游的别名（自己排第一，其余作兜底）
    for slug in list(routes):
        cands = routes[slug]["candidates"]
        if len(cands) > 1:
            for c in cands:
                routes[f"{slug}-{c['upstream']}"] = {
                    "candidates": [c] + [x for x in cands if x is not c]}

    # 一键兜底：按顺序试，第一个能用的上；跳过欠费的当主力（它会先失败再跳走）
    chain = []
    for u, m in AUTO_CHAIN:
        if m in per.get(u, []):
            for slug, r in routes.items():
                hit = [c for c in r["candidates"] if c["upstream"] == u and c["model"] == m]
                if hit:
                    chain.append(dict(hit[0]))
                    break
    if chain:
        # 把探测通过的排前面，欠费的垫底
        chain.sort(key=lambda c: 0 if c.get("probe") == "ok" else 1)
        routes["auto"] = {"candidates": chain}

    # 跨模型兜底：某个模型的候选全挂了，就换别的家的别的模型顶上
    # （例如 gpt-6-astra 全被限流时落到 NVIDIA 的 nemotron）
    added = []
    for slug, chain_spec in (FALLBACKS or {}).items():
        r = routes.get(slug)
        if not r:
            continue
        have = {(c["upstream"], c["model"]) for c in r["candidates"]}
        for u, m in chain_spec:
            if (u, m) in have or u not in per or m not in per.get(u, []):
                continue
            extra = {"upstream": u, "model": m, "probe": "ok", "fallback": True}
            r["candidates"].append(extra)
            have.add((u, m))
            added.append((slug, u, m))
    if added:
        log("\n  跨模型兜底已加入：")
        for slug, u, m in added:
            log(f"    {slug} 失败后 → {u}/{m}")
    return routes


def live_slugs(routes):
    """能进模型选择器的：只要不是「所有候选都欠费」就放进去。

    只欠费的模型先不出现在选择器里（选了也白选），但在路由里留着 ——
    万一上游充值了，同名兜底照样能落到它身上。
    满载（overloaded）要放进去：那是临时的，用户重试或走 auto 兜底都能用上。
    """
    return [s for s, r in routes.items()
            if any(c.get("probe") != "nobalance" for c in r["candidates"])]


# ---------------------------------------------------------------- 校验候选
PROBE_BODY = {
    "instructions": "be brief",
    "input": [{"type": "message", "role": "user",
               "content": [{"type": "input_text", "text": "hi"}]}],
    "tools": [], "tool_choice": "auto", "parallel_tool_calls": True,
    "store": False, "stream": True, "include": ["reasoning.encrypted_content"],
    "prompt_cache_key": "setup-probe", "text": {"verbosity": "medium"},
    "reasoning": {"effort": "low"},
}


def probe_one(up, model, timeout=12):
    """真发一次请求，逐个 key 试。返回 (verdict, detail)。

    ok        至少一个 key 可用
    unsupported 明确不支持（404 / 400 invalid model / 非配额 401,403）—— 要从目录里剔掉
    nobalance   所有 key 都额度不足 —— 模型存在，充值后可用
    overloaded  上游临时满载（500 get_channel_failed 等）—— 模型是存在的，保留
    timeout/net 探测不出结论 —— 保留但标注
    """
    worst = ("unknown", "")
    for key in (up.get("keys") or [up.get("key")]):
        verdict, detail = _probe_one_key(up, model, key, timeout)
        if verdict == "ok":
            return "ok", f"HTTP 200（key …{key[-6:]}）" if len(up.get("keys") or []) > 1 \
                else "HTTP 200"
        # 优先级：nobalance/overloaded 这类"模型没问题、只是当前用不了"要压过 unsupported 的误判
        if verdict in ("overloaded", "nobalance", "timeout", "net") and worst[0] == "unknown":
            worst = (verdict, f"…{key[-6:]} {detail}")
        elif worst[0] == "unknown":
            worst = (verdict, f"…{key[-6:]} {detail}")
    return worst


def _probe_one_key(up, model, key, timeout):
    import queue as _q
    import threading as _t
    proto = up.get("protocol") or "responses"
    if proto == "chat":
        # chat 上游按非流式探测：一次性拿到完整回包，好看清 content 有没有出来
        sys.path.insert(0, str(SRC_ROUTER_DIR))
        from chat_bridge import responses_to_chat
        probe = dict(PROBE_BODY)
        probe["stream"] = False
        probe.pop("include", None)
        probe["model"] = model
        body = responses_to_chat(probe)
        path = "/chat/completions"
    else:
        body = dict(PROBE_BODY)
        body["model"] = model
        path = "/responses"
    rq = urllib.request.Request(
        up["base_url"] + path, data=json.dumps(body).encode("utf-8"),
        headers={"content-type": "application/json", "accept": "text/event-stream",
                 "authorization": "Bearer " + key,
                 "originator": "codex_exec", "user-agent": "codex_cli_rs/0.160.0"},
        method="POST")
    res = _q.Queue()

    def work():
        try:
            op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            r = op.open(rq, timeout=60)
            code, txt = r.status, ""
            if code == 200 and proto == "chat":
                # 200 也可能是"内容为空"（推理模型把额度花在思维链上）
                txt = r.read(20000).decode("utf-8", "replace")
            elif code != 200:
                txt = r.read(300).decode("utf-8", "replace")
            r.close()
            res.put(("ok" if code == 200 else "http", code, txt))
        except urllib.error.HTTPError as e:
            res.put(("http", e.code, e.read(300).decode("utf-8", "replace")))
        except Exception as e:
            res.put(("net", type(e).__name__, str(e)))

    _t.Thread(target=work, daemon=True).start()
    try:
        kind, a, b = res.get(timeout=timeout)
    except _q.Empty:
        return "timeout", f">{timeout}s 无响应"
    if kind == "ok":
        if proto == "chat":
            # chat 上游：看 content / tool_calls 有没有真出来
            try:
                ch = (json.loads(b).get("choices") or [{}])[0]
                msg = ch.get("message") or {}
                if msg.get("content") or msg.get("tool_calls"):
                    return "ok", "HTTP 200"
                if msg.get("reasoning_content"):
                    return "empty", "HTTP 200 但只回了思维链（推理模型，正式用要放宽 max_tokens）"
                return "empty", f"HTTP 200 但内容为空 finish={ch.get('finish_reason')!r}"
            except Exception:
                return "empty", f"HTTP 200 但响应解析不出内容: {b[:60]}"
        return "ok", "HTTP 200"
    if kind == "net":
        return "net", f"{a}: {b[:80]}"
    if a == 404 or "不支持所选模型" in b or "invalid_value" in b:
        return "unsupported", f"HTTP {a} {b[:90]}"
    if a in (500, 502, 503, 504) or "get_channel_failed" in b or "负载" in b:
        return "overloaded", f"HTTP {a} {b[:90]}"
    if a == 403 and any(s in b for s in ("额度", "余额", "quota", "insufficient", "balance")):
        return "nobalance", f"HTTP {a} {b[:90]}"
    if a in (401, 403):
        return "unsupported", f"HTTP {a} {b[:90]}"
    return "other", f"HTTP {a} {b[:90]}"


def verify_routes(upstreams, routes, workers=4, timeout=12):
    from concurrent.futures import ThreadPoolExecutor
    jobs = [(slug, c) for slug, r in routes.items() for c in r["candidates"]
            if not slug.startswith("auto")]
    seen = {}
    log(f"  逐个真实探测 {len(jobs)} 个候选（并发 {workers}，超时 {timeout}s）...")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {(slug, c["upstream"], c["model"]):
                pool.submit(probe_one, upstreams[c["upstream"]], c["model"], timeout)
                for slug, c in jobs}
        for (slug, up_short, model), f in futs.items():
            try:
                seen[(up_short, model)] = f.result()
            except Exception as e:
                seen[(up_short, model)] = ("net", str(e))
    return seen

def build_catalog(routes, template_path):
    tpl = None
    if template_path.exists():
        d = json.loads(template_path.read_text(encoding="utf-8"))
        ms = d.get("models") or []
        if ms:
            tpl = ms[0]
    if tpl is None:
        sys.exit(f"没有可用的条目模板: {template_path}")
    models = []
    for slug in routes:
        e = json.loads(json.dumps(tpl))          # 深拷贝模板，保证字段结构一致
        e["slug"] = slug
        e["display_name"] = slug
        e["description"] = "聚合路由: " + " -> ".join(
            f"{c['upstream']}/{c['model']}" for c in routes[slug]["candidates"])
        e["default_reasoning_level"] = "medium"
        e["default_reasoning_summary"] = "auto"   # 合法值；"none" 会被网关拒
        e["input_modalities"] = ["text", "image"]
        e["supports_reasoning_summaries"] = True
        e["supported_reasoning_levels"] = [
            {"description": "Fast responses with lighter reasoning", "effort": "low"},
            {"description": "Balances speed and reasoning depth", "effort": "medium"},
            {"description": "Greater reasoning depth for complex problems", "effort": "high"},
            {"description": "Extra high reasoning depth", "effort": "xhigh"},
        ]
        models.append(e)
    return {"models": models}


# ---------------------------------------------------------------- 写配置
def provider_config_text(default_model):
    return f"""model_provider = "{SEGMENT}"
model = "{default_model}"
model_catalog_json = "codex-router-catalog.json"
model_reasoning_effort = "high"

[model_providers.{SEGMENT}]
name = "{PROVIDER_NAME}"
base_url = "http://{LISTEN}/v1"
wire_api = "responses"
requires_openai_auth = true
experimental_bearer_token = "local-router"
"""


def patch_disk_config(default_model, dry):
    text = CONFIG_TOML.read_text(encoding="utf-8")
    orig = text
    text = re.sub(r'(?m)^model_provider\s*=\s*"[^"]*"', f'model_provider = "{SEGMENT}"', text, count=1)
    text = re.sub(r'(?m)^model\s*=\s*"[^"]*"', f'model = "{default_model}"', text, count=1)
    text = re.sub(r'(?m)^model_catalog_json\s*=\s*"[^"]*"',
                  'model_catalog_json = "codex-router-catalog.json"', text, count=1)
    if f"[model_providers.{SEGMENT}]" not in text:
        text = text.rstrip("\n") + "\n\n" + f"[model_providers.{SEGMENT}]\n" + \
            f'name = "{PROVIDER_NAME}"\n' \
            f'base_url = "http://{LISTEN}/v1"\n' \
            'wire_api = "responses"\n' \
            'requires_openai_auth = true\n' \
            'experimental_bearer_token = "local-router"\n'
    if text == orig:
        log("  config.toml 已是最新，无需改")
        return False
    if dry:
        log("  [dry] 会改 config.toml 的 model_provider / model / model_catalog_json + 加 router 段")
        return True
    shutil.copy2(CONFIG_TOML, CONFIG_TOML.with_suffix(".toml.bak-router"))
    CONFIG_TOML.write_text(text, encoding="utf-8", newline="\n")
    log(f"  已改 config.toml（备份 config.toml.bak-router）")
    return True


def upsert_db_provider(default_model, activate, dry):
    db = sqlite3.connect(str(CC_DB))
    cfg = provider_config_text(default_model)
    settings = json.dumps({"config": cfg, "auth": {"OPENAI_API_KEY": "local-router"}},
                          ensure_ascii=False)
    exists = db.execute("SELECT id FROM providers WHERE id=? AND app_type='codex'",
                        (PROVIDER_ID,)).fetchone()
    if dry:
        log(f"  [dry] 会{'更新' if exists else '新增'} provider 记录 id={PROVIDER_ID}")
        return
    if exists:
        db.execute("UPDATE providers SET name=?, settings_config=? WHERE id=? AND app_type='codex'",
                   (PROVIDER_NAME, settings, PROVIDER_ID))
        log("  已更新 cc-switch 里的聚合路由 provider")
    else:
        db.execute(
            "INSERT INTO providers (id, app_type, name, settings_config, category, meta, "
            "is_current, in_failover_queue, cost_multiplier, sort_index) "
            "VALUES (?, 'codex', ?, ?, 'custom', '{}', 0, 0, '1.0', 0)",
            (PROVIDER_ID, PROVIDER_NAME, settings))
        log("  已新增 cc-switch provider：聚合路由")
    if activate:
        db.execute("UPDATE providers SET is_current=0 WHERE app_type='codex'")
        db.execute("UPDATE providers SET is_current=1 WHERE id=? AND app_type='codex'", (PROVIDER_ID,))
        log("  已把聚合路由设为当前 provider")
    db.commit()
    if activate:
        s = json.loads(CC_SETTINGS.read_text(encoding="utf-8"))
        s["currentProviderCodex"] = PROVIDER_ID
        CC_SETTINGS.write_text(json.dumps(s, ensure_ascii=False, indent=2),
                               encoding="utf-8", newline="\n")
        log("  已同步 settings.json 的 currentProviderCodex")


def install_launcher(dry, autostart):
    """把 router 三个脚本装到 ~/.codex/router/，并写一个 start.cmd。

    start.cmd 拉起的是**看门狗**（不是路由本身）：看门狗会把路由拉起来并每 60 秒
    探活一次，路由挂了自动救回来。
    """
    ROUTER_DIR.mkdir(parents=True, exist_ok=True)
    if dry:
        log(f"  [dry] 会安装 {ROUTER_DIR}/ 下的 codex_router.py / chat_bridge.py / watchdog.py / ensure_router.py"
            " 与 start.cmd" + ("，并放到开机启动" if autostart else ""))
        return
    for fn in ("codex_router.py", "chat_bridge.py", "watchdog.py", "ensure_router.py"):
        shutil.copy2(SRC_ROUTER_DIR / fn, ROUTER_DIR / fn)
    cmd = ROUTER_DIR / "start.cmd"
    cmd.write_text(
        "@echo off\r\n"
        "rem 聚合路由看门狗：路由挂了会自动拉回来，这个窗口不用管\r\n"
        "rem 想彻底停掉：任务管理器里结束 pythonw.exe\r\n"
        f'start "" "{pythonw()}" "{ROUTER_DIR / "watchdog.py"}"\r\n',
        encoding="utf-8", newline="")
    log(f"  已安装 {ROUTER_DIR}/ 下的三个脚本与 start.cmd")
    if autostart:
        try:
            STARTUP.mkdir(parents=True, exist_ok=True)
            shutil.copy2(cmd, STARTUP / "聚合路由.cmd")
            log(f"  已加入开机启动（删掉 {STARTUP / '聚合路由.cmd'} 即可取消）")
        except Exception as e:
            log(f"  !! 加入开机启动失败: {type(e).__name__} {e}")


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--no-activate", action="store_true", help="不切当前 provider")
    ap.add_argument("--no-autostart", action="store_true")
    ap.add_argument("--no-verify", action="store_true", help="跳过真实探测（快，但可能混进用不了的模型）")
    a = ap.parse_args()

    log("=== 0) 读取上游来源规则 ===")
    load_sources(dry=a.dry)
    log(f"  上游: {list(UPSTREAM_SOURCES)}   候选顺序: {UPSTREAM_ORDER}   兜底链: {AUTO_CHAIN}")
    log(f"  provider 名: {PROVIDER_NAME}")

    log("\n=== 1) 读取各上游的地址与密钥 ===")
    upstreams = read_upstreams()
    if len(upstreams) < 1:
        sys.exit("没有取到任何上游，退出")

    log("\n=== 2) 拉模型列表并生成候选路由 ===")
    base, per = build_base_routes(upstreams)
    if not base:
        sys.exit("路由为空，退出")

    log("\n=== 2b) 真实探测，剔掉用不了的 ===")
    if a.no_verify:
        log("  已跳过（--no-verify）")
        verdicts = {}
    else:
        verdicts = verify_routes(upstreams, base)
        from collections import Counter
        c = Counter(v for v, _ in verdicts.values())
        log(f"  探测结果: {dict(c)}")
        for (u, m), (v, d) in sorted(verdicts.items()):
            if v != "ok":
                log(f"    {u:<10} {m:<24} [{v}] {d}")

    routes = finalize_routes(base, per, verdicts)
    live = live_slugs(routes)
    log(f"\n  路由 {len(routes)} 条，其中 {len(live)} 条能进模型选择器")
    for slug, r in routes.items():
        chain = " -> ".join(
            f"{x['upstream']}/{x['model']}"
            + ("(chat)" if (upstreams.get(x["upstream"], {}).get("protocol") == "chat") else "")
            + ("" if x.get("probe") in (None, "ok") else f"[{x['probe']}]")
            for x in r["candidates"])
        mark = " " if slug in live else "×"
        log(f"    {mark} {slug:<26} {chain}")
    excluded = [s for s in routes if s not in live]
    if excluded:
        log(f"\n  以下先不放选择器（当前没有可用上游，充值/恢复后重跑本脚本即回来）：")
        log(f"    {excluded}")

    if a.dry:
        log("\n=== [dry] 之后会做 ===")
        patch_disk_config("auto", True)
        upsert_db_provider("auto", not a.no_activate, True)
        install_launcher(True, not a.no_autostart)
        return

    BACKUP.mkdir(parents=True, exist_ok=True)
    for f in (CC_DB, CONFIG_TOML, DISK_CATALOG, CC_SETTINGS):
        if f.exists():
            shutil.copy2(f, BACKUP / f.name)
    log(f"\n已备份到 {BACKUP}")

    log("\n=== 3) 写路由配置 ===")
    ROUTER_CONFIG.write_text(json.dumps({
        "listen": LISTEN, "default": "auto", "header_timeout": 30,
        # expected_keys 是给 router.py --doctor 用的基线：下次发现 key 变少会报警
        "expected_keys": {k: len(v.get("keys") or []) for k, v in upstreams.items()},
        "upstreams": upstreams, "routes": routes,
    }, ensure_ascii=False, indent=2), encoding="utf-8", newline="\n")
    log(f"  {ROUTER_CONFIG}")

    log("\n=== 4) 写合并模型目录 ===")
    cat = build_catalog({s: routes[s] for s in live}, DISK_CATALOG)
    ROUTER_CATALOG.write_text(json.dumps(cat, ensure_ascii=False, indent=2),
                              encoding="utf-8", newline="\n")
    log(f"  {ROUTER_CATALOG}（{len(cat['models'])} 个模型）")

    log("\n=== 5) 改 Codex 配置与 cc-switch ===")
    patch_disk_config("auto", False)
    upsert_db_provider("auto", not a.no_activate, False)

    log("\n=== 6) 安装启动器 ===")
    install_launcher(False, not a.no_autostart)

    log("\n完成。启动路由（双击即可，已在跑就什么都不做）：")
    log(f'  {ROUTER_DIR / "start.cmd"}    （直接起：'
        f'"{pythonw()}" "{ROUTER_DIR / "codex_router.py"}"）')
    log("\n体检 / 排查：")
    log(f'  "{sys.executable}" "{ROUTER_DIR / "codex_router.py"}" --doctor    # 一键体检')
    log(f'  "{sys.executable}" "{ROUTER_DIR / "codex_router.py"}" --routes    # 路由表')
    log(f'  "{sys.executable}" "{ROUTER_DIR / "codex_router.py"}" --keys      # key 与冷却状态')


if __name__ == "__main__":
    main()
