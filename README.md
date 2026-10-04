# Aggregate Route · Codex 多网关聚合路由

把多个中转网关（anyrouter / DeepSeek / ice 之类）收进**一个 Codex provider**：
Codex 只连一个本地地址，路由按**模型名**把请求转发到对应网关；
同一个网关有多把 key 就**轮着用**，一把不行自动换下一把，一家不行自动换下一家。

不用再在 cc-switch 里来回切 provider 了 —— 在模型选择器里换个模型，就等于换个 API。

## 解决什么问题

- 中转网关经常**限流/满载**，报错还是形如 `当前模型 X 负载已经达到上限`。手动切 provider 太慢。
- 同一个站往往有好几把 key，希望**自动轮换上**，而不是一把被打满就整站不可用。
- 不同网关的**模型名不一样**，希望"选谁就走谁家"，而不是先想"我现在挂在哪个 provider 上"。

## 特性

- **一个 provider 用遍所有网关**：Codex 只认 `http://127.0.0.1:8788/v1`，剩下的交给路由。
- **chat 协议桥接**：Codex **只支持 Responses API**（`wire_api = "chat"` 已被官方移除），
  而很多网关（如 NVIDIA NIM）只有 `/chat/completions`。
  路由内置翻译层，把 Responses 请求/事件流与 Chat 的 messages/delta 流互转，
  于是**只有 chat 的网关也能挂进来**（工具调用、图片输入都实测通过）。
- **多 key 轮换 + 冷却**：每把 key 依次轮换起步；失败的那把临时冷置
  （额度不足 15 分钟 / 限流 2 分钟 / 5xx 与超时 1 分钟），正常的优先用，全冷置时仍逐个试。
- **两级故障转移**：同一网关的下一把 key → 路由表里的下一个候选网关。
- **快失败**：等响应头超过 30 秒就当这个候选不行，直接换下一个
  （有些网关满载要 80 秒才回 500，不能陪它等）。
- **真实错误透传**：全部候选都失败时，把**最后一个上游的原话**回给 Codex，而不是笼统的 502。
- **模型目录自动生成**：装配时**逐个模型真发一次请求**校验，只把真能用的放进模型选择器
  （网关的 `/v1/models` 列表经常骗人）。
- **看门狗**：路由挂了 60 秒内自动拉起来，开机自启，平时不用管。
- **零依赖**：只用 Python 标准库。

## 架构

```
        ┌──────────┐   /v1/responses    ┌────────────────┐
        │  Codex   │ ─────────────────► │  codex_router  │
        │ (单一    │                    │  127.0.0.1:8788│
        │ provider)│ ◄───────────────── │                │
        └──────────┘      SSE 流        └───────┬────────┘
                                                │ 按模型名查路由表
                        ┌───────────────────────┼───────────────────────┐
                        ▼                       ▼                       ▼
                  ┌───────────┐          ┌───────────┐          ┌───────────┐
                  │ 网关 A    │          │ 网关 B    │          │ 网关 C    │
                  │ responses │          │ responses │          │ chat 协议 │
                  │ key×N 轮换│          │ key×1     │          │（桥接翻译）│
                  └───────────┘          └───────────┘          └───────────┘
```

请求处理顺序：`路由表取候选 → 候选1 的 key 轮换 → 候选2 的 key 轮换 → … → 全失败则透出最后一个错误`

## 快速开始

需要 Python 3.9+（只用标准库，无需 pip 安装任何东西）。

```bash
git clone git@github.com:dcsnkj/Aggregate-Route.git
cd Aggregate-Route
```

**1. 准备上游来源**（首次运行会自动生成模板 `~/.codex/router-sources.json`）

```json
{
  "upstreams": {
    "anyrouter": { "name_match": "anyrouter", "segment": "anyrouter", "extra_keys": true },
    "deepseek":  { "name_exact": "DeepSeek",  "segment": "custom" }
  },
  "order": ["anyrouter", "deepseek"],
  "auto_chain": [["anyrouter", "gpt-6-astra"], ["deepseek", "deepseek-flash"]]
}
```

| 字段 | 含义 |
|---|---|
| `name_match` | cc-switch 里**名字含**该串的条目全收（同一个站的多把 key 会自动合并轮换） |
| `name_exact` | cc-switch 里**名字等于**该串的那一条 |
| `segment` | 读该条目的 `[model_providers.<segment>]` 段取 `base_url` |
| `extra_keys` | 额外把 `~/.codex/router-extra-keys.txt` 里的 key 也并进来 |
| `order` | 同名模型跨网关时的候选顺序（靠前的优先） |
| `auto_chain` | `auto` 这个兜底模型的尝试顺序 |
| `provider_name` | 可选，覆盖在 cc-switch 里显示的名字 |
| `fallbacks` | 可选，**跨模型兜底**：某个模型的候选全挂了就换别家的别的模型顶上。

```json
"fallbacks": {
  "gpt-6-astra": [["nvidia", "nvidia/nemotron-3-ultra-550b-a55b"],
                  ["deepseek", "deepseek-flash"]]
}
```

网关不在 cc-switch 里时，可以**直接声明**：

```json
"nvidia": {
  "base_url": "https://integrate.api.nvidia.com/v1",
  "keys_file": "~/.codex/nvidia-keys.txt",
  "models": ["nvidia/nemotron-3-ultra-550b-a55b"]
}
```

| 字段 | 含义 |
|---|---|
| `base_url` | 网关地址（带路径前缀） |
| `keys_file` | 一行一把 key 的文本文件（`#` 注释） |
| `models` | 只挑这些模型（不写就拉网关的 `/v1/models` 全量；
  网关列表常混着大量无权限/已停用的模型，写白名单更省事） |
| `slug` | 可选，**单一入口**：把该网关的多个模型合成一个模型名，
  选择器里只出现这一个（内部按候选顺序换用）。**模型名带 `/` 时必须用它** ——
  Codex 的模型选择器不显示带斜杠的名字 |
| `extra_body` | 可选，**原样并入 chat 请求体**的额外参数。典型用法：`{"reasoning_effort": "low"}` —— 推理模型默认会先长时间输出思维链，而桥接层只转发正式内容，界面会一直显示「正在思考」 |
  选择器里只出现这一个（内部按候选顺序换用）。**模型名带 `/` 时必须用它** ——
  Codex 的模型选择器不显示带斜杠的名字 |

> `protocol`（`responses` / `chat`）**不用手写** —— 装配时会自动探测并把该字段写进路由配置。

**2. 装配**

```bash
python setup/setup_router.py --dry     # 先看会改什么
python setup/setup_router.py           # 执行（会把路由设为当前 provider，并装开机自启）
```

它会：从 cc-switch 取地址与密钥 → 拉各网关的模型列表 → **逐个真发请求校验** →
写路由配置与模型目录 → 在 cc-switch 与 `config.toml` 里加一个 `router` provider → 装启动器。

**3. 启动**

```bash
# Windows：双击（幂等，已在跑就什么都不做）
%USERPROFILE%\.codex\router\start.cmd
# 或直接跑看门狗（它会拉起路由并每 60 秒探活）
python ~/.codex/router/watchdog.py
```

**4. 重启 Codex**（配置和模型目录都是启动时读的），然后在模型选择器里选模型即可。
推荐直接用 `auto`：它按 `auto_chain` 依次尝试，第一个能用的就上。

## 常用命令

```bash
python ~/.codex/router/codex_router.py --doctor   # 一键体检：路由/看门狗/key 数/目录/近期失败
python ~/.codex/router/codex_router.py --routes   # 路由表
python ~/.codex/router/codex_router.py --keys     # 各上游 key 数与冷却状态
python ~/.codex/router/codex_router.py --check    # 逐个候选 + key 真实探测
```

工具（`tools/`）：

```bash
# 单模型健康检查：拿网关转发的「上游真实错误」，秒级出结果
python tools/probe_gateway.py --base-url https://gw.example.com/v1 --key sk-xxx

# 模型 × 协议可用性矩阵：这个网关到底哪些模型能用、分别能用在哪条协议上
python tools/probe_model_matrix.py --base-url https://gw.example.com/v1 --key sk-xxx

# 抓 Codex 的原始请求体（RUST_LOG 不记 body，只能这么看）
python tools/capture_request.py &
```

## 目录结构

```
router/
  codex_router.py     路由本体：流式 SSE 透传、多 key 轮换、冷却、两级故障转移、热加载配置
  chat_bridge.py      chat ↔ responses 协议桥：让只有 /chat/completions 的网关也能给 Codex 用
  watchdog.py         看门狗：每 60 秒探活，路由挂了自动拉回
  ensure_router.py    "确保在跑"：给会话启动钩子之类的一次性调用
setup/
  setup_router.py     装配：生成路由配置 + 模型目录 + Codex provider + 启动器
tools/
  probe_gateway.py    单模型健康检查
  probe_model_matrix.py  模型 × 协议可用性矩阵
  capture_request.py  本地抓包：看 Codex 真实请求体
docs/
  使用说明.md          更详细的中文说明
```

## 配置文件

| 文件 | 说明 |
|---|---|
| `~/.codex/router-sources.json` | 上游来源规则（见上） |
| `~/.codex/router-extra-keys.txt` | 手动追加的 key，一行一把，`#` 注释。**推荐新 key 写这里**（独立于 cc-switch） |
| `~/.codex/codex-model-router.json` | 装配产物：上游地址/密钥/路由表/基线。**改了会自动热加载** |
| `~/.codex/codex-router-catalog.json` | 装配产物：Codex 的模型目录（决定选择器里显示哪些） |
| `~/.codex/logs/model-router.log` | 转发日志 + 看门狗日志（JSON 行） |

## 注意事项

- **密钥只存在本机**：本仓库不含任何 key。路由配置里的密钥是明文，
  来源是 cc-switch 数据库或 `router-extra-keys.txt`，请注意这两个文件的权限。
- 目前只在 **Windows** 上验证过（看门狗、开机自启用的是启动文件夹）。
  路由本体是跨平台的，macOS/Linux 需要自己接一下自启。
- `--doctor` 的 key 数量对比依赖装配时写入的 `expected_keys` 基线；
  手工改过路由配置后基线可能失真，重跑一次装配即可。
- 网关的 `/v1/models` **不可信**：不同协议（`/responses` / `/chat/completions` / `/messages`）
  支持的模型完全不同，用 `tools/probe_model_matrix.py` 实测为准。
- **Codex 只认 Responses 协议**。所以「网关能不能用」的第一道门槛是
  `POST <base>/responses` 有没有这个端点：`200/400` 说明有，`404 page not found` 说明没有
  （没有的走桥接层）。

## License

未指定。如需使用请自行判断。
