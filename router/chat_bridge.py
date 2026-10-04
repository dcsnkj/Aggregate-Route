#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""chat ↔ responses 协议桥。

为什么需要：Codex 只认 Responses API（`wire_api = "chat"` 已被官方移除），
而大量网关（如 NVIDIA NIM）只有 OpenAI 的 `/chat/completions`。
本模块把两个方向都翻掉：

    请求：Responses 形状（instructions + input[]）  → Chat 形状（messages[]）
    响应：Chat 的 `choices[].delta` 流              → Responses 的 SSE 事件流

Responses 事件的确切形状是照着真实网关回包定的（`sequence_number` 必须递增、
`response` 对象要带齐字段），不是照文档猜的 —— 少字段 Codex 会解析失败。
"""
import json
import time
import uuid

__all__ = ["responses_to_chat", "ChatStreamBridge", "chat_to_responses"]


def _rid(prefix):
    return prefix + uuid.uuid4().hex + uuid.uuid4().hex[:8]


# ---------------------------------------------------------------- 请求方向
def _content_to_chat(content):
    """Responses 的 content 数组 → Chat 的 content。全文本时压成字符串（兼容性最好）。"""
    if isinstance(content, str):
        return content
    parts = []
    for p in content or []:
        pt = p.get("type")
        if pt in ("input_text", "output_text", "text"):
            parts.append({"type": "text", "text": p.get("text") or ""})
        elif pt in ("input_image", "image", "image_url"):
            u = p.get("image_url")
            if isinstance(u, dict):
                u = u.get("url") or u.get("image_url") or ""
            parts.append({"type": "image_url", "image_url": {"url": u or p.get("image_url") or ""}})
        # 其它类型（audio/file 等）直接丢
    if not parts:
        return ""
    if all(p["type"] == "text" for p in parts):
        return "".join(p["text"] for p in parts)
    return parts


def responses_to_chat(body):
    """Responses 请求体 → Chat 请求体。"""
    messages = []
    ins = body.get("instructions")
    if ins:
        messages.append({"role": "system", "content": ins})

    for item in body.get("input") or []:
        if isinstance(item, str):
            messages.append({"role": "user", "content": item})
            continue
        t = item.get("type")
        if t == "message":
            messages.append({"role": item.get("role") or "user",
                             "content": _content_to_chat(item.get("content"))})
        elif t == "function_call":
            messages.append({
                "role": "assistant", "content": None,
                "tool_calls": [{
                    "id": item.get("call_id") or item.get("id") or _rid("call_"),
                    "type": "function",
                    "function": {"name": item.get("name") or "",
                                 "arguments": item.get("arguments") or "{}"},
                }],
            })
        elif t == "function_call_output":
            out = item.get("output")
            if not isinstance(out, str):
                out = json.dumps(out, ensure_ascii=False)
            messages.append({"role": "tool",
                             "tool_call_id": item.get("call_id") or "",
                             "content": out})
        elif t == "reasoning":
            continue                     # 思维链不回灌给上游
        elif item.get("role"):
            messages.append({"role": item["role"],
                             "content": _content_to_chat(item.get("content"))})

    chat = {"model": body.get("model"), "messages": messages}

    tools = []
    for t in body.get("tools") or []:
        if t.get("type") != "function":
            continue                     # local_shell / web_search 之类 chat 侧没有
        fn = {"name": t.get("name") or "",
              "description": t.get("description") or "",
              "parameters": t.get("parameters") or {"type": "object", "properties": {}}}
        if t.get("strict") is not None:
            fn["strict"] = t["strict"]
        tools.append({"type": "function", "function": fn})
    if tools:
        chat["tools"] = tools
        tc = body.get("tool_choice")
        if isinstance(tc, dict) and (tc.get("type") == "function" or tc.get("name")):
            chat["tool_choice"] = {"type": "function",
                                   "function": {"name": tc.get("name") or ""}}
        else:
            chat["tool_choice"] = tc if tc in ("auto", "none", "required") else "auto"
        if body.get("parallel_tool_calls") is not None:
            chat["parallel_tool_calls"] = bool(body["parallel_tool_calls"])

    for a, b in (("temperature", "temperature"), ("top_p", "top_p"),
                 ("max_output_tokens", "max_tokens")):
        if body.get(a) is not None:
            chat[b] = body[a]

    if body.get("stream"):
        chat["stream"] = True
        chat["stream_options"] = {"include_usage": True}
    return chat


# ---------------------------------------------------------------- 响应方向
_RESPONSE_SKELETON = {
    "object": "response", "background": False, "completed_at": None,
    "content_filters": None, "error": None, "frequency_penalty": 0.0,
    "incomplete_details": None, "instructions": None, "max_output_tokens": None,
    "max_tool_calls": None, "moderation": None, "parallel_tool_calls": True,
    "presence_penalty": 0.0, "previous_response_id": None,
    "prompt_cache_key": None, "prompt_cache_retention": None,
    "reasoning": {"context": "all_turns", "effort": "high", "mode": "standard",
                  "summary": "detailed"},
    "safety_identifier": None, "service_tier": "auto", "store": False,
    "temperature": 1.0, "text": {"format": {"type": "text"}, "verbosity": "medium"},
    "tool_choice": "auto", "tools": [], "top_logprobs": 0, "top_p": 0.98,
    "truncation": "disabled", "usage": None, "user": None, "metadata": {},
}


def _usage_from_chat(u):
    if not u:
        return None
    pt = u.get("prompt_tokens") or 0
    ct = u.get("completion_tokens") or 0
    det = u.get("completion_tokens_details") or {}
    return {
        "input_tokens": pt, "input_tokens_details": {"cached_tokens": (u.get("prompt_tokens_details") or {}).get("cached_tokens", 0)},
        "output_tokens": ct, "output_tokens_details": {"reasoning_tokens": det.get("reasoning_tokens", 0)},
        "total_tokens": u.get("total_tokens") or (pt + ct),
    }


class ChatStreamBridge:
    """把 Chat 的流式分块翻成 Responses 的 SSE 事件串。"""

    def __init__(self, model, instructions=None, parallel_tool_calls=True):
        self.model = model
        self.resp_id = _rid("resp_")
        self.created = int(time.time())
        self.seq = -1
        self.instructions = instructions
        self.parallel_tool_calls = parallel_tool_calls
        self.text_item_id = None
        self.text_parts = []
        self.text_started = False
        self.text_closed = False
        self.tools = {}          # chat tool_call index -> 状态
        self.order = []          # 输出项顺序：[("message", id)] / [("function_call", index)]
        self.usage = None
        self.finish_reason = None
        self.started = False
        # 思维链（非标字段 reasoning_content）会被丢弃：Codex 只认 output_text。
        # 记数量方便排查"界面一直显示思考中" —— 那是模型在思考，不是卡死。
        self.reasoning_chars = 0

    # ---- 内部 ----
    def _ev(self, etype, **kw):
        self.seq += 1
        d = {"type": etype}
        d.update(kw)
        d["sequence_number"] = self.seq
        return b"event: " + etype.encode() + b"\ndata: " + \
            json.dumps(d, ensure_ascii=False).encode("utf-8") + b"\n\n"

    def _response_obj(self, status, output=None, completed=False):
        r = dict(_RESPONSE_SKELETON)
        r.update({
            "id": self.resp_id, "created_at": self.created, "status": status,
            "model": self.model, "instructions": self.instructions,
            "output": output if output is not None else [],
            "parallel_tool_calls": self.parallel_tool_calls,
            "completed_at": int(time.time()) if completed else None,
            "usage": self.usage if completed else None,
        })
        return r

    def _tool_item(self, st, status="in_progress"):
        return {"id": st["item_id"], "type": "function_call", "status": status,
                "call_id": st["call_id"], "name": st["name"],
                "arguments": st["args"] if status == "completed" else ""}

    # ---- 流式 ----
    def start(self, stream_id=None):
        self.started = True
        obj = self._response_obj("in_progress")
        if stream_id:
            obj["id"] = stream_id
            self.resp_id = stream_id
        out = self._ev("response.created", response=obj)
        out += self._ev("response.in_progress", response=self._response_obj("in_progress"))
        return out

    def _ensure_text_item(self):
        if self.text_started:
            return b""
        self.text_item_id = _rid("msg_")
        self.text_started = True
        self.order.append(("message", self.text_item_id))
        out = self._ev("response.output_item.added", output_index=len(self.order) - 1,
                       item={"id": self.text_item_id, "type": "message",
                             "status": "in_progress", "content": [],
                             "phase": "final_answer", "role": "assistant"})
        out += self._ev("response.content_part.added", item_id=self.text_item_id,
                        output_index=len(self.order) - 1, content_index=0,
                        part={"type": "output_text", "annotations": [], "logprobs": [],
                              "text": ""})
        return out

    def feed(self, chunk):
        """吃一个 chat 分块（dict），吐出要发给 Codex 的字节。"""
        out = b""
        if not chunk:
            return out
        if chunk.get("usage"):
            self.usage = _usage_from_chat(chunk["usage"])
        choices = chunk.get("choices") or []
        if not choices:
            return out
        ch = choices[0]
        delta = ch.get("delta") or {}
        if ch.get("finish_reason"):
            self.finish_reason = ch["finish_reason"]

        rc = delta.get("reasoning_content")
        if rc:
            self.reasoning_chars += len(rc)

        text = delta.get("content")
        if text:
            out += self._ensure_text_item()
            self.text_parts.append(text)
            out += self._ev("response.output_text.delta", item_id=self.text_item_id,
                            output_index=self.order.index(("message", self.text_item_id)),
                            content_index=0, delta=text, logprobs=[])

        for tc in delta.get("tool_calls") or []:
            idx = tc.get("index", 0)
            st = self.tools.get(idx)
            if st is None:
                st = {"item_id": _rid("fc_"), "call_id": tc.get("id") or _rid("call_"),
                      "name": "", "args": "", "started": False}
                self.tools[idx] = st
                self.order.append(("function_call", idx))
            if tc.get("id"):
                st["call_id"] = tc["id"]
            fn = tc.get("function") or {}
            if fn.get("name"):
                st["name"] = fn["name"]
            first = not st["started"]
            if first:
                st["started"] = True
                out += self._ev("response.output_item.added",
                                output_index=self.order.index(("function_call", idx)),
                                item=self._tool_item(st))
            if fn.get("arguments"):
                st["args"] += fn["arguments"]
                out += self._ev("response.function_call_arguments.delta",
                                item_id=st["item_id"],
                                output_index=self.order.index(("function_call", idx)),
                                delta=fn["arguments"])
        return out

    def finish(self):
        """收尾：补齐 done 事件 + response.completed。"""
        out = b""
        for kind, key in self.order:
            if kind == "message":
                oi = self.order.index((kind, key))
                full = "".join(self.text_parts)
                out += self._ev("response.output_text.done", item_id=key, output_index=oi,
                                content_index=0, text=full, logprobs=[])
                out += self._ev("response.content_part.done", item_id=key, output_index=oi,
                                content_index=0,
                                part={"type": "output_text", "annotations": [],
                                      "logprobs": [], "text": full})
                out += self._ev("response.output_item.done", output_index=oi,
                                item={"id": key, "type": "message", "status": "completed",
                                      "content": [{"type": "output_text", "annotations": [],
                                                   "logprobs": [], "text": full}],
                                      "phase": "final_answer", "role": "assistant"})
                self.text_closed = True
            else:
                st = self.tools[key]
                oi = self.order.index((kind, key))
                out += self._ev("response.function_call_arguments.done",
                                item_id=st["item_id"], output_index=oi,
                                arguments=st["args"])
                out += self._ev("response.output_item.done", output_index=oi,
                                item=self._tool_item(st, "completed"))
        output = []
        for kind, key in self.order:
            if kind == "message":
                output.append({"id": key, "type": "message", "status": "completed",
                               "content": [{"type": "output_text", "annotations": [],
                                            "logprobs": [], "text": "".join(self.text_parts)}],
                               "phase": "final_answer", "role": "assistant"})
            else:
                st = self.tools[key]
                output.append(dict(self._tool_item(st, "completed")))
        out += self._ev("response.completed",
                        response=self._response_obj("completed", output=output,
                                                    completed=True))
        return out


# ---------------------------------------------------------------- 非流式
def chat_to_responses(obj, model=None):
    """Chat 的完整响应 → Responses 的完整响应（非流式用）。"""
    br = ChatStreamBridge(model or obj.get("model") or "")
    br.seq = -1
    ch = (obj.get("choices") or [{}])[0]
    msg = ch.get("message") or {}
    text = msg.get("content")
    if text:
        br.feed({"choices": [{"delta": {"content": text}, "finish_reason": None}]})
    for i, tc in enumerate(msg.get("tool_calls") or []):
        br.feed({"choices": [{"delta": {"tool_calls": [dict(tc, index=i)]},
                              "finish_reason": None}]})
    br.finish_reason = ch.get("finish_reason")
    br.usage = _usage_from_chat(obj.get("usage"))
    output = []
    for kind, key in br.order:
        if kind == "message":
            output.append({"id": key, "type": "message", "status": "completed",
                           "content": [{"type": "output_text", "annotations": [],
                                        "logprobs": [], "text": "".join(br.text_parts)}],
                           "phase": "final_answer", "role": "assistant"})
        else:
            st = br.tools[key]
            output.append(br._tool_item(st, "completed"))
    return br._response_obj("completed", output=output, completed=True)
