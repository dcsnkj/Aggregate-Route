#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""本地抓包服务：捕获 Codex 发出的**真实请求体**。

用途：判断 Codex 到底往上游发了什么。
Codex 的 RUST_LOG=trace 只记状态码、不记 request body，想看原始 body 只能这样抓。

原理：把 Codex 临时指向本机这个 HTTP 服务，它会把你发的请求体原样落盘，
再用一份现成的 SSE 回包让 Codex 正常收尾（所以 Codex 那边不会报错）。
用 `-c` 传 inline table 就能临时加一个 provider，**不用改 config.toml**。

用法：
    1) 起服务:  python capture_request.py [端口，默认 8899]
    2) 让 Codex 指向它:
       codex exec --skip-git-repo-check -c model_provider='"capture"' \\
         -c 'model_providers.capture={name="Capture",base_url="http://127.0.0.1:8899/v1",wire_api="responses",requires_openai_auth=true}' \\
         -m deepseek-flash "hi"
    3) 看结果:  tmp/captured_request.json
"""
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8899
HERE = Path(__file__).resolve().parent.parent / "tmp"
HERE.mkdir(parents=True, exist_ok=True)
SSE_FILE = HERE / "sse_sample.txt"
OUT_FILE = HERE / "captured_request.json"

CAPTURED = []


def canned_sse():
    if SSE_FILE.exists():
        return SSE_FILE.read_bytes()
    return b'event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_x","object":"response","status":"completed","output":[]}}\n\n'


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _record(self, body):
        entry = {
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            "method": self.command,
            "path": self.path,
            "headers": {k: ("<redacted>" if k.lower() == "authorization" else v)
                        for k, v in self.headers.items()},
            "body": None,
        }
        if body:
            try:
                entry["body"] = json.loads(body.decode("utf-8", "replace"))
            except Exception:
                entry["body"] = body.decode("utf-8", "replace")[:2000]
        CAPTURED.append(entry)
        OUT_FILE.write_text(json.dumps(CAPTURED, ensure_ascii=False, indent=2), encoding="utf-8")

    def do_GET(self):
        self._record(None)
        if self.path.rstrip("/").endswith("/models"):
            payload = json.dumps({"object": "list", "data": [
                {"id": "deepseek-flash", "object": "model"},
                {"id": "gpt-6-astra", "object": "model"},
            ]}).encode()
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        self.send_response(404)
        self.send_header("content-length", "0")
        self.end_headers()

    def do_POST(self):
        n = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(n) if n else b""
        self._record(body)
        data = canned_sse()
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("cache-control", "no-cache")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    print(f"capture server on http://127.0.0.1:{PORT}  -> {OUT_FILE}")
    HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
