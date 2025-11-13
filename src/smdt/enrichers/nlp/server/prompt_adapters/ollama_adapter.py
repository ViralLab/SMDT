from __future__ import annotations
from typing import List, Dict

try:
    import aiohttp
except ImportError:
    raise ImportError(
        "aiohttp package is not installed. Please install it with `pip install aiohttp`."
    )
from .base import LLMAdapter, ChatMessage, GenParams


class OllamaAdapter(LLMAdapter):
    def __init__(self, model: str, base_url: str = "http://localhost:11434"):
        self.model = model
        self.base_url = base_url

    def _to_ollama_messages(self, msgs: List[ChatMessage]) -> List[Dict[str, str]]:
        out: List[Dict[str, str]] = []
        sys = "\n".join([m.content for m in msgs if m.role == "system"]).strip()
        injected = False
        for m in msgs:
            if m.role == "user":
                content = f"{sys}\n\n{m.content}" if sys and not injected else m.content
                injected = True
                out.append({"role": "user", "content": content})
            elif m.role == "assistant":
                out.append({"role": "assistant", "content": m.content})
            elif m.role == "tool":
                out.append({"role": "assistant", "content": f"[TOOL]\n{m.content}"})
        return out

    async def complete(self, messages: List[ChatMessage], params: GenParams) -> str:
        payload = {
            "model": self.model,
            "messages": self._to_ollama_messages(messages),
            "options": {
                "temperature": params.temperature,
                "num_predict": params.max_tokens,
                "top_p": params.top_p,
            },
            "stream": False,
        }
        async with aiohttp.ClientSession() as sess:
            async with sess.post(f"{self.base_url}/api/chat", json=payload) as r:
                data = await r.json()
        return data.get("message", {}).get("content", "").strip()
