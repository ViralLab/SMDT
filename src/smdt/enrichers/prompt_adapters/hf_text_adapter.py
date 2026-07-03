from __future__ import annotations
from typing import List, Optional, Dict, Any

try:
    import aiohttp
except ImportError:
    raise ImportError(
        "aiohttp package is not installed. Please install it with `pip install aiohttp`."
    )
from .base import LLMAdapter, ChatMessage, GenParams


class HFTextGenAdapter(LLMAdapter):
    def __init__(self, endpoint: str, api_key: Optional[str] = None):
        self.endpoint = endpoint
        self.api_key = api_key

    @staticmethod
    def _collapse(messages: List[ChatMessage]) -> str:
        sys = "\n".join([m.content for m in messages if m.role == "system"]).strip()
        usr = "\n\n".join([m.content for m in messages if m.role == "user"]).strip()
        hist = "\n\n".join(
            [m.content for m in messages if m.role == "assistant"]
        ).strip()
        pre = ""
        if sys:
            pre += f"<System>\n{sys}\n</System>\n"
        if hist:
            pre += f"<History>\n{hist}\n</History>\n"
        pre += f"<User>\n{usr}\n</User>\nAssistant:"
        return pre

    async def complete(self, messages: List[ChatMessage], params: Optional[GenParams] = None) -> str:
        params = params or GenParams()
        prompt = self._collapse(messages)
        
        parameters = {}
        if params.max_tokens is not None:
            parameters["max_new_tokens"] = params.max_tokens
        if params.temperature is not None:
            parameters["temperature"] = params.temperature
        if params.top_p is not None:
            parameters["top_p"] = params.top_p

        payload: Dict[str, Any] = {
            "inputs": prompt,
        }
        if parameters:
            payload["parameters"] = parameters
        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        async with aiohttp.ClientSession() as sess:
            async with sess.post(self.endpoint, json=payload, headers=headers) as r:
                data = await r.json()
        if isinstance(data, list) and data and "generated_text" in data[0]:
            return data[0]["generated_text"].split("Assistant:", 1)[-1].strip()
        if "generated_text" in data:
            return data["generated_text"].strip()
        if "text" in data:
            return data["text"].strip()
        return str(data)
