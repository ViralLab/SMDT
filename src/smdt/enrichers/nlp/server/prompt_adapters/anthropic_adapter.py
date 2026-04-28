from __future__ import annotations
from typing import List, Dict, Optional

try:
    from anthropic import AsyncAnthropic
except ImportError:
    raise ImportError(
        "anthropic package is not installed. Please install it with `pip install anthropic`."
    )
from .base import LLMAdapter, ChatMessage, GenParams


class AnthropicAdapter(LLMAdapter):
    def __init__(self, api_key: str, model: str):
        self.client = AsyncAnthropic(api_key=api_key)
        self.model = model

    @staticmethod
    def _split_system(
        msgs: List[ChatMessage],
    ) -> tuple[Optional[str], List[Dict[str, str]]]:
        system = None
        chat: List[Dict[str, str]] = []
        for m in msgs:
            if m.role == "system":
                system = (system + "\n" if system else "") + m.content
            elif m.role in {"user", "assistant"}:
                chat.append({"role": m.role, "content": m.content})
            elif m.role == "tool":
                chat.append({"role": "assistant", "content": f"[TOOL]\n{m.content}"})
        return system, chat

    async def complete(self, messages: List[ChatMessage], params: Optional[GenParams] = None) -> str:
        params = params or GenParams()
        system, chat = self._split_system(messages)
        kwargs = {
            "model": self.model,
            "system": system,
            "messages": chat,
            "max_tokens": params.max_tokens if params.max_tokens is not None else 4096,
        }
        
        if params.top_p is not None:
            kwargs["top_p"] = params.top_p
        
        if params.enable_thinking:
            # Anthropic requires budget_tokens >= 1024 and max_tokens > budget_tokens
            budget = max(1024, kwargs["max_tokens"] - 1)
            kwargs["max_tokens"] = max(kwargs["max_tokens"], budget + 1)
            
            kwargs["thinking"] = {
                "type": "enabled",
                "budget_tokens": budget
            }
            # Anthropic requires temperature=1.0 when thinking is enabled
            kwargs["temperature"] = 1.0
        else:
            if params.temperature is not None:
                kwargs["temperature"] = params.temperature

        resp = await self.client.messages.create(**kwargs)
        texts = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
        return "\n".join(texts).strip()
