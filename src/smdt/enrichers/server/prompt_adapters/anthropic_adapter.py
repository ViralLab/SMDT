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

    async def complete(self, messages: List[ChatMessage], params: GenParams) -> str:
        system, chat = self._split_system(messages)
        resp = await self.client.messages.create(
            model=self.model,
            system=system,
            messages=chat,
            max_tokens=params.max_tokens,
            temperature=params.temperature,
            top_p=params.top_p,
        )
        texts = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
        return "\n".join(texts).strip()
