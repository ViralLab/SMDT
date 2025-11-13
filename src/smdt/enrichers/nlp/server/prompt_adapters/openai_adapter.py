from __future__ import annotations
from typing import List, Dict

try:
    from openai import AsyncOpenAI
except ImportError:
    raise ImportError(
        "openai package is not installed. Please install it with `pip install openai`)."
    )
from .base import LLMAdapter, ChatMessage, GenParams


class OpenAIChatAdapter(LLMAdapter):
    def __init__(self, base_url: str, api_key: str, model: str):
        self.client = AsyncOpenAI(base_url=base_url, api_key=api_key)
        self.model = model

    @staticmethod
    def _to_openai_messages(msgs: List[ChatMessage]) -> List[Dict[str, str]]:
        out: List[Dict[str, str]] = []
        for m in msgs:
            role = m.role if m.role in {"system", "user", "assistant"} else "assistant"
            content = m.content if m.role != "tool" else f"[TOOL]\n{m.content}"
            out.append({"role": role, "content": content})
        return out

    async def complete(self, messages: List[ChatMessage], params: GenParams) -> str:
        resp = await self.client.chat.completions.create(
            model=self.model,
            messages=self._to_openai_messages(messages),
            temperature=params.temperature,
            max_tokens=params.max_tokens,
            top_p=params.top_p,
        )
        return (resp.choices[0].message.content or "").strip()
