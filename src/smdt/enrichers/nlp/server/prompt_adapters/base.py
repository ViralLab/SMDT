from __future__ import annotations
from dataclasses import dataclass
from typing import List, Literal, Optional
from abc import ABC, abstractmethod

Role = Literal["system", "user", "assistant", "tool"]


@dataclass
class ChatMessage:
    role: Role
    content: str


@dataclass
class GenParams:
    temperature: float = 0.2
    max_tokens: int = 256
    top_p: float = 1.0


class LLMAdapter(ABC):
    @abstractmethod
    async def complete(self, messages: List[ChatMessage], params: GenParams) -> str: ...


ProviderKind = Literal["openai", "anthropic", "hf-text", "ollama", "gemini"]


@dataclass
class ProviderConfig:
    kind: ProviderKind
    model: str
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    endpoint: Optional[str] = None  # for hf-text single-prompt endpoints


def make_adapter(cfg: "ProviderConfig") -> LLMAdapter:
    if cfg.kind == "openai":
        from .openai_adapter import OpenAIChatAdapter

        return OpenAIChatAdapter(
            base_url=cfg.base_url or "", api_key=cfg.api_key or "", model=cfg.model
        )
    if cfg.kind == "anthropic":
        from .anthropic_adapter import AnthropicAdapter

        return AnthropicAdapter(api_key=cfg.api_key or "", model=cfg.model)
    if cfg.kind == "hf-text":
        from .hf_text_adapter import HFTextGenAdapter

        return HFTextGenAdapter(endpoint=cfg.endpoint or "", api_key=cfg.api_key)
    if cfg.kind == "ollama":
        from .ollama_adapter import OllamaAdapter

        return OllamaAdapter(
            model=cfg.model, base_url=cfg.base_url or "http://localhost:11434"
        )
    if cfg.kind == "gemini":
        from .gemini_adapter import GeminiAdapter

        return GeminiAdapter(api_key=cfg.api_key or "", model=cfg.model)

    raise ValueError(f"Unknown provider kind: {cfg.kind}")
