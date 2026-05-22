from __future__ import annotations
from dataclasses import dataclass
from typing import List, Literal, Optional
from abc import ABC, abstractmethod

Role = Literal["system", "user", "assistant", "tool"]


@dataclass
class ChatMessage:
    """A single message in a chat conversation.

    Attributes:
        role: Speaker role — one of ``"system"``, ``"user"``, ``"assistant"``, ``"tool"``.
        content: Text content of the message.
    """
    role: Role
    content: str


@dataclass
class GenParams:
    """Inference parameters passed to an LLM adapter.

    Attributes:
        temperature: Sampling temperature (``None`` uses the model default).
        max_tokens: Maximum tokens to generate (``None`` uses the model default).
        top_p: Nucleus sampling probability mass (``None`` uses the model default).
        enable_thinking: Enable extended thinking / chain-of-thought if supported.
    """
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    top_p: Optional[float] = None
    enable_thinking: bool = False


class LLMAdapter(ABC):
    """Abstract base class for LLM provider adapters.

    Implement ``complete`` to wrap any chat-completion API
    (OpenAI-compatible, Anthropic, Ollama, Gemini, etc.).
    """

    @abstractmethod
    async def complete(self, messages: List[ChatMessage], params: Optional[GenParams] = None) -> str: ...


ProviderKind = Literal["openai", "anthropic", "hf-text", "ollama", "gemini"]


@dataclass
class ProviderConfig:
    """Connection configuration for an LLM provider.

    Attributes:
        kind: Provider type — one of ``"openai"``, ``"anthropic"``,
            ``"hf-text"``, ``"ollama"``, ``"gemini"``.
        model: Model name or ID as recognised by the provider.
        base_url: Base URL for OpenAI-compatible or Ollama endpoints.
        api_key: API key for authenticated providers.
        endpoint: Single-prompt endpoint URL (used by ``"hf-text"`` adapters).
    """
    kind: ProviderKind
    model: str
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    endpoint: Optional[str] = None


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
