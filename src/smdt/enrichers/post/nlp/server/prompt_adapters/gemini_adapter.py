from __future__ import annotations
from typing import List, Optional
from google.genai.types import Candidate

try:
    import google.genai as genai
except ImportError:
    raise ImportError(
        "google-genai package is not installed. Please install it with `pip install google-genai`."
    )

from .base import LLMAdapter, ChatMessage, GenParams


class GeminiAdapter(LLMAdapter):
    def __init__(self, api_key: str, model: str):
        self.api_key = api_key
        self.model = model

    @staticmethod
    def _to_gemini_content(msgs: List[ChatMessage]) -> str:
        """Convert ChatMessage list to a single content string for Gemini.

        Since Gemini's simple API works with a single prompt, we'll combine
        all messages into a single coherent prompt.
        """
        content_parts = []

        for m in msgs:
            if m.role == "system":
                content_parts.append(f"System: {m.content}")
            elif m.role == "user":
                content_parts.append(f"User: {m.content}")
            elif m.role == "assistant":
                content_parts.append(f"Assistant: {m.content}")
            elif m.role == "tool":
                content_parts.append(f"Tool: {m.content}")

        return {"text": "\n\n".join(content_parts)}

    async def complete(self, messages: List[ChatMessage], params: Optional[GenParams] = None) -> str:
        """Generate completion using Gemini API."""
        params = params or GenParams()
        content = self._to_gemini_content(messages)

        config = {}
        if params.temperature is not None:
            config["temperature"] = params.temperature
        if params.max_tokens is not None:
            config["max_output_tokens"] = params.max_tokens
        if params.top_p is not None:
            config["top_p"] = params.top_p

        if params.enable_thinking:
            budget = max(1024, params.max_tokens - 1) if params.max_tokens is not None else 1024
            if "max_output_tokens" in config:
                config["max_output_tokens"] = max(config["max_output_tokens"], budget + 1)
            config["thinking_config"] = {"thinking_budget_tokens": budget}

        try:
            async with genai.Client(api_key=self.api_key).aio as aclient:
                response = await aclient.models.generate_content(
                    model=self.model,
                    contents=content,
                    config=config,
                )

                # Extract text from response with proper null checks
                if hasattr(response, "text") and response.text is not None:
                    result = response.text.strip()
                    return result
                elif hasattr(response, "candidates") and response.candidates:
                    candidate: Candidate = response.candidates[0]
                    if hasattr(candidate, "content") and candidate.content:
                        if (
                            hasattr(candidate.content, "parts")
                            and candidate.content.parts
                        ):
                            text_parts = [
                                part.text
                                for part in candidate.content.parts
                                if hasattr(part, "text") and part.text is not None
                            ]
                            if text_parts:
                                result = "\n".join(text_parts).strip()
                                return result
                        elif (
                            hasattr(candidate.content, "text")
                            and candidate.content.text is not None
                        ):
                            result = candidate.content.text.strip()
                            return result

                print(
                    f"[DEBUG] No valid text found in Gemini response, returning empty string"
                )
                return ""

        except Exception as e:
            # Log error and re-raise with more context
            raise RuntimeError(f"Gemini API error: {str(e)}") from e
