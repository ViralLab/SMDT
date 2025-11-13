from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
import json

try:
    import yaml
except Exception:
    yaml = None

from .prompt_adapters.base import ChatMessage


@dataclass
class PromptTemplate:
    id: str
    system: str
    user: str
    defaults: Dict[str, Any]

    def render(self, **kwargs: Any) -> List[ChatMessage]:
        ctx = {**self.defaults, **kwargs}
        system = self.system.format(**ctx)
        user = self.user.format(**ctx)
        return [
            ChatMessage(role="system", content=system),
            ChatMessage(role="user", content=user),
        ]

    @staticmethod
    def _load_data(p: Path) -> Dict[str, Any]:
        text = p.read_text(encoding="utf-8")
        if p.suffix.lower() in {".yaml", ".yml"}:
            if yaml is None:
                raise RuntimeError(
                    "PyYAML not installed; cannot read YAML prompt files."
                )
            return yaml.safe_load(text)
        return json.loads(text)

    @classmethod
    def from_file(cls, path: str, prompt_id: Optional[str] = None) -> "PromptTemplate":
        p = Path(path)
        data = cls._load_data(p)
        prompts = data.get("prompts", {})
        if not prompts:
            raise ValueError("No 'prompts' key found in prompt file.")
        key = prompt_id or data.get("default")
        if key is None:
            key = next(iter(prompts.keys())) if len(prompts) == 1 else None
        if key is None or key not in prompts:
            raise ValueError(f"Prompt id '{prompt_id}' not found and no default set.")
        spec = prompts[key]
        return cls(
            id=key,
            system=(spec.get("system") or "").strip(),
            user=(spec.get("user") or "").strip(),
            defaults=spec.get("defaults", {}) or {},
        )
