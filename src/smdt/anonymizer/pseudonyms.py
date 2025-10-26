from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional
import hashlib

try:
    import whirlpool  # type: ignore

    _HAVE_WHIRLPOOL = True
except Exception:  # pragma: no cover
    whirlpool = None
    _HAVE_WHIRLPOOL = False


class Algorithm(str, Enum):
    SHA256 = "sha256"
    SHA512 = "sha512"
    WHIRLPOOL = "whirlpool"
    MD5 = "md5"  # discouraged; allowed for compatibility


@dataclass(frozen=True)
class Pseudonymizer:
    """Deterministic, keyed pseudonymization via HMAC-like construction.

    We *always* use a secret pepper. Even if the user chooses MD5/Whirlpool/SHAx,
    we wrap it with a keyed construction to avoid rainbow-table reversals.

    - NULL handling: caller must avoid passing None if they want a value; we
      deliberately return None for None inputs.
    - Unicode normalization and case policy are applied *before* hashing via a
      normalizer callable.
    """

    algo: Algorithm
    pepper: bytes
    output_hex_len: int = 64
    normalizer: Optional[Callable[[str], str]] = None

    def _h(self):
        if self.algo == Algorithm.SHA256:
            return hashlib.sha256
        if self.algo == Algorithm.SHA512:
            return hashlib.sha512
        if self.algo == Algorithm.MD5:
            return hashlib.md5
        if self.algo == Algorithm.WHIRLPOOL:
            if not _HAVE_WHIRLPOOL:
                raise RuntimeError(
                    "Whirlpool selected but whirlpool-hash is not installed."
                )
            # whirlpool.new(data, key=None) → we emulate HMAC below
            return None  # special case
        raise ValueError(f"Unsupported algo: {self.algo}")

    def _normalize(self, s: str) -> str:
        if not s:
            return s
        x = s.strip()
        return self.normalizer(x) if self.normalizer else x

    def _hmac_hex(self, msg: str) -> str:
        """Keyed hashing (HMAC-like) that works for all algos incl. whirlpool.
        We avoid the stdlib hmac to keep the Whirlpool path consistent.
        """
        if msg is None:  # type: ignore[unreachable]
            return None  # pragma: no cover
        data = self._normalize(msg).encode("utf-8", errors="surrogatepass")
        # Simple HMAC-like: hash( pepper || 0x00 || data || 0xFF ) then hash again
        # to mix and expand. This is sufficient for pseudonymization.
        hfn = self._h()
        if self.algo == Algorithm.WHIRLPOOL:
            d1 = whirlpool.new(self.pepper + b"\x00" + data + b"\xff").digest()
            d2 = whirlpool.new(self.pepper + b"\x01" + d1 + b"\xee").hexdigest()
        else:
            d1 = hfn(self.pepper + b"\x00" + data + b"\xff").digest()
            d2 = hfn(self.pepper + b"\x01" + d1 + b"\xee").hexdigest()
        return d2[: self.output_hex_len]

    def make(self, value: Optional[str]) -> Optional[str]:
        """Return pseudonymized hex string, or None if input is None.
        Empty string remains empty string to preserve semantics.
        """
        if value is None:
            return None
        if value == "":
            return ""
        return self._hmac_hex(value)
