"""Importing each enricher module triggers its @register(...) decorator,
populating the registry (see registry.py). Local (no heavy network deps)
enrichers are imported unconditionally aside from bot_detection which needs
numpy/dateutil; the transformers/torch-based and server-backed ones are
best-effort so importing `smdt.enrichers` doesn't require every optional
dependency to be installed.
"""

try:
    from . import bot_detection
except ImportError:
    pass

try:
    from . import toxicity
except ImportError:
    pass

try:
    from . import language_detection
except ImportError:
    pass

try:
    from . import sentence_classifier
except ImportError:
    pass

try:
    from . import text_generation
except ImportError:
    pass

try:
    from . import embeddings
except ImportError:
    pass
