try:
    from .post.nlp import server, local
except ImportError:
    pass

from . import account
