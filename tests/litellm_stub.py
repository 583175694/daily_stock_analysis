# -*- coding: utf-8 -*-
"""
Shared test helper to ensure imports work when litellm is unavailable.
"""

import importlib.util
import sys
import types


def ensure_litellm_stub() -> None:
    """Install a minimal litellm stub only when litellm is unavailable."""
    if "litellm" in sys.modules:
        return

    try:
        if importlib.util.find_spec("litellm") is not None:
            return
    except ValueError:
        # A previously injected incomplete stub may leave __spec__ unset.
        pass

    litellm_stub = types.ModuleType("litellm")

    class _DummyRouter:  # pragma: no cover
        pass

    litellm_stub.Router = _DummyRouter
    litellm_stub.register_model = lambda *_args, **_kwargs: None
    litellm_stub.completion = lambda **kwargs: None
    litellm_stub.set_verbose = False
    litellm_stub.suppress_debug_info = True
    sys.modules["litellm"] = litellm_stub
