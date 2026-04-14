# -*- coding: utf-8 -*-
"""Regression tests for LiteLLM pricing aliases and log suppression."""

import sys
import types
import unittest
from unittest.mock import patch

from tests.litellm_stub import ensure_litellm_stub

ensure_litellm_stub()

dotenv_stub = types.ModuleType("dotenv")
dotenv_stub.load_dotenv = lambda *args, **kwargs: None
dotenv_stub.dotenv_values = lambda *args, **kwargs: {}
sys.modules.setdefault("dotenv", dotenv_stub)

from src.agent.llm_adapter import LLMToolAdapter
from src.logging_config import DEFAULT_QUIET_LOGGERS, _configure_litellm_runtime_logging


class TestLiteLLMCustomPricing(unittest.TestCase):
    def test_registers_minimax_m25_highspeed_aliases(self) -> None:
        expected_aliases = {
            "MiniMax-M2.5-highspeed",
            "MiniMax-M2.5-HighSpeed",
            "openai/MiniMax-M2.5-highspeed",
            "openai/MiniMax-M2.5-HighSpeed",
            "minimax/MiniMax-M2.5-highspeed",
            "minimax/MiniMax-M2.5-HighSpeed",
        }

        with patch("src.agent.llm_adapter.litellm.register_model", create=True) as mock_register:
            LLMToolAdapter._register_custom_model_pricing()

        registered_aliases = {
            next(iter(call.args[0].keys()))
            for call in mock_register.call_args_list
            if call.args and isinstance(call.args[0], dict) and call.args[0]
        }

        self.assertTrue(expected_aliases.issubset(registered_aliases))


class TestLiteLLMLoggingSuppression(unittest.TestCase):
    def test_default_quiet_loggers_include_litellm_names(self) -> None:
        self.assertIn("litellm", DEFAULT_QUIET_LOGGERS)
        self.assertIn("LiteLLM", DEFAULT_QUIET_LOGGERS)

    def test_runtime_logging_flags_are_disabled_when_litellm_is_available(self) -> None:
        fake_litellm = types.ModuleType("litellm")
        fake_litellm.set_verbose = True
        fake_litellm.suppress_debug_info = False

        with patch.dict(sys.modules, {"litellm": fake_litellm}):
            _configure_litellm_runtime_logging()

        self.assertFalse(fake_litellm.set_verbose)
        self.assertTrue(fake_litellm.suppress_debug_info)
