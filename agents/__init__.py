"""Acme Property Services Agent Modules.

Pure logic modules containing PROMPTS and DECISION LOGIC ONLY.
All MCP connections are managed by main.py orchestrator.
"""
from agents.dispatcher import get_system_instruction as get_dispatcher_instruction
from agents.finance import get_system_instruction as get_finance_instruction
from agents.ops import get_system_instruction as get_ops_instruction

__all__ = [
    "get_dispatcher_instruction",
    "get_finance_instruction", 
    "get_ops_instruction",
]
