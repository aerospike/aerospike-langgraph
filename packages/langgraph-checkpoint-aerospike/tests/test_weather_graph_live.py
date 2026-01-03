# tests/test_weather_graph_live.py
from __future__ import annotations

import os
import re
from copy import deepcopy
from typing import TypedDict, Optional, Dict, Any, List

import pytest
from langgraph.graph import StateGraph, END
from langchain_community.chat_models import ChatOllama
from langchain_core.messages import (
    SystemMessage,
    HumanMessage,
    AIMessage,
    ToolMessage,
    BaseMessage,
)
from langchain_core.tools import tool
from langchain_core.runnables import RunnableLambda
from langchain_core.messages import ToolMessage

# ----------------------------
# State definition for graph
# ----------------------------

class AgentState(TypedDict):
    messages: List[BaseMessage]
    tool_called: bool
    pending_tool_call: Optional[Dict[str, Any]]


# ----------------------------
# Fake tool: get_weather
# ----------------------------

@tool
def get_weather(location: str) -> str:
    """Fake weather tool. Returns a pretend forecast for a given city."""
    return f"The weather in {location} is sunny and 72°F (fake data)."


# ----------------------------
# Local LLM (Ollama only)
# ----------------------------

def build_llm() -> ChatOllama:
    """
    Build a local LLM using Ollama.
    Uses OLLAMA_MODEL env var if set, otherwise 'llama3.2'.
    """
    model_name = os.getenv("OLLAMA_MODEL", "llama3.2")
    return ChatOllama(model=model_name, temperature=0.2)


# ----------------------------
# Heuristic for weather questions
# ----------------------------

def maybe_extract_weather_location(messages: List[BaseMessage]) -> Optional[str]:
    """
    Look at the last human message and try to extract a location from:
        "weather in Paris"
        "what's the weather in new york?"
    """
    last_human: Optional[HumanMessage] = None
    for m in reversed(messages):
        if isinstance(m, HumanMessage):
            last_human = m
            break

    if not last_human:
        return None

    text = last_human.content.strip()
    match = re.search(r"weather in ([^?.!]+)", text, flags=re.IGNORECASE)
    if not match:
        return None

    location = match.group(1).strip()
    return location.title() if location else None


# ----------------------------
# Node: model (decide if we need tool)
# ----------------------------

def call_model(state: AgentState) -> AgentState:
    llm = build_llm()
    messages = state["messages"]

    system = SystemMessage(
        content=(
            "You are a helpful assistant. "
            "The user may ask about the weather in a city. "
            "If you are given weather info from a tool, use it. "
            "Otherwise, answer normally."
        )
    )

    response: AIMessage = llm.invoke([system] + messages)

    # Local LLM has no native tool-calling, so we use a heuristic
    pending_tool_call: Optional[Dict[str, Any]] = None
    location = maybe_extract_weather_location(messages)
    if location:
        pending_tool_call = {
            "id": "local-fallback-get_weather",
            "name": "get_weather",
            "args": {"location": location},
        }

    tool_called = pending_tool_call is not None

    return {
        "messages": messages + [response],
        "tool_called": tool_called,
        "pending_tool_call": pending_tool_call,
    }


# ----------------------------
# Node: tools (execute get_weather)
# ----------------------------

def call_tools(state: AgentState) -> AgentState:
    messages = state["messages"]
    pending = state.get("pending_tool_call")

    if not pending:
        # No tool call requested
        return state

    name = pending.get("name")
    args = pending.get("args", {})

    if name == "get_weather":
        location = args.get("location", "somewhere")
        result = get_weather.invoke({"location": location})

        tool_msg = ToolMessage(
            content=result,
            name="get_weather",
            tool_call_id=pending.get("id", "fallback-tool-call-id"),
        )
        messages = messages + [tool_msg]

    return {
        "messages": messages,
        "tool_called": True,
        "pending_tool_call": None,
    }


# ----------------------------
# Node: model_after_tools (final answer)
# ----------------------------

def call_model_after_tools(state: AgentState) -> AgentState:
    llm = build_llm()
    messages = state["messages"]

    # Convert ToolMessage → HumanMessage so Ollama can handle it
    converted: List[BaseMessage] = []
    for m in messages:
        if isinstance(m, ToolMessage):
            # Rephrase tool output as if the user is giving you context
            converted.append(
                HumanMessage(
                    content=f"Tool `{m.name}` returned this information: {m.content}"
                )
            )
        else:
            converted.append(m)

    system = SystemMessage(
        content=(
            "You are a helpful assistant. "
            "You may see information that originally came from tools such as `get_weather`. "
            "Use that information to answer the user's question clearly."
        )
    )

    response: AIMessage = llm.invoke([system] + converted)

    return {
        "messages": messages + [response],
        "tool_called": state["tool_called"],
        "pending_tool_call": None,
    }


# ----------------------------
# Graph builder
# ----------------------------

def _build_weather_graph(checkpointer):
    g = StateGraph(AgentState)
    g.add_node("model", RunnableLambda(call_model))
    g.add_node("tools", RunnableLambda(call_tools))
    g.add_node("model_after_tools", RunnableLambda(call_model_after_tools))

    g.set_entry_point("model")

    def route_after_model(state: AgentState):
        return "tools" if state["tool_called"] else "model_after_tools"

    g.add_conditional_edges("model", route_after_model)
    g.add_edge("tools", "model_after_tools")
    g.add_edge("model_after_tools", END)

    return g.compile(checkpointer=checkpointer)


# ----------------------------
# Pytest: live graph + AerospikeSaver
# ----------------------------

def test_weather_graph_runs_and_persists_checkpoint(saver, cfg_base):
    # Build LangGraph app with your AerospikeSaver
    app = _build_weather_graph(saver)

    # Use a separate checkpoint_ns to keep it clean
    cfg = deepcopy(cfg_base)
    cfg["configurable"]["checkpoint_ns"] = "weather-demo"

    initial_state: AgentState = {
        "messages": [
            HumanMessage(content="Hi, what is the weather in Paris today?")
        ],
        "tool_called": False,
        "pending_tool_call": None,
    }

    # Try running; if Ollama isn't running, skip the test instead of failing everything
    try:
        out = app.invoke(initial_state, cfg)
    except Exception as e:
        msg = str(e).lower()
        if "connection refused" in msg or "failed to establish" in msg or "connection error" in msg:
            pytest.skip(f"Ollama not running or unreachable: {e}")
        raise

    # Basic sanity checks on output
    assert "messages" in out
    assert isinstance(out["messages"], list)
    assert len(out["messages"]) >= 2  # at least human + final AI

    # Make sure we got some AI response that mentions weather or Paris
    final_contents = " ".join(
        m.content
        for m in out["messages"]
        if isinstance(m, (AIMessage, ToolMessage))
    ).lower()
    assert "weather" in final_contents or "paris" in final_contents

    # Verify a checkpoint exists
    latest = saver.get_tuple(cfg)
    assert latest is not None
    assert isinstance(latest.checkpoint, dict)

    # And that there is some timeline history
    timeline = list(saver.list(cfg, limit=5))
    assert len(timeline) >= 1
