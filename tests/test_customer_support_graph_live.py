# tests/test_customer_support_graph_live.py
from __future__ import annotations

import os
import re
import sqlite3
from copy import deepcopy
from datetime import datetime, date
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


# ----------------------------
# State definition
# ----------------------------

class SupportState(TypedDict):
    messages: List[BaseMessage]
    issue_type: str  # "flight" | "refund" | "hotel" | "car" | "excursions" | "general" | ""
    itinerary: Dict[str, Any]
    lodging: Dict[str, Any]
    car_rental: Dict[str, Any]
    excursions: Dict[str, Any]


# ----------------------------
# DB helpers
# ----------------------------

DB_PATH = os.getenv("TRAVEL_DB_PATH", "travel2.sqlite")


def _connect():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(
            f"travel DB not found at {DB_PATH}. "
            "Set TRAVEL_DB_PATH or place travel2.sqlite in repo root."
        )
    return sqlite3.connect(DB_PATH)


# ----------------------------
# Tools (real, SQLite-backed)
# ----------------------------

@tool
def fetch_user_flight_information(passenger_id: str) -> List[Dict[str, Any]]:
    """
    Fetch all tickets for a passenger along with flight + seat info.
    Note: passenger_id must exist in the travel DB.
    """
    conn = _connect()
    cur = conn.cursor()
    query = """
    SELECT
        t.ticket_no, t.book_ref,
        f.flight_id, f.flight_no, f.departure_airport, f.arrival_airport,
        f.scheduled_departure, f.scheduled_arrival,
        bp.seat_no, tf.fare_conditions
    FROM
        tickets t
        JOIN ticket_flights tf ON t.ticket_no = tf.ticket_no
        JOIN flights f ON tf.flight_id = f.flight_id
        JOIN boarding_passes bp
            ON bp.ticket_no = t.ticket_no AND bp.flight_id = f.flight_id
    WHERE
        t.passenger_id = ?
    """
    cur.execute(query, (passenger_id,))
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    results = [dict(zip(cols, r)) for r in rows]
    cur.close()
    conn.close()
    return results


@tool
def search_flights(origin: str, destination: str, date: str) -> List[Dict[str, Any]]:
    """
    Real flight search tool backed by the travel DB.

    origin/destination are IATA airport codes (e.g. 'SFO', 'JFK').
    date is 'YYYY-MM-DD' (we search flights on that calendar day).
    """
    conn = _connect()
    cur = conn.cursor()

    start_dt = datetime.fromisoformat(date)
    end_dt = start_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

    query = """
    SELECT
        flight_id, flight_no, departure_airport, arrival_airport,
        scheduled_departure, scheduled_arrival
    FROM flights
    WHERE
        departure_airport = ?
        AND arrival_airport = ?
        AND scheduled_departure BETWEEN ? AND ?
    LIMIT 20
    """
    cur.execute(
        query,
        (
            origin,
            destination,
            start_dt.isoformat(" "),
            end_dt.isoformat(" "),
        ),
    )
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    results = [dict(zip(cols, r)) for r in rows]
    cur.close()
    conn.close()
    return results


@tool
def cancel_ticket(ticket_no: str) -> str:
    """
    Cancel a ticket by deleting its ticket_flights entries.
    (Simplified version for demo.)
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM ticket_flights WHERE ticket_no = ?", (ticket_no,))
    conn.commit()
    deleted = cur.rowcount
    cur.close()
    conn.close()
    if deleted > 0:
        return f"Ticket {ticket_no} successfully cancelled."
    return f"No ticket found with number {ticket_no}."


@tool
def update_ticket_to_new_flight(ticket_no: str, new_flight_id: int) -> str:
    """
    Update a ticket to use a different flight.
    (Simplified version: no time / ownership checks.)
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "UPDATE ticket_flights SET flight_id = ? WHERE ticket_no = ?",
        (new_flight_id, ticket_no),
    )
    conn.commit()
    updated = cur.rowcount
    cur.close()
    conn.close()
    if updated > 0:
        return f"Ticket {ticket_no} updated to flight {new_flight_id}."
    return f"No ticket found with number {ticket_no}."


@tool
def search_hotels(location: str, nights: int) -> List[Dict[str, Any]]:
    """
    Real hotel search tool backed by travel DB.
    nights is currently unused in the query but kept for prompt clarity.
    """
    conn = _connect()
    cur = conn.cursor()
    query = """
    SELECT id, name, location, price_tier
    FROM hotels
    WHERE location LIKE ?
    LIMIT 20
    """
    cur.execute(query, (f"%{location}%",))
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    results = [dict(zip(cols, r)) for r in rows]
    cur.close()
    conn.close()
    return results


@tool
def book_hotel(hotel_id: int) -> str:
    """
    Book a hotel by id.
    Assumes 'booked' INTEGER column exists in the hotels table (0/1).
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE hotels SET booked = 1 WHERE id = ?", (hotel_id,))
        conn.commit()
        ok = cur.rowcount > 0
    finally:
        cur.close()
        conn.close()

    if ok:
        return f"Hotel {hotel_id} successfully booked."
    return f"No hotel found with ID {hotel_id}."


@tool
def search_cars(location: str, days: int) -> List[Dict[str, Any]]:
    """
    Real car rental search tool backed by travel DB.
    days is currently unused in the query but kept for prompt clarity.
    """
    conn = _connect()
    cur = conn.cursor()
    query = """
    SELECT id, name, location, price_tier, booked
    FROM car_rentals
    WHERE location LIKE ?
    LIMIT 20
    """
    cur.execute(query, (f"%{location}%",))
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    results = [dict(zip(cols, r)) for r in rows]
    cur.close()
    conn.close()
    return results


@tool
def book_car_rental(rental_id: int) -> str:
    """
    Book a car rental by id (sets booked=1).
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE car_rentals SET booked = 1 WHERE id = ?", (rental_id,))
        conn.commit()
        ok = cur.rowcount > 0
    finally:
        cur.close()
        conn.close()

    if ok:
        return f"Car rental {rental_id} successfully booked."
    return f"No car rental found with ID {rental_id}."


@tool
def search_excursions(
    location: Optional[str] = None,
    name: Optional[str] = None,
    interests: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Search for excursions (trip recommendations) using the trip_recommendations table.

    This is equivalent in spirit to search_trip_recommendations:
    - Filter by location (LIKE)
    - Optionally filter by name (LIKE)
    - Optionally filter by interests/keywords (LIKE on 'keywords' column)
    """
    conn = _connect()
    cur = conn.cursor()

    query = """
    SELECT id, name, location, keywords, details, booked
    FROM trip_recommendations
    WHERE 1=1
    """
    params: List[Any] = []

    if location:
        query += " AND location LIKE ?"
        params.append(f"%{location}%")

    if name:
        query += " AND name LIKE ?"
        params.append(f"%{name}%")

    if interests:
        # e.g. "museum, wine" -> ["museum", "wine"]
        interest_list = [kw.strip() for kw in interests.split(",") if kw.strip()]
        if interest_list:
            keyword_conditions = " OR ".join("keywords LIKE ?" for _ in interest_list)
            query += f" AND ({keyword_conditions})"
            params.extend([f"%{kw}%" for kw in interest_list])

    query += " LIMIT 20"

    cur.execute(query, params)
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    results = [dict(zip(cols, r)) for r in rows]

    cur.close()
    conn.close()
    return results


@tool
def book_excursion(recommendation_id: int) -> str:
    """
    Book an excursion (trip recommendation) by its ID.
    Operates on the trip_recommendations table (sets booked=1).
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        # Mark as booked
        cur.execute(
            "UPDATE trip_recommendations SET booked = 1 WHERE id = ?",
            (recommendation_id,),
        )
        conn.commit()

        if cur.rowcount == 0:
            return f"No trip recommendation found with ID {recommendation_id}."

        # Fetch details to make the response nicer
        cur.execute(
            "SELECT id, name, location FROM trip_recommendations WHERE id = ?",
            (recommendation_id,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if not row:
        return f"Trip recommendation {recommendation_id} was not found after booking."

    _, name, location = row
    return f"Trip recommendation {recommendation_id} ('{name}' in {location}) successfully booked."


@tool
def search_trip_recommendations(location: str, category: str) -> List[Dict[str, Any]]:
    """
    Convenience wrapper to search excursions/trip recommendations
    by location + category/interest keywords.
    """
    return search_excursions.invoke({"location": location, "interests": category})


@tool
def lookup_policy(topic: str) -> str:
    """
    Still a simple, non-DB policy tool.
    You could later replace this with a vector-search/FAQ-based implementation.
    """
    t = topic.lower()
    if "refund" in t or "cancel" in t or "cancellation" in t:
        return (
            "Refund policy: You can cancel up to 24h before departure for a full "
            "refund. Within 24h, a change or cancellation fee may apply."
        )
    return (
        "General policy: Changes depend on your fare class. Basic economy has "
        "more restrictions than flexible tickets."
    )


# ----------------------------
# LLM (Ollama only)
# ----------------------------

def build_llm() -> ChatOllama:
    """
    Build a local LLM using Ollama.
    Uses OLLAMA_MODEL env var if set, otherwise 'llama3.2'.
    """
    model_name = os.getenv("OLLAMA_MODEL", "llama3.2")
    return ChatOllama(model=model_name, temperature=0.2)


# ----------------------------
# Helpers
# ----------------------------

def _latest_user_message(messages: List[BaseMessage]) -> Optional[HumanMessage]:
    for m in reversed(messages):
        if isinstance(m, HumanMessage):
            return m
    return None


def classify_issue_from_text(text: str) -> str:
    """
    Heuristic classifier:
      - refund/cancel -> 'refund'
      - hotel/lodging/stay -> 'hotel'
      - car/rental -> 'car'
      - excursion/tour/activity -> 'excursions'
      - flight/book/reservation/ticket -> 'flight'
      - else 'general'
    """
    t = text.lower()
    if any(w in t for w in ["refund", "cancel", "cancellation"]):
        return "refund"
    if any(w in t for w in ["hotel", "lodging", "stay"]):
        return "hotel"
    if any(w in t for w in ["car", "rental", "rent a car"]):
        return "car"
    if any(w in t for w in ["excursion", "tour", "activity", "activities"]):
        return "excursions"
    if any(w in t for w in ["flight", "book", "booking", "reservation", "ticket"]):
        return "flight"
    return "general"


# ----------------------------
# Nodes
# ----------------------------

def classify_issue(state: SupportState) -> SupportState:
    """Classify user's request into a coarse issue_type."""
    messages = state["messages"]
    last_user = _latest_user_message(messages)
    issue_type = ""
    if last_user:
        issue_type = classify_issue_from_text(last_user.content)

    return {
        "messages": messages,
        "issue_type": issue_type,
        "itinerary": state.get("itinerary", {}),
        "lodging": state.get("lodging", {}),
        "car_rental": state.get("car_rental", {}),
        "excursions": state.get("excursions", {}),
    }


def handle_flight(state: SupportState) -> SupportState:
    """Use search_flights tool for flight-related questions."""
    messages = state["messages"]
    last_user = _latest_user_message(messages)
    text = last_user.content if last_user else ""

    # Very simple pattern: "from XXX to YYY on DATE"
    origin = "SFO"
    destination = "JFK"
    date_str = "2025-12-01"
    route_match = re.search(r"from ([A-Za-z]{3}) to ([A-Za-z]{3})", text)
    if route_match:
        origin = route_match.group(1).upper()
        destination = route_match.group(2).upper()

    date_match = re.search(r"on (\d{4}-\d{2}-\d2)", text)
    if date_match:
        date_str = date_match.group(1)

    result = search_flights.invoke(
        {"origin": origin, "destination": destination, "date": date_str}
    )

    tool_msg = ToolMessage(
        content=result,
        name="search_flights",
        tool_call_id="search_flights-1",
    )

    messages = messages + [tool_msg]
    itinerary = {
        "origin": origin,
        "destination": destination,
        "date": date_str,
        "options": result,
    }

    return {
        "messages": messages,
        "issue_type": state["issue_type"],
        "itinerary": itinerary,
        "lodging": state.get("lodging", {}),
        "car_rental": state.get("car_rental", {}),
        "excursions": state.get("excursions", {}),
    }


def handle_refund(state: SupportState) -> SupportState:
    """Use lookup_policy for refund/cancellation questions."""
    messages = state["messages"]
    last_user = _latest_user_message(messages)
    text = last_user.content if last_user else ""
    policy = lookup_policy.invoke({"topic": text})

    tool_msg = ToolMessage(
        content=policy,
        name="lookup_policy",
        tool_call_id="lookup_policy-1",
    )
    messages = messages + [tool_msg]

    return {
        "messages": messages,
        "issue_type": state["issue_type"],
        "itinerary": state.get("itinerary", {}),
        "lodging": state.get("lodging", {}),
        "car_rental": state.get("car_rental", {}),
        "excursions": state.get("excursions", {}),
    }


def handle_hotel(state: SupportState) -> SupportState:
    """Use search_hotels for lodging questions."""
    messages = state["messages"]
    last_user = _latest_user_message(messages)
    text = last_user.content if last_user else ""

    # crude: default city & nights, or parse "in CITY for N nights"
    location = "New York"
    nights = 3
    loc_match = re.search(r"in ([A-Za-z ]+)", text)
    if loc_match:
        location = loc_match.group(1).strip().title()

    nights_match = re.search(r"(\d+)\s+nights?", text)
    if nights_match:
        nights = int(nights_match.group(1))

    result = search_hotels.invoke({"location": location, "nights": nights})

    tool_msg = ToolMessage(
        content=result,
        name="search_hotels",
        tool_call_id="search_hotels-1",
    )
    messages = messages + [tool_msg]

    lodging = {
        "location": location,
        "nights": nights,
        "options": result,
    }

    return {
        "messages": messages,
        "issue_type": state["issue_type"],
        "itinerary": state.get("itinerary", {}),
        "lodging": lodging,
        "car_rental": state.get("car_rental", {}),
        "excursions": state.get("excursions", {}),
    }


def handle_car(state: SupportState) -> SupportState:
    """Use search_cars for rental questions."""
    messages = state["messages"]
    last_user = _latest_user_message(messages)
    text = last_user.content if last_user else ""

    location = "New York"
    days = 3
    loc_match = re.search(r"in ([A-Za-z ]+)", text)
    if loc_match:
        location = loc_match.group(1).strip().title()

    days_match = re.search(r"(\d+)\s+days?", text)
    if days_match:
        days = int(days_match.group(1))

    result = search_cars.invoke({"location": location, "days": days})

    tool_msg = ToolMessage(
        content=result,
        name="search_cars",
        tool_call_id="search_cars-1",
    )
    messages = messages + [tool_msg]

    car_rental = {
        "location": location,
        "days": days,
        "options": result,
    }

    return {
        "messages": messages,
        "issue_type": state["issue_type"],
        "itinerary": state.get("itinerary", {}),
        "lodging": state.get("lodging", {}),
        "car_rental": car_rental,
        "excursions": state.get("excursions", {}),
    }


def handle_excursions(state: SupportState) -> SupportState:
    """Use search_excursions (trip_recommendations) for excursion/activity questions."""
    messages = state["messages"]
    last_user = _latest_user_message(messages)
    text = last_user.content if last_user else ""

    location = "New York"
    interests = "museums"
    loc_match = re.search(r"in ([A-Za-z ]+)", text)
    if loc_match:
        location = loc_match.group(1).strip().title()

    # Very simple interest extraction
    if "wine" in text.lower():
        interests = "wine"
    elif "museum" in text.lower():
        interests = "museum"
    elif "outdoor" in text.lower():
        interests = "outdoor"

    result = search_excursions.invoke({"location": location, "interests": interests})

    tool_msg = ToolMessage(
        content=result,
        name="search_excursions",
        tool_call_id="search_excursions-1",
    )
    messages = messages + [tool_msg]

    excursions = {
        "location": location,
        "interests": interests,
        "options": result,
    }

    return {
        "messages": messages,
        "issue_type": state["issue_type"],
        "itinerary": state.get("itinerary", {}),
        "lodging": state.get("lodging", {}),
        "car_rental": state.get("car_rental", {}),
        "excursions": excursions,
    }


def final_answer(state: SupportState) -> SupportState:
    """
    Use Ollama to generate the final reply.

    Convert ToolMessages -> HumanMessages so ChatOllama can handle them.
    """
    llm = build_llm()
    messages = state["messages"]

    converted: List[BaseMessage] = []
    for m in messages:
        if isinstance(m, ToolMessage):
            converted.append(
                HumanMessage(
                    content=f"Tool `{m.name}` returned: {m.content}"
                )
            )
        else:
            converted.append(m)

    system = SystemMessage(
        content=(
            "You are a helpful airline customer support assistant. "
            "You can help with flights, refunds, hotels, car rentals, and excursions. "
            "Use any tool information (search_flights, lookup_policy, search_hotels, "
            "search_cars, search_excursions, book_car_rental, book_hotel, "
            "book_excursion, cancel_ticket, update_ticket_to_new_flight, "
            "fetch_user_flight_information, search_trip_recommendations) "
            "to give a clear, concise answer."
        )
    )

    response: AIMessage = llm.invoke([system] + converted)

    return {
        "messages": messages + [response],
        "issue_type": state["issue_type"],
        "itinerary": state.get("itinerary", {}),
        "lodging": state.get("lodging", {}),
        "car_rental": state.get("car_rental", {}),
        "excursions": state.get("excursions", {}),
    }


# ----------------------------
# Graph builder
# ----------------------------

def _build_support_graph(checkpointer):
    g = StateGraph(SupportState)

    g.add_node("classify_issue", RunnableLambda(classify_issue))
    g.add_node("handle_flight", RunnableLambda(handle_flight))
    g.add_node("handle_refund", RunnableLambda(handle_refund))
    g.add_node("handle_hotel", RunnableLambda(handle_hotel))
    g.add_node("handle_car", RunnableLambda(handle_car))
    g.add_node("handle_excursions", RunnableLambda(handle_excursions))
    g.add_node("final_answer", RunnableLambda(final_answer))

    g.set_entry_point("classify_issue")

    def route_from_classify(state: SupportState) -> str:
        issue_type = (state.get("issue_type") or "").lower()
        if issue_type == "flight":
            return "handle_flight"
        if issue_type == "refund":
            return "handle_refund"
        if issue_type == "hotel":
            return "handle_hotel"
        if issue_type == "car":
            return "handle_car"
        if issue_type == "excursions":
            return "handle_excursions"
        # fallback
        return "final_answer"

    g.add_conditional_edges("classify_issue", route_from_classify)
    g.add_edge("handle_flight", "final_answer")
    g.add_edge("handle_refund", "final_answer")
    g.add_edge("handle_hotel", "final_answer")
    g.add_edge("handle_car", "final_answer")
    g.add_edge("handle_excursions", "final_answer")
    g.add_edge("final_answer", END)

    return g.compile(checkpointer=checkpointer)


# ----------------------------
# Pytest: stress-test with AerospikeSaver
# ----------------------------

def test_customer_support_graph_runs_and_persists_checkpoint(saver, cfg_base):
    # Build graph app with your AerospikeSaver
    app = _build_support_graph(saver)

    cfg = deepcopy(cfg_base)
    cfg["configurable"]["checkpoint_ns"] = "customer-support"

    tutorial_questions = [
        "Hi there, what time is my flight?",
        "Am i allowed to update my flight to something sooner? I want to leave later today.",
        "Update my flight to sometime next week then",
        "The next available option is great",
        "what about lodging and transportation?",
        "Yeah i think i'd like an affordable hotel for my week-long stay (7 days). And I'll want to rent a car.",
        "OK could you place a reservation for your recommended hotel? It sounds nice.",
        "yes go ahead and book anything that's moderate expense and has availability.",
        "Now for a car, what are my options?",
        "Awesome let's just get the cheapest option. Go ahead and book for 7 days",
        "Cool so now what recommendations do you have on excursions?",
        "Are they available while I'm there?",
        "interesting - i like the museums, what options are there? ",
        "OK great pick one and book it for my second day there.",
    ]

    state: SupportState = {
        "messages": [],
        "issue_type": "",
        "itinerary": {},
        "lodging": {},
        "car_rental": {},
        "excursions": {},
    }

    for turn in tutorial_questions:
        state["messages"].append(HumanMessage(content=turn))

        # If Ollama isn't running, skip instead of failing the whole suite
        try:
            state = app.invoke(state, cfg)
        except Exception as e:
            msg = str(e).lower()
            if "connection refused" in msg or "failed to establish" in msg or "connection error" in msg:
                pytest.skip(f"Ollama not running or unreachable: {e}")
            raise

    out = state

    # Basic sanity checks
    assert "messages" in out
    assert isinstance(out["messages"], list)
    assert len(out["messages"]) >= 2  # user + final AI, plus tool messages

    # Make sure we got at least one AI answer mentioning refund or hotel or car
    ai_text = " ".join(
        m.content for m in out["messages"] if isinstance(m, AIMessage)
    ).lower()
    assert any(word in ai_text for word in ["refund", "hotel", "car", "flight"])

    # Check that a checkpoint exists
    latest = saver.get_tuple(cfg)
    assert latest is not None
    assert isinstance(latest.checkpoint, dict)

    # And that we have some timeline entries for this thread/ns
    timeline = list(saver.list(cfg, limit=10))
    assert len(timeline) >= 10
