import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
from agent.agent import ask
from agent.tools import (
    get_race_results,
    get_qualifying_results,
    get_driver_standings,
    get_lap_times,
    get_fastest_laps,
    get_tyre_strategy,
    compare_drivers
)
from plots.charts import (
    plot_lap_times,
    plot_qualifying_results,
    plot_head_to_head,
    plot_tyre_strategy,
    plot_race_results,
    plot_driver_standings,
    plot_fastest_laps
)

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────

st.set_page_config(
    page_title="F1 Agent",
    page_icon="🏎️",
    layout="wide"
)

st.title("🏎️ F1 Agent")
st.caption("Ask anything about the 2024 and 2025 Formula 1 seasons.")

# ─────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────

if "messages" not in st.session_state:
    st.session_state.messages = []

if "history" not in st.session_state:
    st.session_state.history = None

# ─────────────────────────────────────────────
# GP NAME RESOLUTION
# ─────────────────────────────────────────────

GP_ALIASES = {
    "silverstone": "british",
    "spa": "belgian",
    "monza": "italian",
    "suzuka": "japanese",
    "interlagos": "brazilian",
    "sao paulo": "brazilian",
    "cota": "united states",
    "austin": "united states",
    "usa": "united states",
    "vegas": "las vegas",
    "baku": "azerbaijan",
    "jeddah": "saudi",
    "imola": "emilia",
    "zandvoort": "dutch",
    "montreal": "canadian",
    "barcelona": "spanish",
    "budapest": "hungarian",
    "spielberg": "austrian",
    "red bull ring": "austrian",
    "shanghai": "chinese",
    "melbourne": "australian",
    "sakhir": "bahrain",
    "lusail": "qatar",
    "marina bay": "singapore",
}

GP_NAMES = [
    "bahrain", "saudi", "australian", "japanese", "chinese", "miami",
    "emilia", "monaco", "canadian", "spanish", "austrian", "british",
    "hungarian", "belgian", "dutch", "italian", "azerbaijan", "singapore",
    "united states", "mexico", "brazilian", "las vegas", "qatar", "abu dhabi"
]

DRIVER_CODES = [
    "VER", "HAM", "LEC", "SAI", "NOR", "PIA", "RUS",
    "ALO", "STR", "GAS", "OCO", "TSU", "RIC", "BOT",
    "ZHO", "MAG", "HUL", "ALB", "SAR", "LAW", "BEA"
]


def resolve_query_gp(q: str) -> str:
    """Resolve any alias in the user query to the official GP name."""
    for alias, official in GP_ALIASES.items():
        if alias in q:
            q = q.replace(alias, official)
    return q


# ─────────────────────────────────────────────
# PLOT DETECTION
# ─────────────────────────────────────────────

def try_generate_plot(question: str, answer: str):
    """
    Try to generate a relevant plot based on the question keywords.
    Returns a plotly figure or None.
    """
    q = resolve_query_gp(question.lower())
    if "2025" in q:
        year = 2025
    elif "2023" in q:
        year = 2023
    else:
        year = 2024

    try:
        # Tyre strategy
        if any(w in q for w in ["tyre", "tire", "strategy", "compound", "stint"]):
            for gp in GP_NAMES:
                if gp in q:
                    data = get_tyre_strategy(year, gp)
                    return plot_tyre_strategy(data)

        # Qualifying results
        if any(w in q for w in ["qualifying", "quali", "pole", "grid", "q1", "q2", "q3"]):
            for gp in GP_NAMES:
                if gp in q:
                    data = get_qualifying_results(year, gp)
                    return plot_qualifying_results(data)
        
        # Head to head
        if any(w in q for w in ["compare", "vs", "versus", "head-to-head", "faster"]):
            found = [d for d in DRIVER_CODES if d in q.upper()]
            if len(found) >= 2:
                for gp in GP_NAMES:
                    if gp in q:
                        data = compare_drivers(year, gp, found[0], found[1])
                        return plot_head_to_head(data, found[0], found[1])

        # Lap times for a single driver
        if any(w in q for w in ["lap time", "lap times", "pace", "laps"]):
            found = [d for d in DRIVER_CODES if d in q.upper()]
            if found:
                for gp in GP_NAMES:
                    if gp in q:
                        data = get_lap_times(year, gp, found[0])
                        return plot_lap_times(data)

        # Fastest laps
        if any(w in q for w in ["fastest lap", "fastest laps", "quickest"]):
            for gp in GP_NAMES:
                if gp in q:
                    data = get_fastest_laps(year, gp)
                    return plot_fastest_laps(data)

        # Race results
        if any(w in q for w in ["result", "results", "won", "winner", "podium", "points"]):
            for gp in GP_NAMES:
                if gp in q:
                    data = get_race_results(year, gp)
                    return plot_race_results(data)

        # Standings
        if any(w in q for w in ["standing", "standings", "championship", "champion"]):
            data = get_driver_standings(year)
            return plot_driver_standings(data)

    except Exception:
        pass

    return None


# ─────────────────────────────────────────────
# RENDER CHAT HISTORY
# ─────────────────────────────────────────────

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if "figure" in msg and msg["figure"] is not None:
            st.plotly_chart(msg["figure"], use_container_width=True)

# ─────────────────────────────────────────────
# CHAT INPUT
# ─────────────────────────────────────────────

if prompt := st.chat_input("Ask about F1 — results, standings, lap times, strategy..."):

    # Show user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Run agent
    with st.chat_message("assistant"):
        with st.spinner("Analysing..."):
            answer, st.session_state.history = ask(prompt, st.session_state.history)
            fig = try_generate_plot(prompt, answer)

        st.markdown(answer)
        if fig:
            st.plotly_chart(fig, use_container_width=True)

    # Store in message history
    st.session_state.messages.append({
        "role": "assistant",
        "content": answer,
        "figure": fig
    })