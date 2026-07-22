import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
from agent.agent import ask
from agent.tools import (
    get_race_results,
    get_driver_standings,
    get_lap_times,
    get_fastest_laps,
    get_tyre_strategy,
    compare_drivers,
    get_qualifying_results,
    get_season_points_progression,
    get_tyre_degradation,
    get_race_position_changes,
    get_fastest_laps_for_drivers,
)
from plots.charts import (
    plot_lap_times,
    plot_head_to_head,
    plot_tyre_strategy,
    plot_race_results,
    plot_driver_standings,
    plot_fastest_laps,
    plot_qualifying_results,
    plot_season_points_progression,
    plot_tyre_degradation,
    plot_race_position_changes,
    plot_fastest_laps_for_drivers,
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
st.caption("Ask anything about the 2023, 2024 and 2025 Formula 1 seasons.")

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
    "são paulo": "brazilian",
    "brazil": "são paulo",
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
    "miami": "miami",
    "austria": "austrian",
    "china": "chinese",
    "qatar": "qatar",
    "belgium": "belgian",
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
    "ZHO", "MAG", "HUL", "ALB", "SAR", "LAW", "BEA",
    "PER", "MSC", "DEV", "FIT", "ZHO"
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

# Keywords that mean the user is explicitly asking for a plot
EXPLICIT_PLOT_WORDS = [
    "plot", "graph", "chart", "visuali", "show me", "draw",
    "display", "diagram", "visual"
]

# Short follow-up phrases that need prior context to make sense
FOLLOWUP_PHRASES = [
    "yes", "sure", "go ahead", "do it", "please", "ok", "okay",
    "yes plot", "yes draw", "yes show", "plot it", "draw it",
    "show it", "create it", "make it", "can you plot", "can you show",
    "can you draw", "can you create", "can you visualize"
]

def user_wants_plot(question: str) -> bool:
    """Return True only if the user explicitly asked for a visualization."""
    return any(w in question.lower() for w in EXPLICIT_PLOT_WORDS)


def is_followup_request(question: str) -> bool:
    """
    Return True if the message is a short vague follow-up that needs
    prior context to make sense e.g. 'yes plot it', 'go ahead'.
    If the user gives a specific request with GP names or driver codes,
    it is NOT a follow-up — use the question as-is.
    """
    q = question.lower().strip()

    # If the question contains a GP name or driver code it is self-contained
    has_gp = any(gp in q for gp in GP_NAMES)
    has_driver = any(d.lower() in q for d in DRIVER_CODES)
    has_year = any(y in q for y in ["2023", "2024", "2025"])

    if has_gp or has_driver or has_year:
        return False

    # Short message that matches a follow-up phrase
    return any(phrase in q for phrase in FOLLOWUP_PHRASES) or len(q.split()) <= 6

def extract_year(text: str) -> int:
    if "2025" in text:
        return 2025
    elif "2023" in text:
        return 2023
    return 2024

def detect_chart_type(question: str) -> str:
    """Detect if the user requested a specific chart type."""
    q = question.lower()
    if any(w in q for w in ["line chart", "line graph", "line plot"]):
        return "line"
    if any(w in q for w in ["bar chart", "bar graph", "bar plot"]):
        return "bar"
    return "auto"  # let the code decide

def try_generate_plot(question: str, answer: str):
    """
    Try to generate a relevant plot based on the question and answer context.
    Only called when the user explicitly asks for a visualization.
    Returns a plotly figure or None.
    """
    q = resolve_query_gp(question.lower())
    a = resolve_query_gp(answer.lower())
    combined = f"{q} {a}"

    year = extract_year(combined)
    chart_type = detect_chart_type(question)

    try:
        # ── Tyre strategy ────────────────────────────────────────
        if any(w in combined for w in ["tyre", "tire", "strategy",
                                        "compound", "stint"]):
            for gp in GP_NAMES:
                if gp in combined:
                    data = get_tyre_strategy(year, gp)
                    return plot_tyre_strategy(data)

        # ── Qualifying ───────────────────────────────────────────
        if any(w in combined for w in ["qualifying", "quali", "pole",
                                        "grid", "q1", "q2", "q3"]):
            for gp in GP_NAMES:
                if gp in combined:
                    data = get_qualifying_results(year, gp)
                    return plot_qualifying_results(data)

        # ── Multi or head-to-head driver comparison ───────────────
        if any(w in combined for w in ["compare", "vs", "versus",
                                        "head-to-head", "faster",
                                        "comparison", "fastest lap",
                                        "lap time", "sector"]):
            found = [d for d in DRIVER_CODES if d in combined.upper()]

            if len(found) >= 3 and chart_type != "line":
                # 3+ drivers — sector breakdown bar chart
                # unless user explicitly asked for a line chart
                for gp in GP_NAMES:
                    if gp in combined:
                        data = get_fastest_laps_for_drivers(year, gp, found[:5])
                        return plot_fastest_laps_for_drivers(data)

            elif len(found) >= 2:
                # 2 drivers or user asked for line chart
                for gp in GP_NAMES:
                    if gp in combined:
                        if chart_type == "line":
                            data = compare_drivers(year, gp, found[0], found[1])
                            return plot_head_to_head(data, found[0], found[1])
                        else:
                            # Default to sector bar for 2 drivers when chart
                            # type is bar or auto and context is about fastest lap
                            if any(w in combined for w in ["fastest lap", "sector",
                                                            "fastest"]):
                                data = get_fastest_laps_for_drivers(
                                    year, gp, found[:2])
                                return plot_fastest_laps_for_drivers(data)
                            else:
                                # Default head-to-head is lap time line chart
                                data = compare_drivers(year, gp, found[0], found[1])
                                return plot_head_to_head(data, found[0], found[1])

        # ── Single driver lap times ───────────────────────────────
        if any(w in combined for w in ["lap time", "lap times", "pace", "laps"]):
            found = [d for d in DRIVER_CODES if d in combined.upper()]
            if found:
                for gp in GP_NAMES:
                    if gp in combined:
                        data = get_lap_times(year, gp, found[0])
                        return plot_lap_times(data)

        # ── Fastest laps across the field ─────────────────────────
        if any(w in combined for w in ["fastest lap", "fastest laps", "quickest"]):
            found = [d for d in DRIVER_CODES if d in combined.upper()]
            if len(found) >= 2:
                for gp in GP_NAMES:
                    if gp in combined:
                        data = get_fastest_laps_for_drivers(year, gp, found[:5])
                        return plot_fastest_laps_for_drivers(data)
            for gp in GP_NAMES:
                if gp in combined:
                    data = get_fastest_laps(year, gp)
                    return plot_fastest_laps(data)

        # ── Race results ──────────────────────────────────────────
        if any(w in combined for w in ["result", "results", "won", "winner",
                                        "podium", "finishing", "classified",
                                        "points"]):
            for gp in GP_NAMES:
                if gp in combined:
                    data = get_race_results(year, gp)
                    return plot_race_results(data)

        # ── Driver standings ──────────────────────────────────────
        if any(w in combined for w in ["standing", "standings", "championship",
                                        "champion"]):
            data = get_driver_standings(year)
            return plot_driver_standings(data)

        # ── Season points progression ─────────────────────────────
        if any(w in combined for w in ["progression", "championship battle",
                                        "points race", "how the championship",
                                        "season points"]):
            data = get_season_points_progression(year)
            return plot_season_points_progression(data)

        # ── Tyre degradation ──────────────────────────────────────
        if any(w in combined for w in ["degradation", "deg", "tyre life",
                                        "tyre performance"]):
            for gp in GP_NAMES:
                if gp in combined:
                    data = get_tyre_degradation(year, gp)
                    return plot_tyre_degradation(data)

        # ── Race position changes ─────────────────────────────────
        if any(w in combined for w in ["position changes", "positions",
                                        "bumpchart", "how the race unfolded",
                                        "lap by lap"]):
            for gp in GP_NAMES:
                if gp in combined:
                    data = get_race_position_changes(year, gp)
                    return plot_race_position_changes(data)

    except Exception:
        pass

    return None


# ─────────────────────────────────────────────
# RENDER CHAT HISTORY
# ─────────────────────────────────────────────

for i, msg in enumerate(st.session_state.messages):
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if "figure" in msg and msg["figure"] is not None:
            st.plotly_chart(msg["figure"], use_container_width=True, key=f"fig_history_{i}")

# ─────────────────────────────────────────────
# CHAT INPUT
# ─────────────────────────────────────────────

if prompt := st.chat_input("Ask about F1 — results, standings, lap times, strategy..."):

    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Get last assistant message for follow-up context
    last_assistant = ""
    for msg in reversed(st.session_state.messages):
        if msg["role"] == "assistant":
            last_assistant = msg["content"]
            break

    with st.chat_message("assistant"):
        with st.spinner("Analysing..."):
            answer, st.session_state.history = ask(prompt, st.session_state.history)

            fig = None
            if user_wants_plot(prompt):
                if is_followup_request(prompt):
                    # Vague follow-up — enrich with prior context
                    plot_context = f"{prompt} {last_assistant}"
                else:
                    # Specific request — use only what the user said
                    # plus the agent answer, ignore prior context
                    plot_context = f"{prompt} {answer}"

                fig = try_generate_plot(plot_context, answer)

                # Fallback — try answer only
                if fig is None:
                    fig = try_generate_plot(answer, answer)

        st.markdown(answer)
        if fig:
            st.plotly_chart(
                fig,
                use_container_width=True,
                key=f"fig_new_{len(st.session_state.messages)}"
            )

    st.session_state.messages.append({
        "role": "assistant",
        "content": answer,
        "figure": fig
    })