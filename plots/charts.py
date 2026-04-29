import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd

# Tyre compound colours — matches F1 official colours
COMPOUND_COLOURS = {
    "SOFT": "#FF3333",
    "MEDIUM": "#FFD700",
    "HARD": "#EEEEEE",
    "INTERMEDIATE": "#39B54A",
    "WET": "#0067FF",
    "UNKNOWN": "#888888"
}


def ms_to_seconds(ms):
    """Convert milliseconds to seconds float."""
    return ms / 1000 if ms is not None else None


# ─────────────────────────────────────────────
# 1. LAP TIME COMPARISON
# ─────────────────────────────────────────────

def plot_lap_times(lap_data: dict) -> go.Figure:
    """
    Line chart of lap times across all laps for a single driver.
    Input: result from get_lap_times()
    """
    laps = lap_data.get("laps", [])
    if not laps:
        return None

    df = pd.DataFrame(laps)
    df["lap_time_s"] = df["lap_time_ms"].apply(ms_to_seconds) if "lap_time_ms" in df.columns else None

    fig = go.Figure()

    # Colour points by compound
    for compound, group in df.groupby("compound"):
        colour = COMPOUND_COLOURS.get(compound, "#888888")
        fig.add_trace(go.Scatter(
            x=group["lap_number"],
            y=group["lap_time_s"],
            mode="lines+markers",
            name=compound,
            line=dict(color=colour),
            marker=dict(color=colour, size=6)
        ))

    fig.update_layout(
        title=f"{lap_data.get('driver')} — Lap Times · {lap_data.get('race')}",
        xaxis_title="Lap",
        yaxis_title="Lap Time (s)",
        template="plotly_dark",
        legend_title="Compound",
        hovermode="x unified"
    )

    return fig


# ─────────────────────────────────────────────
# 2. DRIVER HEAD-TO-HEAD LAP TIMES
# ─────────────────────────────────────────────

def plot_head_to_head(compare_data: dict, driver1: str, driver2: str) -> go.Figure:
    """
    Overlaid lap time lines for two drivers.
    Input: result from compare_drivers()
    """
    laps = compare_data.get("lap_by_lap", [])
    if not laps:
        return None

    df = pd.DataFrame(laps)
    df["lap_time_s"] = df["lap_time_ms"].apply(ms_to_seconds)

    fig = go.Figure()

    colours = ["#FF6B6B", "#4ECDC4"]
    for i, driver in enumerate([driver1.upper(), driver2.upper()]):
        d = df[df["driver_code"].str.upper() == driver]
        fig.add_trace(go.Scatter(
            x=d["lap_number"],
            y=d["lap_time_s"],
            mode="lines+markers",
            name=driver,
            line=dict(color=colours[i]),
            marker=dict(size=5)
        ))

    fig.update_layout(
        title=f"{driver1.upper()} vs {driver2.upper()} — Lap Times · {compare_data.get('race')}",
        xaxis_title="Lap",
        yaxis_title="Lap Time (s)",
        template="plotly_dark",
        hovermode="x unified"
    )

    return fig


# ─────────────────────────────────────────────
# 3. TYRE STRATEGY
# ─────────────────────────────────────────────

def plot_tyre_strategy(strategy_data: dict) -> go.Figure:
    """
    Horizontal bar chart showing each driver's tyre stints.
    Input: result from get_tyre_strategy()
    """
    strategy = strategy_data.get("strategy", {})
    if not strategy:
        return None

    drivers = list(strategy.keys())
    fig = go.Figure()

    for driver in drivers:
        stints = strategy[driver]
        for stint in stints:
            compound = stint.get("compound", "UNKNOWN")
            colour = COMPOUND_COLOURS.get(compound, "#888888")
            fig.add_trace(go.Bar(
                name=compound,
                x=[stint["laps"]],
                y=[driver],
                orientation="h",
                marker_color=colour,
                text=f"{compound} ({stint['laps']} laps)",
                textposition="inside",
                hovertemplate=(
                    f"<b>{driver}</b><br>"
                    f"Compound: {compound}<br>"
                    f"Laps: {stint['stint_start']}–{stint['stint_end']}<br>"
                    f"Duration: {stint['laps']} laps<extra></extra>"
                ),
                showlegend=compound not in [t.name for t in fig.data]
            ))

    fig.update_layout(
        title=f"Tyre Strategy · {strategy_data.get('race')}",
        xaxis_title="Laps",
        yaxis_title="Driver",
        barmode="stack",
        template="plotly_dark",
        legend_title="Compound",
        height=max(400, len(drivers) * 35)
    )

    return fig


# ─────────────────────────────────────────────
# 4. RACE RESULTS BAR CHART
# ─────────────────────────────────────────────

def plot_race_results(results_data: dict) -> go.Figure:
    """
    Horizontal bar chart of points scored per driver.
    Input: result from get_race_results()
    """
    results = results_data.get("results", [])
    if not results:
        return None

    df = pd.DataFrame(results)
    df = df[df["points"] > 0].sort_values("points", ascending=True)

    fig = go.Figure(go.Bar(
        x=df["points"],
        y=df["driver_code"],
        orientation="h",
        marker_color="#FF6B6B",
        text=df["points"],
        textposition="outside"
    ))

    fig.update_layout(
        title=f"Points Scored · {results_data.get('race')}",
        xaxis_title="Points",
        yaxis_title="Driver",
        template="plotly_dark",
        height=400
    )

    return fig


# ─────────────────────────────────────────────
# 5. DRIVER STANDINGS
# ─────────────────────────────────────────────

def plot_driver_standings(standings_data: dict) -> go.Figure:
    """
    Horizontal bar chart of championship standings.
    Input: result from get_driver_standings()
    """
    standings = standings_data.get("standings", [])
    if not standings:
        return None

    df = pd.DataFrame(standings)
    df = df.sort_values("total_points", ascending=True)

    fig = go.Figure(go.Bar(
        x=df["total_points"],
        y=df["driver_code"],
        orientation="h",
        marker_color="#4ECDC4",
        text=df["total_points"],
        textposition="outside"
    ))

    fig.update_layout(
        title=f"{standings_data.get('year')} Driver Championship Standings",
        xaxis_title="Points",
        yaxis_title="Driver",
        template="plotly_dark",
        height=500
    )

    return fig


# ─────────────────────────────────────────────
# 6. FASTEST LAPS COMPARISON
# ─────────────────────────────────────────────

def plot_fastest_laps(fastest_data: dict) -> go.Figure:
    """
    Bar chart of fastest lap per driver in a race.
    Input: result from get_fastest_laps()
    """
    laps = fastest_data.get("fastest_laps", [])
    if not laps:
        return None

    df = pd.DataFrame(laps)

    # Parse lap time string back to seconds for plotting
    def laptime_to_seconds(lt):
        try:
            m, s = lt.split(":")
            return int(m) * 60 + float(s)
        except Exception:
            return None

    df["lap_time_s"] = df["fastest_lap"].apply(laptime_to_seconds)
    df = df.sort_values("lap_time_s", ascending=False)

    fig = go.Figure(go.Bar(
        x=df["lap_time_s"],
        y=df["driver_code"],
        orientation="h",
        marker_color="#FFD700",
        text=df["fastest_lap"],
        textposition="outside"
    ))

    fig.update_layout(
        title=f"Fastest Laps · {fastest_data.get('race')}",
        xaxis_title="Lap Time (s)",
        yaxis_title="Driver",
        template="plotly_dark",
        height=500
    )

    return fig

# ─────────────────────────────────────────────
# 7. QUALIFYING RESULTS
# ─────────────────────────────────────────────

def plot_qualifying_results(qualifying_data: dict) -> go.Figure:
    """
    Horizontal bar chart of qualifying times per driver.
    Input: result from get_qualifying_results()
    """
    qualifying = qualifying_data.get("qualifying", [])
    if not qualifying:
        return None

    df = pd.DataFrame(qualifying)

    def laptime_to_seconds(lt):
        try:
            m, s = lt.split(":")
            return int(m) * 60 + float(s)
        except Exception:
            return None

    df["best_time_s"] = df["best_time"].apply(laptime_to_seconds)
    df = df.dropna(subset=["best_time_s"])
    df = df.sort_values("best_time_s", ascending=False)

    fig = go.Figure(go.Bar(
        x=df["best_time_s"],
        y=df["driver_code"],
        orientation="h",
        marker_color="#FF6B6B",
        text=df["best_time"],
        textposition="outside"
    ))

    fig.update_layout(
        title=f"Qualifying Times · {qualifying_data.get('race')}",
        xaxis_title="Best Time (s)",
        yaxis_title="Driver",
        template="plotly_dark",
        height=500
    )

    return fig

# ─────────────────────────────────────────────
# 8. SEASON POINTS PROGRESSION
# ─────────────────────────────────────────────

def plot_season_points_progression(progression_data: dict) -> go.Figure:
    """
    Line chart of cumulative championship points per driver across a season.
    Input: result from get_season_points_progression()
    """
    progression = progression_data.get("progression", {})
    rounds = progression_data.get("rounds", [])
    if not progression or not rounds:
        return None

    round_nums = [str(r["round"]) for r in rounds]
    round_labels = [r["gp_name"].replace(" Grand Prix", "") for r in rounds]

    fig = go.Figure()

    colours = px.colors.qualitative.Set3

    for i, (driver, data) in enumerate(progression.items()):
        points = [data["points_per_round"].get(rn, None) for rn in round_nums]
        fig.add_trace(go.Scatter(
            x=round_labels,
            y=points,
            mode="lines+markers",
            name=driver,
            line=dict(color=colours[i % len(colours)], width=2),
            marker=dict(size=5),
            hovertemplate=f"<b>{driver}</b><br>%{{x}}<br>Points: %{{y}}<extra></extra>"
        ))

    fig.update_layout(
        title=f"{progression_data.get('year')} Championship Points Progression",
        xaxis_title="Race",
        yaxis_title="Cumulative Points",
        template="plotly_dark",
        legend_title="Driver",
        hovermode="x unified",
        xaxis=dict(tickangle=-45),
        height=500
    )

    return fig


# ─────────────────────────────────────────────
# 9. TYRE DEGRADATION CURVES
# ─────────────────────────────────────────────

def plot_tyre_degradation(degradation_data: dict) -> go.Figure:
    """
    Line chart of average lap time vs tyre age per compound.
    Input: result from get_tyre_degradation()
    """
    degradation = degradation_data.get("degradation", {})
    if not degradation:
        return None

    fig = go.Figure()

    for compound, data in degradation.items():
        if not data:
            continue
        colour = COMPOUND_COLOURS.get(compound, "#888888")
        df = pd.DataFrame(data)
        fig.add_trace(go.Scatter(
            x=df["tyre_life"],
            y=df["lap_time_s"],
            mode="lines+markers",
            name=compound,
            line=dict(color=colour, width=2),
            marker=dict(size=5, color=colour),
            hovertemplate=(
                f"<b>{compound}</b><br>"
                "Tyre Age: %{x} laps<br>"
                "Avg Lap: %{y:.3f}s<extra></extra>"
            )
        ))

    fig.update_layout(
        title=f"Tyre Degradation · {degradation_data.get('race')}",
        xaxis_title="Tyre Age (laps)",
        yaxis_title="Avg Lap Time (s)",
        template="plotly_dark",
        legend_title="Compound",
        hovermode="x unified",
        height=450
    )

    return fig


# ─────────────────────────────────────────────
# 10. RACE POSITION CHANGES
# ─────────────────────────────────────────────

def plot_race_position_changes(position_data: dict) -> go.Figure:
    """
    Bumpchart showing lap by lap position for all drivers.
    Input: result from get_race_position_changes()
    """
    drivers = position_data.get("drivers", {})
    if not drivers:
        return None

    fig = go.Figure()
    colours = px.colors.qualitative.Light24

    for i, (driver, laps) in enumerate(drivers.items()):
        if not laps:
            continue
        df = pd.DataFrame(laps)
        fig.add_trace(go.Scatter(
            x=df["lap_number"],
            y=df["position"],
            mode="lines",
            name=driver,
            line=dict(color=colours[i % len(colours)], width=1.5),
            hovertemplate=(
                f"<b>{driver}</b><br>"
                "Lap: %{x}<br>"
                "Position: %{y}<extra></extra>"
            )
        ))

    total_laps = position_data.get("total_laps", 60)

    fig.update_layout(
        title=f"Race Position Changes · {position_data.get('race')}",
        xaxis_title="Lap",
        yaxis_title="Position",
        yaxis=dict(
            autorange="reversed",      # P1 at top
            tickmode="linear",
            tick0=1,
            dtick=1
        ),
        template="plotly_dark",
        legend_title="Driver",
        hovermode="x unified",
        height=600,
        xaxis=dict(range=[1, total_laps])
    )

    return fig