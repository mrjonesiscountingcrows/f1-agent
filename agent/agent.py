import os
import json
from openai import OpenAI
from dotenv import load_dotenv

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.tools import (
    get_race_results,
    get_qualifying_results,
     get_sprint_results,               
    get_sprint_qualifying_results, 
    get_driver_standings,
    get_constructor_standings,
    get_lap_times,
    get_fastest_laps,
    get_tyre_strategy,
    compare_drivers,
    get_season_calendar,
    get_driver_profile,          
    get_driver_career_stats,      
    get_driver_team_history,
    get_season_points_progression,    
    get_tyre_degradation,            
    get_race_position_changes,       
    TOOL_DEFINITIONS
)

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_PROMPT = """
You are an expert Formula 1 analyst with deep knowledge of racing strategy,
driver performance, and technical regulations.

You have access to a database covering the 2023, 2024 and 2025 F1 seasons containing:
- Race and qualifying results
- Sprint race and sprint qualifying results
- Lap-by-lap timing data
- Tyre strategy and compound information
- Driver and constructor championship standings
- Driver profiles, career stats, and team history

Sprint weekends in 2024: Chinese, Miami, Austrian, United States, São Paulo, Qatar.
Sprint weekends in 2025: Chinese, Miami, Belgian, United States, São Paulo, Qatar.

Guidelines:
- Always use the available tools to fetch data before answering questions
- If unsure of the exact GP name, call get_season_calendar first to find it
- For sprint weekend questions use get_sprint_results or get_sprint_qualifying_results
- If a sprint tool returns an error with available_sprint_weekends, retry using
  one of those exact names
- For driver background questions use get_driver_profile and get_driver_career_stats
- GP names in the database are official e.g. 'British' not 'Silverstone' —
  use get_season_calendar to confirm if unsure
- Note: career stats only cover ingested seasons (2023-2025), not full F1 careers
- When comparing drivers use compare_drivers for head-to-head analysis
- Be specific with lap times — always present them in m:ss.mmm format
- If data is not available clearly tell the user
- Keep answers concise but insightful
- When relevant suggest a follow-up plot the user might find interesting
"""

# Map tool names to actual functions
TOOL_REGISTRY = {
    "get_race_results": get_race_results,
    "get_qualifying_results": get_qualifying_results,
    "get_sprint_results": get_sprint_results,               
    "get_sprint_qualifying_results": get_sprint_qualifying_results,
    "get_driver_standings": get_driver_standings,
    "get_constructor_standings": get_constructor_standings,
    "get_lap_times": get_lap_times,
    "get_fastest_laps": get_fastest_laps,
    "get_tyre_strategy": get_tyre_strategy,
    "compare_drivers": compare_drivers,
    "get_season_calendar": get_season_calendar,
    "get_driver_profile": get_driver_profile,            
    "get_driver_career_stats": get_driver_career_stats,   
    "get_driver_team_history": get_driver_team_history,
    "get_season_points_progression": get_season_points_progression,   
    "get_tyre_degradation": get_tyre_degradation,                     
    "get_race_position_changes": get_race_position_changes,           
}


def run_tool(name: str, args: dict) -> str:
    """Execute a tool and return the result as a JSON string."""
    if name not in TOOL_REGISTRY:
        return json.dumps({"error": f"Unknown tool: {name}"})
    try:
        result = TOOL_REGISTRY[name](**args)
        return json.dumps(result, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


def chat(messages: list) -> tuple[str, list]:
    """
    Run one turn of the agent loop.
    Returns the final text response and the updated messages list.
    """
    while True:
        response = client.chat.completions.create(
            model="gpt-4o",
            tools=TOOL_DEFINITIONS,
            messages=messages,
            tool_choice="auto"
        )

        message = response.choices[0].message

        # Add assistant message to history
        messages.append(message)

        # If no tool calls, we have the final answer
        if not message.tool_calls:
            return message.content, messages

        # Process each tool call
        for tool_call in message.tool_calls:
            name = tool_call.function.name
            args = json.loads(tool_call.function.arguments)

            print(f"  🔧 Calling tool: {name}({args})")

            result = run_tool(name, args)

            # Append tool result to messages
            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "content": result
            })


def ask(question: str, history: list = None) -> tuple[str, list]:
    """
    Public interface for asking the agent a question.
    Pass history to maintain a multi-turn conversation.
    Returns the answer and updated history.
    """
    if history is None:
        history = [{"role": "system", "content": SYSTEM_PROMPT}]

    history.append({"role": "user", "content": question})
    answer, history = chat(history)
    return answer, history