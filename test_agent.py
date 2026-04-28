from agent.agent import ask

print("=" * 60)
print("F1 Agent Test")
print("=" * 60)

# Test 1 — simple race result
print("\n🏁 Q: Who won the 2024 Bahrain Grand Prix?")
answer, history = ask("Who won the 2024 Bahrain Grand Prix?")
print(f"A: {answer}")

# Test 2 — multi-turn: follow up on the same conversation
print("\n🏁 Q: What was his fastest lap?")
answer, history = ask("What was his fastest lap?", history)
print(f"A: {answer}")

# Test 3 — standings
print("\n🏆 Q: Who won the 2024 drivers championship?")
answer, _ = ask("Who won the 2024 drivers championship?")
print(f"A: {answer}")

# Test 4 — tyre strategy
print("\n🔴 Q: What tyre strategy did the top 3 use in the 2024 Monaco GP?")
answer, _ = ask("What tyre strategy did the top 3 use in the 2024 Monaco GP?")
print(f"A: {answer}")

# Test 5 — head to head
print("\n⚔️  Q: Compare Verstappen and Leclerc's pace at the 2024 Monaco GP")
answer, _ = ask("Compare Verstappen and Leclerc's pace at the 2024 Monaco GP")
print(f"A: {answer}")