# F1 Agent

An AI-powered Formula 1 Q&A agent with plotting capabilities.

## Stack
- **Data**: FastF1 + DuckDB
- **Agent**: OpenAI GPT-4o with function calling
- **Plots**: Plotly
- **UI**: Streamlit

## Setup
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # add your OpenAI API key
```

## Run ingestion
```bash
python -m ingestion.ingest
```

## Run the app
```bash
streamlit run ui/app.py
```
