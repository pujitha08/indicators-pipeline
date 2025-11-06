# Indicators / Indices Pipeline

This project implements a mini data-engineering pipeline that:
- Ingests **economic indicators** (e.g., CPI, UNRATE, SP500) from the [FRED API](https://fred.stlouisfed.org/)
- Stores them into a **DuckDB warehouse**
- Performs **data-quality checks** (freshness, nulls, duplicates)
- Publishes a **Streamlit status dashboard**

---

## ⚙️ Setup Instructions

### 1. Clone & Environment
```bash
git clone https://github.com/<your-repo>/indicators-pipeline.git
cd indicators-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
