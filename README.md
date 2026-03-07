# KKD Network Performance Dashboard

A Streamlit-based telecom network analytics dashboard for KKD/KKD SSA performance monitoring.

## Features

- 📊 Monthly Overview with KPI cards and heatmaps
- 📈 Month-on-Month shift analysis
- 📉 Historical trend charts
- 🗺️ SDCA drill-down reports
- 🔗 Correlation analysis
- 🚀 Top/Bottom 25 sites
- 🔄 Technology shift tracking
- 👷 Incharge-wise analysis
- 💰 Revenue Report (2G/3G/4G tech-wise, band-wise, SDCA heatmap)
- 📅 Revenue Per Day analysis
- 🟡 Good Availability + Low Revenue actionable site reports
- 🌟 Consistent Good Availability multi-month analysis
- 🏆 Executive Report

## How to Use

1. Open the app using the link below
2. In the **sidebar**, upload your files in order:
   - ① Reference file: `BTSIPID_PKEY1_excel.xlsx`
   - ② Performance files: monthly CSV or XLSX files (Jan, Dec, etc.)
   - ③ Revenue files: `RBC_*.xlsx` files
3. All tabs will populate automatically

## Running Locally

```bash
pip install -r requirements.txt
streamlit run network_dashboard.py
```

## Deployment

Hosted on [Streamlit Community Cloud](https://streamlit.io/cloud).
