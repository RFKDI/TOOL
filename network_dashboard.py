"""
KKD Network Performance Dashboard — v4
Changes:
  • Default view = latest month (auto-selected from MONTH/YEAR columns)
  • Reference file (BTSIPID_PKEY1_excel.xlsx) joined on BTS IP ID → BTSIPID
    to enrich: SDCA (authoritative), LOCATION, SITENAME, incharge, JTO INCHARGE
  • New Tab 9 – Incharge Analysis (incharge + JTO INCHARGE drill-down)
  • Unmatched sites retain their original SDCA from the data file
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="KKD Network Dashboard", layout="wide", page_icon="📡")

# ─────────────────────────── HELPERS ──────────────────────────────────────────

MONTH_ORDER = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

def month_sort_key(label):
    """Sort 'dec25', 'jan26' chronologically."""
    try:
        m = label[:3].lower()
        y = int(label[3:])
        return y * 100 + MONTH_ORDER.get(m, 0)
    except Exception:
        return 0

def identify_vendor_2g(site_id):
    if pd.isna(site_id): return "Unknown"
    s = str(site_id).strip()
    if "PLMN" in s or "WBTS" in s: return "Nokia"
    if s.isdigit(): return "Nortel"
    return "Nokia"

def identify_vendor_3g(site_id):
    if pd.isna(site_id): return "Unknown"
    s = str(site_id).strip()
    return "Nokia" if "WBTS" in s else "ZTE"

def make_month_label(month_str, year_val):
    try:
        m = str(month_str).strip()[:3].lower()
        y = str(int(year_val))[-2:]
        return f"{m}{y}"
    except Exception:
        return str(month_str)

def load_perf_file(uploaded_file):
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    xf = pd.ExcelFile(uploaded_file)
    sheet = "Sheet1" if "Sheet1" in xf.sheet_names else xf.sheet_names[0]
    return pd.read_excel(uploaded_file, sheet_name=sheet)

def load_ref_file(uploaded_file):
    xf = pd.ExcelFile(uploaded_file)
    sheet = xf.sheet_names[0]
    df = pd.read_excel(uploaded_file, sheet_name=sheet)
    # Keep only needed columns; rename for consistent use
    keep = [c for c in ["BTSIPID", "SDCA", "SDCANAME", "SITENAME", "LOCATION",
                         "incharge", "JTO INCHARGE"] if c in df.columns]
    df = df[keep].copy()
    df["BTSIPID"] = df["BTSIPID"].astype(str).str.strip()
    if "SDCA" in df.columns:
        df["SDCA"] = df["SDCA"].str.strip().str.title()
    return df

def standardize(df, ref_df=None):
    """Clean, derive vendor/month columns, and join reference data."""
    df = df.copy()

    # Ensure BTS IP ID is string for join
    if "BTS IP ID" in df.columns:
        df["BTS IP ID"] = df["BTS IP ID"].astype(str).str.strip()

    # Numeric columns
    num_cols = [
        "Nw Avail (2G)", "Nw Avail (3G)", "Nw Avail (4G)", "Nw Avail (4G TCS)",
        "Erl (2g)", "Erl (3g)", "Erl (4g)", "Erl (2100)", "Erl (2500)", "Erl (700)", "Erl Total",
        "Data GB (2g)", "Data GB (3g)", "Data GB (4g)", "Data GB (2100)", "Data GB (2500)",
        "Data GB (700)", "Data GB Total",
        "2G cnt", "3G cnt", "4G cnt", "Latitude", "Longitude",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Vendor detection
    df["Vendor_2G_Derived"] = df.get("BTS Site ID (2G)", pd.Series(dtype=str)).apply(identify_vendor_2g)
    df["Vendor_3G_Derived"] = df.get("BTS Site ID (3G)", pd.Series(dtype=str)).apply(identify_vendor_3g)
    df["Vendor_4G_Derived"] = "Tejas"

    # Physical 4G flag
    has_4g = pd.Series(False, index=df.index)
    for band_col in ["BTS Site ID (700)", "BTS Site ID (2100)", "BTS Site ID (2500)"]:
        if band_col in df.columns:
            has_4g |= df[band_col].notna()
    df["Has_4G_Physical"] = has_4g

    # Month label from MONTH + YEAR columns
    if "MONTH" in df.columns and "YEAR" in df.columns:
        df["Month_Label"] = df.apply(lambda r: make_month_label(r["MONTH"], r["YEAR"]), axis=1)
    else:
        df["Month_Label"] = "unknown"

    # ── Join reference file ────────────────────────────────────────────────
    if ref_df is not None and "BTS IP ID" in df.columns:
        # Merge on BTS IP ID = BTSIPID
        df = df.merge(ref_df, left_on="BTS IP ID", right_on="BTSIPID", how="left",
                      suffixes=("", "_ref"))

        # Authoritative SDCA: prefer ref file; fall back to data file SDCA
        if "SDCA_ref" in df.columns:
            df["SDCA"] = df["SDCA_ref"].combine_first(
                df["SDCA"].str.strip().str.title() if "SDCA" in df.columns else pd.Series(dtype=str))
            df.drop(columns=["SDCA_ref"], inplace=True, errors="ignore")
        elif "SDCA" in df.columns:
            df["SDCA"] = df["SDCA"].str.strip().str.title()

        # Drop duplicate BTSIPID column from merge
        df.drop(columns=["BTSIPID"], inplace=True, errors="ignore")
    else:
        if "SDCA" in df.columns:
            df["SDCA"] = df["SDCA"].str.strip().str.title()

    return df

# ─────────────────────────── SAFE STYLER ──────────────────────────────────────
# Bulletproof wrapper: handles applymap→map rename (pandas ≥2.1),
# non-unique index, duplicate columns, and any other Styler errors.
# Falls back to plain dataframe if styling fails for any reason.

def _avail_colour(val):
    """Red <90 %, orange 90-95 %, green ≥ 95 %."""
    try:
        v = float(val)
        if v < 90: return "background-color:#ffcccc"
        if v < 95: return "background-color:#fff3cd"
        return "background-color:#d4edda"
    except Exception:
        return ""

def _poor_colour(val):
    """Red <90 %, amber 90-95 %, no colour otherwise."""
    try:
        v = float(val)
        if v < 90: return "background-color:#ffcccc; color:#900"
        if v < 95: return "background-color:#fff3cd; color:#664"
    except Exception:
        pass
    return ""

def safe_style(df, colour_fn=_avail_colour, subset_cols=None):
    """Return a styled DataFrame or plain DataFrame if styling fails."""
    # 1. Always reset index so it's unique (0, 1, 2, …)
    df2 = df.reset_index(drop=True).copy()
    # 2. Drop duplicate columns (keep first occurrence)
    df2 = df2.loc[:, ~df2.columns.duplicated()]
    # 3. Filter subset to only columns that exist
    if subset_cols is not None:
        subset_cols = [c for c in subset_cols if c in df2.columns]
        if not subset_cols:
            subset_cols = None
    try:
        styler = df2.style.map(colour_fn, subset=subset_cols)
        styler._compute()       # validate eagerly – raises if still broken
        return styler
    except Exception:
        return df2              # safe fallback: plain dataframe

# ─────────────────────────── SESSION STATE ────────────────────────────────────

if "master_df" not in st.session_state:
    st.session_state.master_df = None
if "ref_df" not in st.session_state:
    st.session_state.ref_df = None
if "rev_df" not in st.session_state:         # dict: month_label → DataFrame
    st.session_state.rev_df = {}

# ─────────────────────────── SIDEBAR ──────────────────────────────────────────

with st.sidebar:
    st.title("📡 KKD Dashboard")

    # ── Reference file ─────────────────────────────────────────────────────
    st.markdown("**① Upload Reference File** (BTSIPID_PKEY1)")
    ref_upload = st.file_uploader("Reference file", type=["xlsx"], key="ref_upload")
    if ref_upload:
        try:
            st.session_state.ref_df = load_ref_file(ref_upload)
            st.success(f"✅ Reference: {len(st.session_state.ref_df)} sites loaded")
        except Exception as e:
            st.error(f"Reference load error: {e}")

    st.markdown("**② Upload Monthly Performance Files** (CSV / XLSX)")
    uploads = st.file_uploader("Monthly files", type=["csv", "xlsx"],
                                accept_multiple_files=True, key="perf_upload")
    if uploads:
        dfs = []
        ref = st.session_state.ref_df   # may be None
        for f in uploads:
            try:
                raw = load_perf_file(f)
                dfs.append(standardize(raw, ref))
            except Exception as e:
                st.error(f"Error loading {f.name}: {e}")
        if dfs:
            st.session_state.master_df = pd.concat(dfs, ignore_index=True)
            months = sorted(st.session_state.master_df["Month_Label"].unique(),
                            key=month_sort_key)
            st.success(f"✅ {len(dfs)} file(s) · {len(st.session_state.master_df)} records")
            st.markdown(f"**Months:** {', '.join(months)}")

    st.markdown("**③ Upload Revenue Files** (RBC — one file per month)")
    st.caption("Auto-detects month from filename: e.g. RBC_FEB_2026.xlsx → feb26")
    rev_uploads = st.file_uploader("RBC Revenue files", type=["xlsx","csv"],
                                   accept_multiple_files=True, key="rev_upload")
    if rev_uploads:
        for rf in rev_uploads:
            try:
                parts = rf.name.upper().replace("-","_").replace(" ","_")
                parts = parts.replace(".XLSX","").replace(".XLS","").replace(".CSV","").split("_")
                month_abbrs = {"JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"}
                m_lbl = None
                for i, p in enumerate(parts):
                    if p in month_abbrs:
                        yr_part = [x for x in parts[i+1:] if x.isdigit() and len(x)==4]
                        yr = yr_part[0][-2:] if yr_part else "??"
                        m_lbl = f"{p.lower()}{yr}"
                        break
                if m_lbl is None:
                    m_lbl = rf.name.lower().replace(".xlsx","").replace(".csv","").replace(" ","_")
                xf = pd.ExcelFile(rf)
                sheet = "RBC DATA" if "RBC DATA" in xf.sheet_names else xf.sheet_names[0]
                rdf = pd.read_excel(rf, sheet_name=sheet)
                rdf = rdf[rdf["SSACODE"].astype(str).str.strip() == "KKD"].copy()
                rev_num_cols = ["2G_Traffic","2G_Data","3G_Traffic","3G_Data","4G_Traffic","4G_Data",
                                "TOT_TRAFFIC","TOT_DATA","TRAFFIC_REV","DATA_REV","TOT_REV","REV_LAKH",
                                "2g_rev","3g_rev","4g_rev","Perday_2G_Erl","Perday_3G_GB","Perday_4G_GB"]
                for c in rev_num_cols:
                    if c in rdf.columns:
                        rdf[c] = pd.to_numeric(rdf[c], errors="coerce")
                rdf["BTSIPID"] = rdf["BTSIPID"].astype(str).str.strip()
                if "SDCANAME" in rdf.columns:
                    rdf["SDCA"] = rdf["SDCANAME"].str.strip().str.title()
                rdf["Rev_Month"] = m_lbl
                st.session_state.rev_df[m_lbl] = rdf
                st.success(f"✅ Revenue {m_lbl.upper()}: {len(rdf)} KKD sites")
            except Exception as e:
                st.error(f"Revenue load error ({rf.name}): {e}")

    if st.session_state.rev_df:
        rev_months_loaded = sorted(st.session_state.rev_df.keys(), key=month_sort_key)
        st.markdown(f"**Revenue months loaded:** {', '.join(m.upper() for m in rev_months_loaded)}")

    st.markdown("---")

    if st.session_state.master_df is not None:
        all_months = sorted(st.session_state.master_df["Month_Label"].unique(),
                            key=month_sort_key)
        # Default = ALL months selected (latest is shown in overview automatically)
        sel_months = st.multiselect("Filter Months (for trend tabs)", all_months,
                                    default=all_months)

        # Re-apply reference if ref loaded after perf files
        if ref_upload and st.session_state.ref_df is not None:
            if "incharge" not in st.session_state.master_df.columns:
                st.info("Re-upload performance files to apply reference enrichment.")
    else:
        sel_months = []

if st.session_state.master_df is None:
    st.info("👆  **Step 1:** Upload the reference file (BTSIPID_PKEY1_excel.xlsx)\n\n"
            "👆  **Step 2:** Upload monthly performance files (Jan CSV, Dec XLSX, …)")
    st.stop()

df_all = st.session_state.master_df.copy()
if sel_months:
    df_all = df_all[df_all["Month_Label"].isin(sel_months)]

months_sorted = sorted(df_all["Month_Label"].unique(), key=month_sort_key)
latest_month  = months_sorted[-1]    # ← always the chronologically latest
prev_month    = months_sorted[-2] if len(months_sorted) >= 2 else None

AVAIL_MAP      = {"2G": "Nw Avail (2G)", "3G": "Nw Avail (3G)", "4G TCS": "Nw Avail (4G TCS)"}
existing_avail = {k: v for k, v in AVAIL_MAP.items() if v in df_all.columns}

has_incharge     = "incharge"      in df_all.columns
has_jto_incharge = "JTO INCHARGE" in df_all.columns
has_location     = "LOCATION"     in df_all.columns
has_sitename     = "SITENAME"     in df_all.columns

# ── Latest-month dataframe (defined here so revenue globals can use it) ────────
df_lat = df_all[df_all["Month_Label"] == latest_month].copy()

# ── Revenue data globals ───────────────────────────────────────────────────────
rev_store    = st.session_state.rev_df               # dict month_label→df
has_revenue  = bool(rev_store)
# Combined revenue dataframe (all months stacked)
if has_revenue:
    rev_all  = pd.concat(rev_store.values(), ignore_index=True)
    # Latest revenue month (chronologically)
    rev_months_sorted = sorted(rev_store.keys(), key=month_sort_key)
    latest_rev_month  = rev_months_sorted[-1]
    rev_lat           = rev_store[latest_rev_month].copy()
    # Merge latest perf + latest rev on BTS IP ID = BTSIPID
    df_lat_rev = df_lat.merge(
        rev_lat[["BTSIPID","REV_LAKH","TOT_REV","TRAFFIC_REV","DATA_REV",
                 "2G_Traffic","2G_Data","3G_Traffic","3G_Data","4G_Traffic","4G_Data",
                 "TOT_TRAFFIC","TOT_DATA","2g_rev","3g_rev","4g_rev",
                 "Perday_2G_Erl","Perday_3G_GB","Perday_4G_GB",
                 "2G_Cat","3G_Cat","4G_Cat","2G TECH","3G TECH","4G TECH"]],
        left_on="BTS IP ID", right_on="BTSIPID", how="left", suffixes=("","_rbc")
    )
    # Tech category order for sorting
    CAT_ORDER = ["VHT","HT","MT","LT","VLT"]
else:
    rev_all = None; rev_lat = None; df_lat_rev = None; latest_rev_month = None
    rev_months_sorted = []; CAT_ORDER = []

# ─────────────────────────── TABS ─────────────────────────────────────────────

tab_labels = [
    "📊 Monthly Overview",
    "📈 MoM Shifts",
    "📉 Historical Trends",
    "🗺️ SDCA Drill-down",
    "🔗 Correlation",
    "🚀 Top / Bottom 25 Sites",
    "🔄 Technology Shift",
    "👷 Incharge Analysis",
    "💰 Revenue Report",
    "📅 Revenue Per Day",
    "🏆 Executive Report",
]
tabs = st.tabs(tab_labels)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 – Monthly Overview
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[0]:
    st.header(f"Monthly Overview — {latest_month.upper()}")
    st.caption("Dashboard always shows the latest month by default. Use sidebar to filter months for trend tabs.")

    # ── KPI row ────────────────────────────────────────────────────────────
    total_sites  = df_lat["BTS IP ID"].nunique()
    phys_4g      = df_lat[df_lat["Has_4G_Physical"]]["BTS IP ID"].nunique()
    s2g_total    = int((df_lat["2G cnt"] > 0).sum()) if "2G cnt" in df_lat.columns else 0
    s3g_total    = int((df_lat["3G cnt"] > 0).sum()) if "3G cnt" in df_lat.columns else 0
    avg2g = df_lat["Nw Avail (2G)"].mean()      if "Nw Avail (2G)"      in df_lat.columns else np.nan
    avg3g = df_lat["Nw Avail (3G)"].mean()      if "Nw Avail (3G)"      in df_lat.columns else np.nan
    avg4g = df_lat[df_lat["Has_4G_Physical"]]["Nw Avail (4G TCS)"].mean() if "Nw Avail (4G TCS)" in df_lat.columns else np.nan

    k1, k2, k3, k4, k5, k6, k7 = st.columns(7)
    k1.metric("Total Sites", total_sites)
    k2.metric("2G Sites",    s2g_total)
    k3.metric("3G Sites",    s3g_total)
    k4.metric("4G Physical", phys_4g)
    k5.metric("Avg 2G %",   f"{avg2g:.2f}%" if not np.isnan(avg2g) else "N/A")
    k6.metric("Avg 3G %",   f"{avg3g:.2f}%" if not np.isnan(avg3g) else "N/A")
    k7.metric("Avg 4G %",   f"{avg4g:.2f}%" if not np.isnan(avg4g) else "N/A")

    # ── Overall Master Summary ──────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Overall Master Summary")

    s700_total  = int(df_lat["BTS Site ID (700)"].notna().sum())  if "BTS Site ID (700)"  in df_lat.columns else 0
    s2100_total = int(df_lat["BTS Site ID (2100)"].notna().sum()) if "BTS Site ID (2100)" in df_lat.columns else 0
    s2500_total = int(df_lat["BTS Site ID (2500)"].notna().sum()) if "BTS Site ID (2500)" in df_lat.columns else 0
    st_counts   = df_lat["Site Type"].value_counts().to_dict() if "Site Type" in df_lat.columns else {}

    master_rows = [
        {"Category": "Total Unique Sites",          "Value": total_sites,      "Note": "All physical sites"},
        {"Category": "2G Active Sites",              "Value": s2g_total,        "Note": f"{round(s2g_total/total_sites*100,1)}% of total"},
        {"Category": "3G Active Sites",              "Value": s3g_total,        "Note": f"{round(s3g_total/total_sites*100,1)}% of total"},
        {"Category": "4G Physical Sites",            "Value": phys_4g,          "Note": f"{round(phys_4g/total_sites*100,1)}% of total"},
        {"Category": "  4G 700MHz band",             "Value": s700_total,       "Note": "Band-level count"},
        {"Category": "  4G 2100MHz band",            "Value": s2100_total,      "Note": "Band-level count"},
        {"Category": "  4G 2500MHz band",            "Value": s2500_total,      "Note": "Band-level count"},
        {"Category": "Avg 2G Availability %",        "Value": f"{avg2g:.2f}%",  "Note": "Network-wide"},
        {"Category": "Avg 3G Availability %",        "Value": f"{avg3g:.2f}%",  "Note": "Network-wide"},
        {"Category": "Avg 4G Availability %",        "Value": f"{avg4g:.2f}%",  "Note": "Physical 4G sites"},
    ]
    for stype, cnt in sorted(st_counts.items()):
        master_rows.append({"Category": f"Site Type: {stype}", "Value": cnt,
                             "Note": f"{round(cnt/total_sites*100,1)}% of total"})
    if "Erl Total"    in df_lat.columns:
        master_rows.append({"Category": "Total Traffic (Erl)",  "Value": round(df_lat["Erl Total"].sum(), 1),    "Note": "All technologies"})
    if "Data GB Total" in df_lat.columns:
        master_rows.append({"Category": "Total Data (GB)",       "Value": round(df_lat["Data GB Total"].sum(), 1),"Note": "All technologies"})
    if has_incharge:
        master_rows.append({"Category": "Incharge Officers",     "Value": df_lat["incharge"].nunique(),            "Note": "Unique incharge"})
    if has_jto_incharge:
        master_rows.append({"Category": "JTO Incharge Units",    "Value": df_lat["JTO INCHARGE"].nunique(),        "Note": "Unique JTO incharge"})

    col_ms1, col_ms2 = st.columns([1, 1.8])
    with col_ms1:
        st.dataframe(pd.DataFrame(master_rows), use_container_width=True, hide_index=True)
    with col_ms2:
        bd = {
            "Category": (["2G Sites","3G Sites","4G Physical","700MHz","2100MHz","2500MHz"]
                         + [f"Type-{k}" for k in sorted(st_counts.keys())]),
            "Count":    ([s2g_total, s3g_total, phys_4g, s700_total, s2100_total, s2500_total]
                         + [st_counts[k] for k in sorted(st_counts.keys())]),
            "Group":    (["Technology"]*6 + ["Site Type"]*len(st_counts)),
        }
        fig_bd = px.bar(pd.DataFrame(bd), x="Category", y="Count", color="Group",
                        text="Count", title="Overall Site Count Breakdown",
                        color_discrete_map={"Technology": "#636EFA", "Site Type": "#EF553B"})
        fig_bd.update_traces(textposition="outside")
        fig_bd.update_layout(xaxis_tickangle=-30)
        st.plotly_chart(fig_bd, use_container_width=True)

    st.markdown("---")

    # ── SDCA-wise Site Count ────────────────────────────────────────────────
    st.subheader("📍 SDCA-wise Site Count")

    sdca_total = df_lat.groupby("SDCA")["BTS IP ID"].nunique().reset_index()
    sdca_total.columns = ["SDCA", "Total Sites"]
    for label, flag_col, is_phys in [("2G Sites","2G cnt",False),("3G Sites","3G cnt",False),("4G Sites (Physical)",None,True)]:
        if is_phys:
            tmp = df_lat[df_lat["Has_4G_Physical"]].groupby("SDCA")["BTS IP ID"].nunique().rename(label)
        elif flag_col in df_lat.columns:
            tmp = df_lat[df_lat[flag_col]>0].groupby("SDCA")["BTS IP ID"].nunique().rename(label)
        else:
            continue
        sdca_total = sdca_total.merge(tmp.reset_index(), on="SDCA", how="left")
    for band, col in [("4G 700MHz","BTS Site ID (700)"),("4G 2100MHz","BTS Site ID (2100)"),("4G 2500MHz","BTS Site ID (2500)")]:
        if col in df_lat.columns:
            tmp = df_lat[df_lat[col].notna()].groupby("SDCA")["BTS IP ID"].nunique().rename(band)
            sdca_total = sdca_total.merge(tmp.reset_index(), on="SDCA", how="left")

    sdca_total = sdca_total.fillna(0).sort_values("Total Sites", ascending=False)
    int_cols   = [c for c in sdca_total.columns if c != "SDCA"]
    sdca_total[int_cols] = sdca_total[int_cols].astype(int)

    col_a, col_b = st.columns([1.4, 1])
    with col_a:
        fig_sdca = px.bar(sdca_total, x="SDCA", y="Total Sites", color="Total Sites",
                          color_continuous_scale="Blues", text="Total Sites",
                          title="Total Site Count per SDCA")
        fig_sdca.update_traces(textposition="outside")
        fig_sdca.update_layout(coloraxis_showscale=False, xaxis_tickangle=-30)
        st.plotly_chart(fig_sdca, use_container_width=True)
    with col_b:
        st.markdown("**SDCA Site Count Table**")
        st.dataframe(sdca_total.reset_index(drop=True), use_container_width=True, height=380)

    # SDCA × Technology grouped bar + table
    melt_cols = [c for c in ["2G Sites","3G Sites","4G Sites (Physical)"] if c in sdca_total.columns]
    if melt_cols:
        fig_st = px.bar(sdca_total.melt("SDCA", value_vars=melt_cols, var_name="Technology", value_name="Sites"),
                        x="SDCA", y="Sites", color="Technology", barmode="group", text="Sites",
                        title="SDCA-wise Site Count by Technology",
                        color_discrete_map={"2G Sites":"#636EFA","3G Sites":"#EF553B","4G Sites (Physical)":"#00CC96"})
        fig_st.update_traces(textposition="outside")
        fig_st.update_layout(xaxis_tickangle=-30)
        st.plotly_chart(fig_st, use_container_width=True)

    st.markdown("**📋 SDCA × Technology Detailed Table**")
    disp_cols = ["SDCA","Total Sites"] + melt_cols + [c for c in ["4G 700MHz","4G 2100MHz","4G 2500MHz"] if c in sdca_total.columns]
    st.dataframe(sdca_total[disp_cols].reset_index(drop=True), use_container_width=True)

    st.markdown("---")

    # ── Availability heatmap with values ───────────────────────────────────
    st.subheader("🌡️ SDCA × Technology Availability Heatmap")
    avail_heat = [c for c in ["Nw Avail (2G)","Nw Avail (3G)","Nw Avail (4G TCS)"] if c in df_lat.columns]
    if avail_heat and "SDCA" in df_lat.columns:
        av_df = df_lat.groupby("SDCA")[avail_heat].mean().round(2).reset_index()
        short  = [c.replace("Nw Avail (","").replace(")","") for c in avail_heat]
        av_df.columns = ["SDCA"] + short
        z = av_df[short].values
        fig_hm = go.Figure(data=go.Heatmap(
            z=z, x=short, y=av_df["SDCA"].tolist(),
            colorscale="RdYlGn", zmin=80, zmax=100,
            text=[[f"{v:.2f}%" if not np.isnan(v) else "N/A" for v in row] for row in z],
            texttemplate="%{text}", textfont={"size":13,"color":"black"},
        ))
        fig_hm.update_layout(title="Avg Network Availability % — SDCA × Technology", height=420)
        st.plotly_chart(fig_hm, use_container_width=True)

    st.markdown("---")

    # ── Technology-wise Data Summary ────────────────────────────────────────
    st.subheader("📶 Technology-wise Data Summary")
    tech_defs = [
        ("2G",          "2G cnt",  "Nw Avail (2G)",     "Erl (2g)",  "Data GB (2g)"),
        ("3G",          "3G cnt",  "Nw Avail (3G)",     "Erl (3g)",  "Data GB (3g)"),
        ("4G Physical", None,      "Nw Avail (4G TCS)", "Erl Total", "Data GB Total"),
    ]
    band_defs = [
        ("4G 700MHz",  "BTS Site ID (700)",  "Nw Avail (4G TCS)","Erl (700)",  "Data GB (700)"),
        ("4G 2100MHz", "BTS Site ID (2100)", "Nw Avail (4G TCS)","Erl (2100)", "Data GB (2100)"),
        ("4G 2500MHz", "BTS Site ID (2500)", "Nw Avail (4G TCS)","Erl (2500)", "Data GB (2500)"),
    ]
    summ_rows = []
    for tech, cnt_col, avail_col, erl_col, data_col in tech_defs:
        sub = df_lat[df_lat["Has_4G_Physical"]] if tech.startswith("4G") else (
              df_lat[df_lat[cnt_col]>0] if cnt_col and cnt_col in df_lat.columns else df_lat)
        summ_rows.append({
            "Technology": tech,
            "Sites": sub["BTS IP ID"].nunique(),
            "Avg Avail %": round(sub[avail_col].mean(),2) if avail_col in sub.columns else "N/A",
            "Total Erl":   round(sub[erl_col].sum(),2)   if erl_col  in sub.columns else "N/A",
            "Total Data GB": round(sub[data_col].sum(),2) if data_col in sub.columns else "N/A",
        })
    for band, bc, avail_col, erl_col, data_col in band_defs:
        if bc in df_lat.columns:
            sub = df_lat[df_lat[bc].notna()]
            summ_rows.append({
                "Technology": band,
                "Sites": sub["BTS IP ID"].nunique(),
                "Avg Avail %": round(sub[avail_col].mean(),2) if avail_col in sub.columns else "N/A",
                "Total Erl":   round(sub[erl_col].sum(),2)   if erl_col  in sub.columns else "N/A",
                "Total Data GB": round(sub[data_col].sum(),2) if data_col in sub.columns else "N/A",
            })
    st.dataframe(pd.DataFrame(summ_rows), use_container_width=True)

    col_t1, col_t2 = st.columns(2)
    with col_t1:
        erl_r = [(r["Technology"],r["Total Erl"]) for r in summ_rows[:3] if isinstance(r["Total Erl"],float)]
        if erl_r:
            st.plotly_chart(px.pie(pd.DataFrame(erl_r,columns=["Technology","Erl"]),
                names="Technology",values="Erl",title="Traffic Share by Technology",hole=0.4),
                use_container_width=True)
    with col_t2:
        dat_r = [(r["Technology"],r["Total Data GB"]) for r in summ_rows[:3] if isinstance(r["Total Data GB"],float)]
        if dat_r:
            st.plotly_chart(px.pie(pd.DataFrame(dat_r,columns=["Technology","GB"]),
                names="Technology",values="GB",title="Data Volume Share by Technology",hole=0.4),
                use_container_width=True)

    st.markdown("---")

    # ── Site Type-wise Count ────────────────────────────────────────────────
    st.subheader("🏗️ Site Type-wise Count")
    if "Site Type" in df_lat.columns:
        stc = df_lat["Site Type"].value_counts().reset_index()
        stc.columns = ["Site Type","Count"]
        stc["% Share"] = (stc["Count"]/stc["Count"].sum()*100).round(1)

        # Rich summary
        avail_st = [c for c in ["Nw Avail (2G)","Nw Avail (3G)","Nw Avail (4G TCS)"] if c in df_lat.columns]
        traf_st  = [c for c in ["Erl (2g)","Erl (3g)","Erl Total","Data GB Total"] if c in df_lat.columns]
        st_summ  = stc.copy()
        if avail_st:
            av = df_lat.groupby("Site Type")[avail_st].mean().round(2)
            av.columns = [c.replace("Nw Avail ","Avail ") for c in avail_st]
            st_summ = st_summ.merge(av.reset_index(), on="Site Type", how="left")
        if traf_st:
            tr = df_lat.groupby("Site Type")[traf_st].sum().round(1)
            st_summ = st_summ.merge(tr.reset_index(), on="Site Type", how="left")
        for tl, fc, bc in [("2G Active","2G cnt",None),("3G Active","3G cnt",None),("4G Physical",None,"Has_4G_Physical")]:
            if bc: tmp = df_lat[df_lat[bc]].groupby("Site Type")["BTS IP ID"].nunique().rename(tl)
            elif fc and fc in df_lat.columns: tmp = df_lat[df_lat[fc]>0].groupby("Site Type")["BTS IP ID"].nunique().rename(tl)
            else: continue
            st_summ = st_summ.merge(tmp.reset_index(), on="Site Type", how="left")

        st.markdown("**📋 Overall Site Type Summary**")
        st.dataframe(st_summ.fillna(0).reset_index(drop=True), use_container_width=True)

        col_s1, col_s2 = st.columns(2)
        with col_s1:
            fig_stpie = px.pie(stc, names="Site Type", values="Count", hole=0.4,
                               title="Site Distribution by Type",
                               color_discrete_sequence=px.colors.qualitative.Set2)
            fig_stpie.update_traces(textinfo="label+percent+value")
            st.plotly_chart(fig_stpie, use_container_width=True)
        with col_s2:
            fig_stbar = px.bar(stc, x="Site Type", y="Count", color="Site Type",
                               text="Count", title="Site Count by Type",
                               color_discrete_sequence=px.colors.qualitative.Set2)
            fig_stbar.update_traces(textposition="outside")
            fig_stbar.update_layout(showlegend=False)
            st.plotly_chart(fig_stbar, use_container_width=True)

        if avail_st:
            st_av = df_lat.groupby("Site Type")[avail_st].mean().round(2).reset_index()
            fig_stav = px.bar(st_av.melt("Site Type",var_name="Technology",value_name="Avg Avail %"),
                              x="Site Type", y="Avg Avail %", color="Technology", barmode="group",
                              text="Avg Avail %", title="Avg Availability % by Site Type & Technology")
            fig_stav.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig_stav.update_layout(yaxis_range=[85,102])
            st.plotly_chart(fig_stav, use_container_width=True)

        if "SDCA" in df_lat.columns:
            st.markdown("**SDCA-wise Site Type Distribution**")
            st_sdca_long = df_lat.groupby(["SDCA","Site Type"])["BTS IP ID"].nunique().reset_index()
            st_sdca_long.columns = ["SDCA","Site Type","Count"]

            col_stb, col_stt = st.columns([1.6,1])
            with col_stb:
                fig_stsdca = px.bar(st_sdca_long, x="SDCA", y="Count", color="Site Type",
                                    barmode="stack", text="Count",
                                    color_discrete_sequence=px.colors.qualitative.Set2,
                                    title="Site Type Count per SDCA")
                fig_stsdca.update_traces(textposition="inside")
                fig_stsdca.update_layout(xaxis_tickangle=-30)
                st.plotly_chart(fig_stsdca, use_container_width=True)
            with col_stt:
                st.markdown("**📋 SDCA × Site Type Table**")
                pivot_st = st_sdca_long.pivot(index="SDCA",columns="Site Type",values="Count").fillna(0).astype(int)
                pivot_st["Total"] = pivot_st.sum(axis=1)
                pivot_st = pivot_st.sort_values("Total", ascending=False)
                pivot_st.loc["TOTAL"] = pivot_st.sum()
                st.dataframe(pivot_st, use_container_width=True)

            # Detailed SDCA × Site Type table
            st.markdown("**📋 SDCA × Site Type — Full Breakdown**")
            det = []
            for sdca_v in sorted(df_lat["SDCA"].dropna().unique()):
                for stype in sorted(df_lat["Site Type"].dropna().unique()):
                    sub = df_lat[(df_lat["SDCA"]==sdca_v) & (df_lat["Site Type"]==stype)]
                    if not len(sub): continue
                    row = {"SDCA":sdca_v,"Site Type":stype,"Total Sites":sub["BTS IP ID"].nunique()}
                    if "2G cnt" in sub.columns: row["2G Active"] = int((sub["2G cnt"]>0).sum())
                    if "3G cnt" in sub.columns: row["3G Active"] = int((sub["3G cnt"]>0).sum())
                    row["4G Physical"] = int(sub[sub["Has_4G_Physical"]]["BTS IP ID"].nunique())
                    for ac in avail_st:
                        row[ac.replace("Nw Avail ","Avail ")] = round(sub[ac].mean(),2) if not sub[ac].isna().all() else None
                    if "Erl Total" in sub.columns:    row["Traffic (Erl)"] = round(sub["Erl Total"].sum(),1)
                    if "Data GB Total" in sub.columns: row["Data (GB)"]     = round(sub["Data GB Total"].sum(),1)
                    det.append(row)
            st.dataframe(pd.DataFrame(det).fillna("—").reset_index(drop=True), use_container_width=True)

            # Site Type × SDCA 4G availability heatmap
            if "Nw Avail (4G TCS)" in df_lat.columns:
                hm_piv = df_lat.pivot_table(index="SDCA",columns="Site Type",
                                             values="Nw Avail (4G TCS)",aggfunc="mean").round(2)
                z_hm = hm_piv.values
                fig_sthm = go.Figure(data=go.Heatmap(
                    z=z_hm, x=hm_piv.columns.tolist(), y=hm_piv.index.tolist(),
                    colorscale="RdYlGn", zmin=85, zmax=100,
                    text=[[f"{v:.1f}%" if not np.isnan(v) else "—" for v in row] for row in z_hm],
                    texttemplate="%{text}", textfont={"size":12,"color":"black"},
                ))
                fig_sthm.update_layout(title="Avg 4G Availability % — SDCA × Site Type", height=420)
                st.plotly_chart(fig_sthm, use_container_width=True)

    # ── LOCATION-based Site Type + Discrepancy Report ──────────────────────
    if has_location:
        st.markdown("---")
        st.subheader("📌 Site Type vs LOCATION — Analysis & Discrepancy Report")
        st.caption("LOCATION from the reference file maps to Site Type: "
                   "**BSNL → BS · NBSNL → NB · IP → IP · USO_Saturation → SA**. "
                   "Both fields represent the same concept — discrepancies may indicate data entry mismatches.")

        _loc_map = {"BSNL":"BS", "NBSNL":"NB", "IP":"IP", "USO_Saturation":"SA"}
        df_lat_loc = df_lat.copy()
        df_lat_loc["Expected_SiteType"] = df_lat_loc["LOCATION"].map(_loc_map)

        # ── Summary counts: LOCATION-derived vs actual Site Type ───────────
        col_loc1, col_loc2 = st.columns(2)
        with col_loc1:
            loc_cnt = df_lat_loc["LOCATION"].value_counts().reset_index()
            loc_cnt.columns = ["LOCATION","Count (from ref file)"]
            loc_cnt["Mapped Site Type"] = loc_cnt["LOCATION"].map(_loc_map).fillna("Unmatched")
            st.markdown("**LOCATION Count (Reference File)**")
            st.dataframe(loc_cnt, use_container_width=True, hide_index=True)
            st.plotly_chart(px.pie(loc_cnt, names="LOCATION", values="Count (from ref file)",
                                   hole=0.4, title="Sites by LOCATION Type",
                                   color_discrete_sequence=px.colors.qualitative.Set2),
                            use_container_width=True)
        with col_loc2:
            st_cnt = df_lat_loc["Site Type"].value_counts().reset_index()
            st_cnt.columns = ["Site Type","Count (from data file)"]
            st.markdown("**Site Type Count (Performance Data File)**")
            st.dataframe(st_cnt, use_container_width=True, hide_index=True)
            st.plotly_chart(px.pie(st_cnt, names="Site Type", values="Count (from data file)",
                                   hole=0.4, title="Sites by Site Type",
                                   color_discrete_sequence=px.colors.qualitative.Pastel),
                            use_container_width=True)

        # ── Cross-tab matrix ───────────────────────────────────────────────
        st.markdown("**Cross-tab: Site Type (data file) × LOCATION (reference file)**")
        ct = pd.crosstab(
            df_lat_loc["Site Type"].fillna("Unknown"),
            df_lat_loc["LOCATION"].fillna("Unmatched"),
            margins=True, margins_name="TOTAL"
        )
        st.dataframe(ct, use_container_width=True)

        # ── Discrepancy report ─────────────────────────────────────────────
        disc = df_lat_loc[
            df_lat_loc["Expected_SiteType"].notna() &
            (df_lat_loc["Site Type"] != df_lat_loc["Expected_SiteType"])
        ].copy()
        agree_count   = int(((df_lat_loc["Expected_SiteType"].notna()) & (df_lat_loc["Site Type"] == df_lat_loc["Expected_SiteType"])).sum())
        unmatched_ref = int(df_lat_loc["Expected_SiteType"].isna().sum())

        m1d, m2d, m3d = st.columns(3)
        m1d.metric("✅ Agree (SiteType = LOCATION)", agree_count)
        m2d.metric("⚠️ Discrepancies", len(disc), delta=f"{round(len(disc)/len(df_lat_loc)*100,1)}% of sites", delta_color="inverse")
        m3d.metric("❓ No Reference Match", unmatched_ref)

        if len(disc) > 0:
            st.markdown(f"**⚠️ Discrepancy Detail — {len(disc)} sites where Site Type ≠ LOCATION-derived type**")
            disc_show = disc[["BTS IP ID","BTS Name","SDCA","Site Type","LOCATION","Expected_SiteType"]].copy()
            disc_show = disc_show.rename(columns={
                "Site Type":          "Site Type (data file)",
                "LOCATION":           "LOCATION (ref file)",
                "Expected_SiteType":  "Expected Site Type (from LOCATION)"
            }).reset_index(drop=True)
            # Highlight mismatch columns
            def _highlight_disc(val):
                return "background-color:#ffcccc; font-weight:bold"
            st.dataframe(safe_style(disc_show, _highlight_disc,
                         ["Site Type (data file)","Expected Site Type (from LOCATION)"]),
                         use_container_width=True)

            # Breakdown chart
            disc_grp = disc.groupby(["Site Type","LOCATION"])["BTS IP ID"].count().reset_index()
            disc_grp.columns = ["Site Type (data)","LOCATION (ref)","Count"]
            disc_grp["Label"] = disc_grp["Site Type (data)"] + " → " + disc_grp["LOCATION (ref)"]
            st.plotly_chart(
                px.bar(disc_grp, x="Label", y="Count", color="Site Type (data)",
                       text="Count", title="Discrepancy Breakdown: Site Type → LOCATION",
                       color_discrete_sequence=px.colors.qualitative.Set1)
                .update_traces(textposition="outside"),
                use_container_width=True
            )
        else:
            st.success("✅ No discrepancies found — Site Type and LOCATION are fully consistent!")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 – MoM Shifts
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[1]:
    st.header("Month-over-Month Availability Shifts")
    if len(months_sorted) < 2:
        st.warning("Need at least 2 months of data.")
    else:
        mom = df_all.groupby("Month_Label")[[v for v in existing_avail.values()]].mean().reset_index()
        mom["sort_key"] = mom["Month_Label"].apply(month_sort_key)
        mom = mom.sort_values("sort_key").drop(columns="sort_key")
        mom_m = mom.melt("Month_Label", var_name="Technology", value_name="Avg Avail %")
        mom_m["Technology"] = mom_m["Technology"].map({v:k for k,v in existing_avail.items()})
        st.plotly_chart(px.line(mom_m, x="Month_Label", y="Avg Avail %", color="Technology",
                                markers=True, title="MoM Availability Trend"),
                        use_container_width=True)
        m1, m2 = mom.iloc[-2], mom.iloc[-1]
        delta = [{"Technology":t,
                  mom["Month_Label"].iloc[-2]: f"{m1[c]:.2f}%",
                  mom["Month_Label"].iloc[-1]: f"{m2[c]:.2f}%",
                  "Δ": f"{m2[c]-m1[c]:+.2f}%"} for t,c in existing_avail.items()]
        st.subheader("MoM Delta Table")
        st.dataframe(pd.DataFrame(delta), use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 – Historical Trends
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[2]:
    st.header("Historical Trends — Traffic & Availability")
    erl_cols  = [c for c in ["Erl (2g)","Erl (3g)","Erl (2100)","Erl (2500)","Erl (700)","Erl Total"] if c in df_all.columns]
    data_cols = [c for c in ["Data GB (2g)","Data GB (3g)","Data GB (2100)","Data GB (2500)","Data GB (700)","Data GB Total"] if c in df_all.columns]
    avail_list = list(existing_avail.values())
    agg_d = {**{c:"sum" for c in erl_cols+data_cols}, **{c:"mean" for c in avail_list}}
    monthly = df_all.groupby("Month_Label")[erl_cols+data_cols+avail_list].agg(agg_d).reset_index()
    monthly["sort_key"] = monthly["Month_Label"].apply(month_sort_key)
    monthly = monthly.sort_values("sort_key").drop(columns="sort_key")

    col_a, col_b = st.columns(2)
    with col_a:
        if erl_cols:
            st.plotly_chart(px.line(monthly.melt("Month_Label",value_vars=erl_cols,var_name="Band",value_name="Erl"),
                x="Month_Label",y="Erl",color="Band",markers=True,title="Traffic (Erl) Trend"), use_container_width=True)
    with col_b:
        if data_cols:
            st.plotly_chart(px.area(monthly.melt("Month_Label",value_vars=data_cols,var_name="Band",value_name="Data GB"),
                x="Month_Label",y="Data GB",color="Band",title="Data Volume Trend"), use_container_width=True)
    if avail_list:
        st.plotly_chart(px.line(monthly.melt("Month_Label",value_vars=avail_list,var_name="Tech",value_name="Avail %"),
            x="Month_Label",y="Avail %",color="Tech",markers=True,title="Avg Availability Trend"), use_container_width=True)

    st.subheader("📋 Historical Trends Table")
    st.dataframe(monthly.round(2).reset_index(drop=True), use_container_width=True)
    if len(monthly) >= 2:
        st.markdown("**Month-over-Month Change (Δ)**")
        delta_df = monthly.set_index("Month_Label")[erl_cols+data_cols+avail_list].diff().round(2).dropna().reset_index()
        delta_df.columns = ["Month_Label"] + [f"Δ {c}" for c in erl_cols+data_cols+avail_list]
        st.dataframe(delta_df, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 – SDCA Drill-down (Previous vs Latest side-by-side)
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[3]:
    st.header("SDCA Drill-down")
    if "SDCA" not in df_all.columns:
        st.warning("SDCA column not found.")
    else:
        sdca_list  = sorted(df_all["SDCA"].dropna().unique())
        sel_sdca   = st.selectbox("Select SDCA", sdca_list)
        avail_show = [c for c in ["Nw Avail (2G)","Nw Avail (3G)","Nw Avail (4G TCS)"] if c in df_all.columns]
        df_sdca_all = df_all[df_all["SDCA"] == sel_sdca]

        if prev_month:
            df_prev  = df_sdca_all[df_sdca_all["Month_Label"] == prev_month]
            df_currt = df_sdca_all[df_sdca_all["Month_Label"] == latest_month]
            col_p, col_c = st.columns(2)
            for cw, dm, lbl in [(col_p, df_prev, f"⬅ {prev_month}"),
                                  (col_c, df_currt, f"➡ {latest_month} (Latest)")]:
                with cw:
                    st.markdown(f"### {lbl}")
                    st.metric("Sites", dm["BTS IP ID"].nunique())
                    for ac in avail_show:
                        tn = ac.replace("Nw Avail ","").replace("(","").replace(")","")
                        st.metric(f"Avg {tn} %",
                                  f"{dm[ac].mean():.2f}%" if len(dm) and not dm[ac].isna().all() else "N/A")
                    if len(dm):
                        t = dm.groupby(["BTS IP ID","BTS Name"])[avail_show].mean().round(2).reset_index()
                        t.columns = ["BTS IP ID","BTS Name"] + [c.replace("Nw Avail ","") for c in avail_show]
                        st.dataframe(t, use_container_width=True)

            if len(df_prev) and len(df_currt):
                st.subheader("Site-level Availability — Previous vs Latest")
                ps = df_prev.groupby("BTS IP ID")[avail_show].mean().round(2).reset_index(); ps["Month"]=prev_month
                cs = df_currt.groupby("BTS IP ID")[avail_show].mean().round(2).reset_index(); cs["Month"]=latest_month
                for ac in avail_show:
                    tl = ac.replace("Nw Avail ","").replace("(","").replace(")","")
                    fig_c = px.bar(pd.concat([ps,cs]), x="BTS IP ID", y=ac, color="Month",
                                   barmode="group", text=ac,
                                   title=f"{tl} — {prev_month} vs {latest_month}")
                    fig_c.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
                    fig_c.update_layout(xaxis_tickangle=-45, yaxis_range=[75,105])
                    st.plotly_chart(fig_c, use_container_width=True)

                st.subheader("📋 MoM Delta per Site")
                mg = ps.merge(cs, on="BTS IP ID", suffixes=(f"_{prev_month}",f"_{latest_month}"))
                for ac in avail_show:
                    mg[f"Δ {ac.replace('Nw Avail ','')}"] = (mg[f"{ac}_{latest_month}"] - mg[f"{ac}_{prev_month}"]).round(2)
                st.dataframe(mg[["BTS IP ID"]+[f"Δ {ac.replace('Nw Avail ','')}" for ac in avail_show]],
                             use_container_width=True)
        else:
            dl = df_sdca_all[df_sdca_all["Month_Label"] == latest_month]
            st.dataframe(dl.groupby(["BTS IP ID","BTS Name"])[avail_show].mean().round(2).reset_index(),
                         use_container_width=True)

        dl2 = df_sdca_all[df_sdca_all["Month_Label"] == latest_month]
        if len(dl2) and avail_show:
            st.plotly_chart(px.box(dl2.melt(id_vars="BTS IP ID",value_vars=avail_show,var_name="Tech",value_name="Avail %"),
                x="Tech", y="Avail %", title=f"Distribution — {sel_sdca} ({latest_month})"),
                use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 – Correlation
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[4]:
    st.header("Advanced Correlation — Traffic vs. Availability")
    num_cols = [c for c in [
        "Erl (2g)","Erl (3g)","Erl (2100)","Erl (2500)","Erl (700)","Erl Total",
        "Data GB (2g)","Data GB (3g)","Data GB (2100)","Data GB (2500)","Data GB (700)","Data GB Total",
        "Nw Avail (2G)","Nw Avail (3G)","Nw Avail (4G TCS)"
    ] if c in df_all.columns]
    if len(num_cols) >= 2:
        corr = df_all[num_cols].corr().round(3)
        fig_cr = px.imshow(corr, text_auto=".2f", color_continuous_scale="RdBu_r",
                           zmin=-1, zmax=1, title="Correlation Matrix")
        fig_cr.update_layout(height=560)
        st.plotly_chart(fig_cr, use_container_width=True)
        st.subheader("📋 Correlation Table")
        st.dataframe(corr, use_container_width=True)
        strong = [{"Variable A":corr.columns[i],"Variable B":corr.columns[j],
                   "r":corr.iloc[i,j],"Strength":"Strong" if abs(corr.iloc[i,j])>=0.7 else "Moderate",
                   "Direction":"Positive" if corr.iloc[i,j]>0 else "Negative"}
                  for i in range(len(corr.columns)) for j in range(i+1,len(corr.columns))
                  if abs(corr.iloc[i,j])>=0.5]
        if strong:
            st.markdown("**Strong Correlations (|r| ≥ 0.5)**")
            st.dataframe(pd.DataFrame(strong).sort_values("r",key=abs,ascending=False),
                         use_container_width=True)
        col_x = st.selectbox("X axis", num_cols, index=0)
        col_y = st.selectbox("Y axis", num_cols, index=min(len(num_cols)-1,12))
        st.plotly_chart(px.scatter(df_all, x=col_x, y=col_y,
            color="SDCA" if "SDCA" in df_all.columns else None,
            hover_data=["BTS IP ID","BTS Name"], trendline="ols",
            title=f"{col_x} vs {col_y}"), use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 6 – Top / Bottom 25 Sites
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[5]:
    st.header("Top 25 Growth & Bottom 25 Decline Sites")
    if len(months_sorted) < 2:
        st.info("Need ≥2 months of data.")
    else:
        m_first, m_last = months_sorted[0], months_sorted[-1]
        erl_col = next((c for c in ["Erl Total","Erl (2g)"] if c in df_all.columns), None)
        if erl_col:
            df_f = df_all[df_all["Month_Label"]==m_first].groupby("BTS IP ID")[erl_col].sum()
            df_l = df_all[df_all["Month_Label"]==m_last ].groupby("BTS IP ID")[erl_col].sum()
            growth = ((df_l-df_f)/df_f.replace(0,np.nan)*100).dropna()
            meta_cols = ["BTS IP ID","BTS Name","SDCA","Site Type"]
            if has_location:     meta_cols.append("LOCATION")
            if has_sitename:     meta_cols.append("SITENAME")
            if has_incharge:     meta_cols.append("incharge")
            if has_jto_incharge: meta_cols.append("JTO INCHARGE")
            if "Latitude"  in df_all.columns: meta_cols.append("Latitude")
            if "Longitude" in df_all.columns: meta_cols.append("Longitude")
            meta = (df_all[df_all["Month_Label"]==m_last]
                    [[c for c in meta_cols if c in df_all.columns]]
                    .drop_duplicates("BTS IP ID").set_index("BTS IP ID"))

            def build_tbl(series):
                t = series.reset_index(); t.columns=["BTS IP ID","Growth %"]
                t["Growth %"] = t["Growth %"].round(2)
                t = t.merge(meta.reset_index(), on="BTS IP ID", how="left")
                t[f"{m_first} Erl"] = df_f.reindex(t["BTS IP ID"]).values.round(2)
                t[f"{m_last} Erl"]  = df_l.reindex(t["BTS IP ID"]).values.round(2)
                return t

            top25 = build_tbl(growth.nlargest(25))
            bot25 = build_tbl(growth.nsmallest(25))

            tab_top, tab_bot = st.tabs([f"🚀 Top 25 ({m_first}→{m_last})", f"📉 Bottom 25 ({m_first}→{m_last})"])
            for tw, tbl, cs, ttl in [(tab_top,top25,"Greens","Top 25 Growth"),
                                      (tab_bot,bot25,"Reds","Bottom 25 Decline")]:
                with tw:
                    st.plotly_chart(px.bar(tbl, x="Growth %", y="BTS Name", orientation="h",
                        color="Growth %", color_continuous_scale=cs,
                        hover_data=["BTS IP ID","SDCA","Site Type"],
                        title=f"{ttl} — {m_first} → {m_last}").update_layout(
                        yaxis={"categoryorder":"total ascending"}, height=700),
                        use_container_width=True)
                    show = ["BTS IP ID","BTS Name","SDCA","Site Type",f"{m_first} Erl",f"{m_last} Erl","Growth %"]
                    if has_location:     show.append("LOCATION")
                    if has_sitename:     show.append("SITENAME")
                    if has_incharge:     show.append("incharge")
                    if has_jto_incharge: show.append("JTO INCHARGE")
                    if "Latitude" in tbl.columns: show += ["Latitude","Longitude"]
                    st.markdown("**📋 Detailed Table**")
                    st.dataframe(tbl[[c for c in show if c in tbl.columns]].reset_index(drop=True),
                                 use_container_width=True)
                    if "Latitude" in tbl.columns:
                        dm = tbl.dropna(subset=["Latitude","Longitude"])
                        if len(dm):
                            st.plotly_chart(px.scatter_mapbox(dm, lat="Latitude", lon="Longitude",
                                color="Growth %", size=dm["Growth %"].abs().clip(lower=1),
                                hover_name="BTS Name",
                                hover_data={c:True for c in ["BTS IP ID","SDCA","Site Type","Growth %"] if c in dm.columns},
                                color_continuous_scale=cs, mapbox_style="carto-positron", zoom=9,
                                title=f"{ttl} — Map"), use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 7 – Technology Shift
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[6]:
    st.header("Technology Shift — Migration Toward 4G")
    def get_tech_combo(row):
        p = []
        if row.get("2G cnt",0)>0: p.append("2G")
        if row.get("3G cnt",0)>0: p.append("3G")
        if row.get("Has_4G_Physical",False): p.append("4G")
        return "+".join(p) if p else "None"
    df_all["Tech_Combo"] = df_all.apply(get_tech_combo, axis=1)

    combo_m = df_all.groupby(["Month_Label","Tech_Combo"])["BTS IP ID"].nunique().reset_index()
    combo_m.columns = ["Month","Tech Combo","Sites"]
    st.plotly_chart(px.bar(combo_m, x="Month", y="Sites", color="Tech Combo",
        barmode="stack", text="Sites", title="Technology Combination per Site — Monthly"), use_container_width=True)

    tech_cnt = [c for c in ["2G cnt","3G cnt","4G cnt"] if c in df_all.columns]
    if tech_cnt:
        mt = df_all.groupby("Month_Label")[tech_cnt].mean().reset_index()
        mt["sk"] = mt["Month_Label"].apply(month_sort_key); mt = mt.sort_values("sk").drop(columns="sk")
        st.plotly_chart(px.area(mt.melt("Month_Label",var_name="Tech",value_name="Avg Cells"),
            x="Month_Label",y="Avg Cells",color="Tech",groupnorm="fraction",
            title="Avg Active Cell Count (normalized)").update_layout(yaxis_tickformat=".0%"),
            use_container_width=True)

    data_sh = [c for c in ["Data GB (2g)","Data GB (3g)","Data GB (2100)","Data GB (2500)","Data GB (700)"] if c in df_all.columns]
    if data_sh:
        md = df_all.groupby("Month_Label")[data_sh].sum().reset_index()
        md["sk"] = md["Month_Label"].apply(month_sort_key); md = md.sort_values("sk").drop(columns="sk")
        st.plotly_chart(px.area(md.melt("Month_Label",var_name="Band",value_name="Data GB"),
            x="Month_Label",y="Data GB",color="Band",groupnorm="fraction",
            title="Data Volume Share by Band (normalized)").update_layout(yaxis_tickformat=".0%"),
            use_container_width=True)

    st.subheader("📋 Detailed Technology Shift Table")
    shift_rows = []
    for month in months_sorted:
        dm = df_all[df_all["Month_Label"]==month]
        total = dm["BTS IP ID"].nunique()
        s2g  = int((dm["2G cnt"]>0).sum())  if "2G cnt" in dm.columns else 0
        s3g  = int((dm["3G cnt"]>0).sum())  if "3G cnt" in dm.columns else 0
        s4g  = int(dm[dm["Has_4G_Physical"]]["BTS IP ID"].nunique())
        s700 = int(dm["BTS Site ID (700)"].notna().sum())  if "BTS Site ID (700)"  in dm.columns else 0
        s2100= int(dm["BTS Site ID (2100)"].notna().sum()) if "BTS Site ID (2100)" in dm.columns else 0
        s2500= int(dm["BTS Site ID (2500)"].notna().sum()) if "BTS Site ID (2500)" in dm.columns else 0
        dg2g = dm["Data GB (2g)"].sum()  if "Data GB (2g)"  in dm.columns else 0
        dg3g = dm["Data GB (3g)"].sum()  if "Data GB (3g)"  in dm.columns else 0
        dg700= dm["Data GB (700)"].sum()  if "Data GB (700)"  in dm.columns else 0
        dg2100=dm["Data GB (2100)"].sum() if "Data GB (2100)" in dm.columns else 0
        dg2500=dm["Data GB (2500)"].sum() if "Data GB (2500)" in dm.columns else 0
        dg4g = dg700+dg2100+dg2500
        dgt  = dm["Data GB Total"].sum() if "Data GB Total" in dm.columns else (dg2g+dg3g+dg4g)
        fully= int(dm[(dm.get("2G cnt", pd.Series(0,index=dm.index))==0) & dm["Has_4G_Physical"]]["BTS IP ID"].nunique())
        shift_rows.append({
            "Month":month,"Total Sites":total,
            "2G Sites":s2g,"2G %":round(s2g/total*100,1) if total else 0,
            "3G Sites":s3g,"3G %":round(s3g/total*100,1) if total else 0,
            "4G Physical":s4g,"4G %":round(s4g/total*100,1) if total else 0,
            "700MHz":s700,"2100MHz":s2100,"2500MHz":s2500,
            "Fully 4G Only":fully,
            "Data 2G GB":round(dg2g,1),"Data 3G GB":round(dg3g,1),"Data 4G GB":round(dg4g,1),
            "Data Total GB":round(dgt,1),"4G Data %":round(dg4g/dgt*100,1) if dgt else 0,
        })
    st.dataframe(pd.DataFrame(shift_rows), use_container_width=True)

    # Combo pivot
    st.markdown("**Tech Combo Pivot per Month**")
    cp = df_all.groupby(["Month_Label","Tech_Combo"])["BTS IP ID"].nunique().unstack(fill_value=0).reset_index()
    cp.columns.name = None
    st.dataframe(cp, use_container_width=True)

    # Fully migrated sites
    fl = df_lat[(df_lat.get("2G cnt", pd.Series(0,index=df_lat.index))==0) & df_lat["Has_4G_Physical"]]
    st.markdown(f"**Fully migrated (4G only, no 2G) in {latest_month}: {fl['BTS IP ID'].nunique()} sites**")
    if len(fl):
        sc = ["BTS IP ID","BTS Name","SDCA","Site Type","Tech_Combo"]
        if has_incharge:     sc.append("incharge")
        if has_jto_incharge: sc.append("JTO INCHARGE")
        st.dataframe(fl[[c for c in sc if c in fl.columns]].drop_duplicates("BTS IP ID").reset_index(drop=True),
                     use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 8 – INCHARGE ANALYSIS (NEW)
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[7]:
    st.header("👷 Incharge Analysis")

    if not has_incharge and not has_jto_incharge:
        st.warning("Incharge data not available. Please upload the reference file (BTSIPID_PKEY1_excel.xlsx) "
                   "before uploading monthly performance files.")
        st.stop()

    avail_cols_ic = [c for c in ["Nw Avail (2G)","Nw Avail (3G)","Nw Avail (4G TCS)"] if c in df_lat.columns]
    traf_cols_ic  = [c for c in ["Erl (2g)","Erl (3g)","Erl Total","Data GB Total"] if c in df_lat.columns]

    def incharge_summary(df, col, label):
        """Build a comprehensive summary table grouped by an incharge column."""
        grp = df.groupby(col)
        base = grp["BTS IP ID"].nunique().rename("Sites").reset_index()
        base["% Network"] = (base["Sites"] / base["Sites"].sum() * 100).round(1)

        # Site Type breakdown
        if "Site Type" in df.columns:
            for stype in sorted(df["Site Type"].dropna().unique()):
                sub = df[df["Site Type"]==stype].groupby(col)["BTS IP ID"].nunique().rename(f"ST:{stype}")
                base = base.merge(sub.reset_index(), on=col, how="left")

        # Tech counts
        if "2G cnt" in df.columns:
            tmp = df[df["2G cnt"]>0].groupby(col)["BTS IP ID"].nunique().rename("2G Sites")
            base = base.merge(tmp.reset_index(), on=col, how="left")
        if "3G cnt" in df.columns:
            tmp = df[df["3G cnt"]>0].groupby(col)["BTS IP ID"].nunique().rename("3G Sites")
            base = base.merge(tmp.reset_index(), on=col, how="left")
        tmp4g = df[df["Has_4G_Physical"]].groupby(col)["BTS IP ID"].nunique().rename("4G Physical")
        base = base.merge(tmp4g.reset_index(), on=col, how="left")

        # Availability
        if avail_cols_ic:
            av = grp[avail_cols_ic].mean().round(2)
            av.columns = [c.replace("Nw Avail ","Avail ") for c in avail_cols_ic]
            base = base.merge(av.reset_index(), on=col, how="left")

        # Traffic
        if traf_cols_ic:
            tr = grp[traf_cols_ic].sum().round(1)
            base = base.merge(tr.reset_index(), on=col, how="left")

        return base.fillna(0).sort_values("Sites", ascending=False)

    for incharge_col, label in [("incharge","Incharge"),("JTO INCHARGE","JTO Incharge")]:
        if incharge_col not in df_lat.columns:
            continue

        st.subheader(f"📋 {label} — Summary ({latest_month})")
        summary_df = incharge_summary(df_lat, incharge_col, label)
        # Add Totals row
        num_ic = [c for c in summary_df.columns if c not in [incharge_col, label]]
        tot_row = summary_df[num_ic].sum(numeric_only=True)
        tot_row[incharge_col] = "TOTAL"
        disp = pd.concat([summary_df, pd.DataFrame([tot_row])], ignore_index=True)
        st.dataframe(disp.reset_index(drop=True), use_container_width=True)

        col_ic1, col_ic2 = st.columns(2)
        with col_ic1:
            fig_ic_bar = px.bar(summary_df, x=incharge_col, y="Sites", color=incharge_col,
                                text="Sites", title=f"Site Count by {label}")
            fig_ic_bar.update_traces(textposition="outside")
            fig_ic_bar.update_layout(xaxis_tickangle=-40, showlegend=False)
            st.plotly_chart(fig_ic_bar, use_container_width=True)
        with col_ic2:
            fig_ic_pie = px.pie(summary_df, names=incharge_col, values="Sites", hole=0.4,
                                title=f"Site Distribution by {label}")
            fig_ic_pie.update_traces(textinfo="label+percent+value")
            st.plotly_chart(fig_ic_pie, use_container_width=True)

        # Availability bar by incharge
        if avail_cols_ic:
            av_cols_short = [c.replace("Nw Avail ","Avail ") for c in avail_cols_ic]
            av_cols_present = [c for c in av_cols_short if c in summary_df.columns]
            if av_cols_present:
                fig_ic_av = px.bar(
                    summary_df.melt(id_vars=incharge_col, value_vars=av_cols_present,
                                    var_name="Technology", value_name="Avg Avail %"),
                    x=incharge_col, y="Avg Avail %", color="Technology", barmode="group",
                    text="Avg Avail %", title=f"Avg Availability % by {label}")
                fig_ic_av.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
                fig_ic_av.update_layout(xaxis_tickangle=-40, yaxis_range=[80,102])
                st.plotly_chart(fig_ic_av, use_container_width=True)

        # ── Worst sites by incharge – Technology-wise ──────────────────────
        st.markdown(f"#### 📉 Worst Sites by {label} — Technology-wise  ·  {latest_month.upper()}")
        st.caption("Shows the worst-performing site per technology for each incharge officer. "
                   "Sites sorted by lowest availability within each incharge group.")

        base_cols = ["BTS IP ID","BTS Name","SDCA","Site Type"]
        ic_vals   = sorted(df_lat[incharge_col].dropna().unique())

        for tech_lbl, avail_col in [("2G","Nw Avail (2G)"),("3G","Nw Avail (3G)"),("4G TCS","Nw Avail (4G TCS)")]:
            if avail_col not in df_lat.columns:
                continue
            with st.expander(f"📡 {tech_lbl} — Worst Sites by {label}  ({latest_month.upper()})"):
                all_worst = []
                for ic_val in ic_vals:
                    sub = df_lat[(df_lat[incharge_col]==ic_val) & df_lat[avail_col].notna()]
                    if len(sub) == 0:
                        continue
                    # Exclude avail_col from avail_cols_ic to prevent duplicate columns
                    extra_avail = [c for c in avail_cols_ic if c != avail_col]
                    worst5 = sub.nsmallest(5, avail_col)[base_cols + [avail_col] + extra_avail].copy()
                    worst5.insert(0, incharge_col, ic_val)
                    all_worst.append(worst5)
                if all_worst:
                    worst_df = pd.concat(all_worst, ignore_index=True).reset_index(drop=True)
                    worst_df.index = range(1, len(worst_df) + 1)
                    # Colour-code: red < 90, orange 90-95, green >= 95
                    def colour_avail(val):
                        try:
                            v = float(val)
                            if v < 90:  return "background-color:#ffcccc"
                            if v < 95:  return "background-color:#fff3cd"
                            return "background-color:#d4edda"
                        except Exception:
                            return ""
                    style_cols = [avail_col] + [c for c in avail_cols_ic if c != avail_col]
                    st.dataframe(safe_style(worst_df.round(2), colour_avail, style_cols),
                                 use_container_width=True)

                    # Summary: avg & min per incharge for this tech
                    ic_tech_summ = (df_lat[df_lat[avail_col].notna()]
                                    .groupby(incharge_col)[avail_col]
                                    .agg(Sites="count", Avg=lambda x: round(x.mean(),2),
                                         Min=lambda x: round(x.min(),2),
                                         Below_95=lambda x: (x<95).sum())
                                    .reset_index()
                                    .sort_values("Avg"))
                    ic_tech_summ.columns = [incharge_col,"Sites",f"Avg {tech_lbl} %",f"Min {tech_lbl} %","Sites < 95%"]
                    st.markdown(f"**{tech_lbl} Summary per {label}**")
                    st.dataframe(ic_tech_summ.reset_index(drop=True), use_container_width=True)

        # SDCA × Incharge matrix
        if "SDCA" in df_lat.columns:
            st.markdown(f"**📋 SDCA × {label} Site Count Matrix**")
            sdca_ic = df_lat.groupby(["SDCA", incharge_col])["BTS IP ID"].nunique().unstack(fill_value=0)
            sdca_ic["Total"] = sdca_ic.sum(axis=1)
            sdca_ic.loc["TOTAL"] = sdca_ic.sum()
            st.dataframe(sdca_ic, use_container_width=True)

            fig_hm_ic = px.density_heatmap(
                df_lat, x=incharge_col, y="SDCA", z="Nw Avail (4G TCS)" if "Nw Avail (4G TCS)" in df_lat.columns else None,
                histfunc="avg", nbinsx=df_lat[incharge_col].nunique(),
                color_continuous_scale="RdYlGn",
                title=f"Avg 4G Availability % — SDCA × {label}")
            fig_hm_ic.update_layout(xaxis_tickangle=-40)
            st.plotly_chart(fig_hm_ic, use_container_width=True)

        # MoM Incharge trend (if multiple months)
        if len(months_sorted) >= 2 and incharge_col in df_all.columns:
            st.markdown(f"**📈 {label} — MoM Availability Trend**")
            ic_mom = df_all.groupby(["Month_Label", incharge_col])[avail_cols_ic].mean().round(2).reset_index()
            for ac in avail_cols_ic:
                tl = ac.replace("Nw Avail ","")
                fig_ic_trend = px.line(ic_mom, x="Month_Label", y=ac, color=incharge_col,
                                       markers=True, title=f"{tl} Availability Trend by {label}")
                fig_ic_trend.update_layout(xaxis_tickangle=-30)
                st.plotly_chart(fig_ic_trend, use_container_width=True)

        st.markdown("---")

    # ── Site-level detail with both incharge columns ────────────────────────
    if has_incharge or has_jto_incharge:
        st.subheader("📋 Site-level Detail with Incharge Info")
        show_site_cols = ["BTS IP ID","BTS Name","SDCA","Site Type"]
        if has_location:     show_site_cols.append("LOCATION")
        if has_incharge:     show_site_cols.append("incharge")
        if has_jto_incharge: show_site_cols.append("JTO INCHARGE")
        show_site_cols += avail_cols_ic
        if "Erl Total" in df_lat.columns:     show_site_cols.append("Erl Total")
        if "Data GB Total" in df_lat.columns: show_site_cols.append("Data GB Total")

        # Filter by incharge
        ic_filter_col = "incharge" if has_incharge else "JTO INCHARGE"
        ic_filter_vals = sorted(df_lat[ic_filter_col].dropna().unique())
        sel_ic = st.multiselect(f"Filter by {ic_filter_col}", ic_filter_vals, default=ic_filter_vals[:3] if len(ic_filter_vals)>3 else ic_filter_vals)
        df_ic_site = df_lat if not sel_ic else df_lat[df_lat[ic_filter_col].isin(sel_ic)]
        st.dataframe(df_ic_site[[c for c in show_site_cols if c in df_ic_site.columns]].round(2).reset_index(drop=True),
                     use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════
    # CONSISTENT POOR PERFORMERS
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.subheader(f"🔴 Consistent Poor Performers — All {len(months_sorted)} Month(s): {', '.join(m.upper() for m in months_sorted)}")
    st.caption("Sites that remained below the availability threshold in EVERY uploaded month. "
               "Red = critical (<90%), Orange = poor (90–95%), shown for 2G / 3G / 4G TCS.")

    poor_threshold = st.slider("Availability threshold (%)", min_value=80, max_value=99, value=95, step=1)

    for tech_lbl, avail_col in [("2G","Nw Avail (2G)"),("3G","Nw Avail (3G)"),("4G TCS","Nw Avail (4G TCS)")]:
        if avail_col not in df_all.columns:
            continue

        # Build set of sites below threshold in EVERY month
        poor_sets = []
        for m in months_sorted:
            dm = df_all[df_all["Month_Label"] == m]
            poor_ids = set(dm[dm[avail_col] < poor_threshold]["BTS IP ID"].dropna().astype(str))
            poor_sets.append(poor_ids)

        if not poor_sets:
            continue
        consistent_ids = poor_sets[0]
        for ps in poor_sets[1:]:
            consistent_ids = consistent_ids & ps

        with st.expander(f"📡 {tech_lbl} — {len(consistent_ids)} Consistent Poor Sites  (< {poor_threshold}% every month)"):
            if not consistent_ids:
                st.success(f"No sites below {poor_threshold}% in every month. ✅")
                continue

            # Build detail table: one row per site with availability for each month
            rows = []
            for site_id in sorted(consistent_ids):
                # Get metadata from latest month
                meta_row = df_all[(df_all["BTS IP ID"].astype(str)==site_id) &
                                  (df_all["Month_Label"]==latest_month)]
                if len(meta_row) == 0:
                    meta_row = df_all[df_all["BTS IP ID"].astype(str)==site_id].iloc[:1]
                if len(meta_row) == 0:
                    continue
                mr = meta_row.iloc[0]
                row = {
                    "BTS IP ID":  site_id,
                    "BTS Name":   mr.get("BTS Name",""),
                    "SDCA":       mr.get("SDCA",""),
                    "Site Type":  mr.get("Site Type",""),
                }
                if has_incharge:     row["incharge"]      = mr.get("incharge","")
                if has_jto_incharge: row["JTO INCHARGE"]  = mr.get("JTO INCHARGE","")
                # Availability per month
                for m in months_sorted:
                    dm = df_all[(df_all["BTS IP ID"].astype(str)==site_id) & (df_all["Month_Label"]==m)]
                    row[m.upper()] = round(dm[avail_col].mean(), 2) if len(dm) else None
                # Avg across months
                month_vals = [row[m.upper()] for m in months_sorted if row.get(m.upper()) is not None]
                row["Avg All Months"] = round(float(np.mean(month_vals)), 2) if month_vals else None
                row["Worst Month"]    = months_sorted[int(np.argmin(month_vals))].upper() if month_vals else None
                rows.append(row)

            poor_df = pd.DataFrame(rows).sort_values("Avg All Months").reset_index(drop=True)

            def colour_poor(val):
                try:
                    v = float(val)
                    if v < 90:  return "background-color:#ffcccc; color:#900"
                    if v < 95:  return "background-color:#fff3cd; color:#664"
                    return ""
                except Exception:
                    return ""

            month_cols = [m.upper() for m in months_sorted]
            style_subset = [c for c in month_cols + ["Avg All Months"] if c in poor_df.columns]
            st.dataframe(safe_style(poor_df.round(2), colour_poor, style_subset),
                         use_container_width=True)

            # Summary stats
            c1p, c2p, c3p = st.columns(3)
            c1p.metric(f"Total Consistent Poor ({tech_lbl})", len(poor_df))
            c1p.caption(f"Below {poor_threshold}% every month")
            if "Avg All Months" in poor_df.columns and poor_df["Avg All Months"].notna().any():
                c2p.metric("Worst Avg Availability", f"{poor_df['Avg All Months'].min():.2f}%")
                c3p.metric("Best of Worst Avg",      f"{poor_df['Avg All Months'].max():.2f}%")

            # Chart: poor sites sorted by avg
            if len(poor_df) > 0:
                fig_poor = px.bar(poor_df.dropna(subset=["Avg All Months"]).nsmallest(25,"Avg All Months"),
                                  x="Avg All Months", y="BTS Name", orientation="h",
                                  color="Avg All Months", color_continuous_scale="RdYlGn",
                                  range_color=[80, poor_threshold],
                                  hover_data=["SDCA","Site Type"] +
                                             (["incharge"] if has_incharge else []) +
                                             (["JTO INCHARGE"] if has_jto_incharge else []),
                                  title=f"Top 25 Worst Consistent Poor Sites — {tech_lbl}")
                fig_poor.update_layout(yaxis={"categoryorder":"total ascending"}, height=600)
                st.plotly_chart(fig_poor, use_container_width=True)

    # ── Consistent Poor by Incharge ──────────────────────────────────────
    st.markdown("---")
    st.subheader(f"👷 Consistent Poor Performers — Incharge-wise  ({', '.join(m.upper() for m in months_sorted)})")
    st.caption("For each incharge officer: how many sites were consistently below threshold across all months.")

    if has_incharge or has_jto_incharge:
        for tech_lbl, avail_col in [("2G","Nw Avail (2G)"),("3G","Nw Avail (3G)"),("4G TCS","Nw Avail (4G TCS)")]:
            if avail_col not in df_all.columns:
                continue
            # Re-compute consistent IDs at current slider value
            poor_sets2 = []
            for m in months_sorted:
                dm = df_all[df_all["Month_Label"]==m]
                poor_sets2.append(set(dm[dm[avail_col] < poor_threshold]["BTS IP ID"].dropna().astype(str)))
            if not poor_sets2: continue
            consistent_ids2 = poor_sets2[0]
            for ps in poor_sets2[1:]: consistent_ids2 = consistent_ids2 & ps
            if not consistent_ids2: continue

            # Get metadata for consistent poor sites
            poor_meta = df_all[(df_all["BTS IP ID"].astype(str).isin(consistent_ids2)) &
                               (df_all["Month_Label"]==latest_month)].drop_duplicates("BTS IP ID")

            with st.expander(f"📡 {tech_lbl} — Consistent Poor by Incharge"):
                for ic_col, ic_lbl in ([("incharge","Incharge")] if has_incharge else []) + \
                                      ([("JTO INCHARGE","JTO Incharge")] if has_jto_incharge else []):
                    if ic_col not in poor_meta.columns: continue
                    ic_poor = (poor_meta.groupby(ic_col)["BTS IP ID"]
                               .nunique().reset_index()
                               .rename(columns={"BTS IP ID": f"Consistent Poor Sites ({tech_lbl})"}))
                    # total sites per incharge
                    ic_total = df_lat[df_lat[ic_col].notna()].groupby(ic_col)["BTS IP ID"].nunique().reset_index()
                    ic_total.columns = [ic_col, "Total Sites"]
                    ic_poor = ic_poor.merge(ic_total, on=ic_col, how="left")
                    ic_poor["Poor %"] = (ic_poor[f"Consistent Poor Sites ({tech_lbl})"] / ic_poor["Total Sites"] * 100).round(1)
                    # Avg availability for those poor sites
                    if avail_col in df_lat.columns:
                        poor_avail = (df_lat[(df_lat["BTS IP ID"].astype(str).isin(consistent_ids2)) &
                                            df_lat[ic_col].notna()]
                                     .groupby(ic_col)[avail_col].mean().round(2)
                                     .rename(f"Avg {tech_lbl} % (poor sites)"))
                        ic_poor = ic_poor.merge(poor_avail.reset_index(), on=ic_col, how="left")
                    ic_poor = ic_poor.sort_values(f"Consistent Poor Sites ({tech_lbl})", ascending=False)
                    ic_poor.loc[len(ic_poor)] = {ic_col: "TOTAL",
                        f"Consistent Poor Sites ({tech_lbl})": ic_poor[f"Consistent Poor Sites ({tech_lbl})"].sum(),
                        "Total Sites": ic_poor["Total Sites"].sum(),
                        "Poor %": round(ic_poor[f"Consistent Poor Sites ({tech_lbl})"].sum() / ic_poor["Total Sites"].sum() * 100, 1)}
                    st.markdown(f"**{ic_lbl}**")
                    st.dataframe(ic_poor.fillna("—").reset_index(drop=True), use_container_width=True)

                    if len(ic_poor) > 1:
                        fig_ic_poor = px.bar(
                            ic_poor[ic_poor[ic_col]!="TOTAL"].sort_values(f"Consistent Poor Sites ({tech_lbl})", ascending=False),
                            x=ic_col, y=f"Consistent Poor Sites ({tech_lbl})",
                            color="Poor %", color_continuous_scale="Reds",
                            text=f"Consistent Poor Sites ({tech_lbl})",
                            title=f"{tech_lbl} Consistent Poor Sites by {ic_lbl}")
                        fig_ic_poor.update_traces(textposition="outside")
                        fig_ic_poor.update_layout(xaxis_tickangle=-40)
                        st.plotly_chart(fig_ic_poor, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 9 – Revenue Report
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[8]:
    st.header("💰 Revenue Report")

    if not has_revenue:
        st.info("👆 Upload RBC Revenue file(s) from the sidebar (③ Upload Revenue Files) to enable this tab.")
        st.stop()

    rev_m_sel = st.selectbox("Select Revenue Month", rev_months_sorted,
                              index=len(rev_months_sorted)-1,
                              format_func=lambda x: x.upper())
    rdf_sel = rev_store[rev_m_sel].copy()

    # ── KPI row ────────────────────────────────────────────────────────────
    tot_rev   = rdf_sel["REV_LAKH"].sum()
    tot_sites = rdf_sel["BTSIPID"].nunique()
    avg_rev   = rdf_sel["REV_LAKH"].mean()
    max_rev   = rdf_sel["REV_LAKH"].max()
    zero_rev  = int((rdf_sel["REV_LAKH"] == 0).sum())
    k1,k2,k3,k4,k5 = st.columns(5)
    k1.metric("Total Revenue (Lakhs)", f"₹ {tot_rev:.2f}")
    k2.metric("Sites",                 tot_sites)
    k3.metric("Avg Rev/Site (Lakhs)",  f"₹ {avg_rev:.3f}")
    k4.metric("Max Site Rev (Lakhs)",  f"₹ {max_rev:.3f}")
    k5.metric("Zero Revenue Sites",    zero_rev, delta_color="inverse")

    st.markdown("---")

    # ── Revenue by SDCA ────────────────────────────────────────────────────
    st.subheader(f"📍 Revenue by SDCA  ·  {rev_m_sel.upper()}")
    sdca_rev = rdf_sel.groupby("SDCA").agg(
        Sites=("BTSIPID","nunique"),
        Total_Rev_Lakh=("REV_LAKH","sum"),
        Avg_Rev_Lakh=("REV_LAKH","mean"),
        Max_Rev_Lakh=("REV_LAKH","max"),
        Zero_Sites=("REV_LAKH", lambda x: (x==0).sum()),
        Traffic_Rev=("TRAFFIC_REV","sum"),
        Data_Rev=("DATA_REV","sum"),
    ).round(3).reset_index().sort_values("Total_Rev_Lakh", ascending=False)

    col_r1, col_r2 = st.columns([1.4, 1])
    with col_r1:
        fig_srev = px.bar(sdca_rev, x="SDCA", y="Total_Rev_Lakh", color="Total_Rev_Lakh",
                          color_continuous_scale="Greens", text="Total_Rev_Lakh",
                          title=f"Total Revenue (Lakhs) by SDCA — {rev_m_sel.upper()}")
        fig_srev.update_traces(texttemplate="₹%{text:.2f}L", textposition="outside")
        fig_srev.update_layout(xaxis_tickangle=-30, coloraxis_showscale=False)
        st.plotly_chart(fig_srev, use_container_width=True)
    with col_r2:
        st.markdown("**SDCA Revenue Table**")
        st.dataframe(sdca_rev.reset_index(drop=True), use_container_width=True, hide_index=True)

    # Traffic vs Data revenue split by SDCA
    sdca_rev_melt = sdca_rev.melt("SDCA", ["Traffic_Rev","Data_Rev"], var_name="Type", value_name="Rev")
    sdca_rev_melt["Type"] = sdca_rev_melt["Type"].map({"Traffic_Rev":"Traffic","Data_Rev":"Data"})
    st.plotly_chart(px.bar(sdca_rev_melt, x="SDCA", y="Rev", color="Type", barmode="stack",
                           title="Traffic vs Data Revenue by SDCA",
                           color_discrete_map={"Traffic":"#636EFA","Data":"#EF553B"}),
                    use_container_width=True)

    st.markdown("---")

    # ── Revenue by Tech Category ───────────────────────────────────────────
    st.subheader("📊 Revenue by Traffic Category")
    cat_tabs = st.tabs(["2G Category", "3G Category", "4G Category"])
    for cti, (cat_col, tech) in enumerate([("2G_Cat","2G"),("3G_Cat","3G"),("4G_Cat","4G")]):
        with cat_tabs[cti]:
            if cat_col not in rdf_sel.columns: continue
            cat_grp = rdf_sel.groupby(cat_col).agg(
                Sites=("BTSIPID","nunique"),
                Total_Rev=("REV_LAKH","sum"),
                Avg_Rev=("REV_LAKH","mean"),
            ).reindex([c for c in CAT_ORDER if c in rdf_sel[cat_col].dropna().unique()]).round(3).reset_index()
            cat_grp.columns = [cat_col,"Sites","Total Rev (Lakhs)","Avg Rev/Site (Lakhs)"]
            col_c1, col_c2 = st.columns(2)
            with col_c1:
                st.plotly_chart(px.bar(cat_grp, x=cat_col, y="Total Rev (Lakhs)",
                    color=cat_col, text="Total Rev (Lakhs)",
                    title=f"{tech} Category vs Total Revenue",
                    color_discrete_sequence=["#1a9641","#a6d96a","#ffffbf","#fdae61","#d7191c"]),
                    use_container_width=True)
            with col_c2:
                st.dataframe(cat_grp, use_container_width=True, hide_index=True)

    st.markdown("---")

    # ── Technology-wise Revenue Distribution ──────────────────────────────
    st.subheader("📡 Technology-wise Revenue Distribution")
    st.caption("Revenue broken down by vendor technology (2G/3G/4G) and per-technology contribution.")

    tech_rev_cols = {"2G": "2g_rev", "3G": "3g_rev", "4G": "4g_rev"}
    tech_vendor_cols = {"2G": "2G TECH", "3G": "3G TECH", "4G": "4G TECH"}

    # Overall 2G / 3G / 4G split
    tech_totals = {}
    for tech, col in tech_rev_cols.items():
        if col in rdf_sel.columns:
            tech_totals[tech] = rdf_sel[col].sum()
    if tech_totals:
        tot_split = pd.DataFrame({"Technology": list(tech_totals.keys()),
                                  "Revenue (₹)": list(tech_totals.values())})
        tot_split["Revenue (Lakhs)"] = (tot_split["Revenue (₹)"] / 100000).round(3)
        tot_split["Share (%)"] = (tot_split["Revenue (₹)"] / tot_split["Revenue (₹)"].sum() * 100).round(1)

        ts1, ts2 = st.columns(2)
        with ts1:
            st.plotly_chart(px.pie(tot_split, names="Technology", values="Revenue (Lakhs)",
                                   hole=0.45, title=f"2G / 3G / 4G Revenue Share — {rev_m_sel.upper()}",
                                   color_discrete_map={"2G":"#636EFA","3G":"#EF553B","4G":"#00CC96"}),
                            use_container_width=True)
        with ts2:
            st.markdown("**Technology Revenue Summary**")
            st.dataframe(tot_split[["Technology","Revenue (Lakhs)","Share (%)"]],
                         use_container_width=True, hide_index=True)

    # Per-technology vendor breakdown
    tech_ven_tabs = st.tabs(["2G Vendor", "3G Vendor", "4G Vendor"])
    for tvi, (tech, rev_col, ven_col) in enumerate([
            ("2G","2g_rev","2G TECH"),("3G","3g_rev","3G TECH"),("4G","4g_rev","4G TECH")]):
        with tech_ven_tabs[tvi]:
            if rev_col not in rdf_sel.columns or ven_col not in rdf_sel.columns:
                st.info(f"No {tech} vendor data available."); continue
            ven_grp = rdf_sel[rdf_sel[ven_col].notna()].groupby(ven_col).agg(
                Sites=("BTSIPID","nunique"),
                Tech_Rev=(rev_col,"sum"),
                Total_Rev=("REV_LAKH","sum"),
                Avg_Rev=("REV_LAKH","mean"),
            ).round(3).reset_index().sort_values("Tech_Rev", ascending=False)
            ven_grp["Tech_Rev_Lakh"] = (ven_grp["Tech_Rev"] / 100000).round(3)

            col_v1, col_v2 = st.columns([1.4, 1])
            with col_v1:
                st.plotly_chart(px.bar(ven_grp, x=ven_col, y="Tech_Rev_Lakh",
                    color=ven_col, text="Tech_Rev_Lakh",
                    title=f"{tech} Revenue by Vendor Technology (Lakhs)",
                    color_discrete_sequence=px.colors.qualitative.Set2),
                    use_container_width=True)
            with col_v2:
                st.dataframe(ven_grp[[ven_col,"Sites","Tech_Rev_Lakh","Total_Rev","Avg_Rev"]],
                             use_container_width=True, hide_index=True)

            # Per-SDCA breakdown for this tech
            sdca_ven = rdf_sel[rdf_sel[ven_col].notna()].groupby(["SDCA",ven_col])[rev_col].sum().reset_index()
            sdca_ven["Rev_Lakh"] = (sdca_ven[rev_col] / 100000).round(3)
            st.plotly_chart(px.bar(sdca_ven, x="SDCA", y="Rev_Lakh", color=ven_col,
                barmode="stack", title=f"{tech} Revenue by SDCA & Vendor (Lakhs)",
                color_discrete_sequence=px.colors.qualitative.Pastel),
                use_container_width=True)

    st.markdown("---")

    # ── 4G Band-wise Revenue Analysis ────────────────────────────────────
    st.subheader("📶 4G Band-wise Revenue Analysis")
    st.caption("Band categories from performance data: **A** = 700 only · **B** = 700+2100 · **D** = 700+2100+2500. "
               "Revenue joined from RBC file. Band traffic from monthly performance data.")

    # Need to join perf data for band info
    perf_data_avail = df_lat_rev is not None and "Erl (700)" in df_lat_rev.columns
    if perf_data_avail:
        band_df = df_lat_rev.copy()
        # Build active band combo from Erl columns
        for bc in ["Erl (700)","Erl (2100)","Erl (2500)"]:
            band_df[bc] = pd.to_numeric(band_df[bc], errors="coerce").fillna(0)

        def _band_combo(row):
            b = []
            if row.get("Erl (700)",0) > 0:  b.append("700")
            if row.get("Erl (2100)",0) > 0: b.append("2100")
            if row.get("Erl (2500)",0) > 0: b.append("2500")
            return "+".join(b) if b else "None/IP only"
        band_df["Band_Combo"] = band_df.apply(_band_combo, axis=1)

        # Band category from perf file
        if "Band category" in band_df.columns:
            band_cat_map = {"A":"700 only","B":"700+2100","D":"700+2100+2500","Null":"No 4G"}
            band_df["Band_Cat_Label"] = band_df["Band category"].map(band_cat_map).fillna(band_df["Band category"])
        else:
            band_df["Band_Cat_Label"] = band_df["Band_Combo"]

        # KPIs per band
        band_kpi = band_df.groupby("Band_Cat_Label").agg(
            Sites=("BTS IP ID","nunique"),
            Rev_Lakh=("REV_LAKH","sum"),
            Avg_Rev=("REV_LAKH","mean"),
            Erl_700=("Erl (700)","sum"),
            Erl_2100=("Erl (2100)","sum"),
            Erl_2500=("Erl (2500)","sum") if "Erl (2500)" in band_df.columns else ("Erl (700)","count"),
        ).round(3).reset_index().sort_values("Rev_Lakh", ascending=False)

        bk1, bk2 = st.columns(2)
        with bk1:
            st.plotly_chart(px.bar(band_kpi, x="Band_Cat_Label", y="Rev_Lakh",
                color="Band_Cat_Label", text="Rev_Lakh",
                title=f"Total Revenue (Lakhs) by 4G Band — {rev_m_sel.upper()}",
                color_discrete_map={"700 only":"#d62728","700+2100":"#ff7f0e",
                                    "700+2100+2500":"#2ca02c","No 4G":"#7f7f7f"}),
                use_container_width=True)
        with bk2:
            st.plotly_chart(px.bar(band_kpi, x="Band_Cat_Label", y="Avg_Rev",
                color="Band_Cat_Label", text="Avg_Rev",
                title="Avg Revenue/Site by 4G Band",
                color_discrete_map={"700 only":"#d62728","700+2100":"#ff7f0e",
                                    "700+2100+2500":"#2ca02c","No 4G":"#7f7f7f"}),
                use_container_width=True)

        st.dataframe(band_kpi.rename(columns={"Band_Cat_Label":"4G Band","Rev_Lakh":"Total Rev (Lakhs)",
                                               "Avg_Rev":"Avg Rev/Site (Lakhs)"}),
                     use_container_width=True, hide_index=True)

        # Traffic breakdown per band
        st.markdown("**Traffic & Data per 4G Band**")
        band_traffic = band_df.groupby("Band_Cat_Label").agg(
            Sites=("BTS IP ID","nunique"),
            Erl_700=("Erl (700)","sum"),
            Erl_2100=("Erl (2100)","sum"),
        ).reset_index()
        for bc_col in ["Erl (2500)","Data GB (700)","Data GB (2100)","Data GB (2500)"]:
            if bc_col in band_df.columns:
                band_df[bc_col] = pd.to_numeric(band_df[bc_col], errors="coerce").fillna(0)
                band_traffic = band_traffic.merge(
                    band_df.groupby("Band_Cat_Label")[bc_col].sum().reset_index(),
                    on="Band_Cat_Label", how="left")
        st.dataframe(band_traffic.round(1), use_container_width=True, hide_index=True)

        # Per-band traffic vs revenue scatter
        st.markdown("**Band Revenue vs 4G Traffic**")
        for bc_erl, bc_label in [("Erl (700)","700 MHz Erl"),("Erl (2100)","2100 MHz Erl"),("Erl (2500)","2500 MHz Erl")]:
            if bc_erl not in band_df.columns: continue
            sc_b = band_df[[bc_erl,"REV_LAKH","BTS Name","SDCA","Band_Cat_Label"]].dropna(subset=[bc_erl,"REV_LAKH"])
            sc_b = sc_b[sc_b[bc_erl] > 0]
            if len(sc_b) < 5: continue
            with st.expander(f"📊 {bc_label} vs Revenue"):
                st.plotly_chart(px.scatter(sc_b, x=bc_erl, y="REV_LAKH",
                    color="Band_Cat_Label", hover_name="BTS Name",
                    trendline="ols", title=f"{bc_label} vs Revenue (Lakhs)",
                    color_discrete_map={"700 only":"#d62728","700+2100":"#ff7f0e",
                                        "700+2100+2500":"#2ca02c"}),
                    use_container_width=True)

        # Per-SDCA band composition
        st.markdown("**Band Distribution by SDCA**")
        sdca_band = band_df.groupby(["SDCA","Band_Cat_Label"])["BTS IP ID"].nunique().reset_index()
        sdca_band.columns = ["SDCA","4G Band","Sites"]
        st.plotly_chart(px.bar(sdca_band, x="SDCA", y="Sites", color="4G Band", barmode="stack",
            title="4G Band Composition by SDCA",
            color_discrete_map={"700 only":"#d62728","700+2100":"#ff7f0e",
                                "700+2100+2500":"#2ca02c","No 4G":"#7f7f7f"}),
            use_container_width=True)
    else:
        st.info("Upload monthly performance data (Jan/Dec CSV/XLSX) along with the RBC file to see 4G band-wise analysis.")

    st.markdown("---")

    # ── Site-level Revenue Table ───────────────────────────────────────────
    st.subheader(f"📋 Site-level Revenue Detail  ·  {rev_m_sel.upper()}")
    # Merge with ref for incharge
    ref_df_state = st.session_state.ref_df
    if ref_df_state is not None:
        rdf_detail = rdf_sel.merge(
            ref_df_state[["BTSIPID","incharge","JTO INCHARGE"]],
            on="BTSIPID", how="left"
        )
    else:
        rdf_detail = rdf_sel.copy()

    show_site_cols = ["BTSIPID","SITENAME","SDCA","LOCATION","REV_LAKH","TOT_REV",
                      "TRAFFIC_REV","DATA_REV","2g_rev","3g_rev","4g_rev",
                      "2G_Cat","3G_Cat","4G_Cat","TOT_TRAFFIC","TOT_DATA"]
    if "incharge" in rdf_detail.columns:     show_site_cols.append("incharge")
    if "JTO INCHARGE" in rdf_detail.columns: show_site_cols.append("JTO INCHARGE")
    show_site_cols = [c for c in show_site_cols if c in rdf_detail.columns]

    # Filter controls
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        sdca_filter = st.multiselect("Filter SDCA", sorted(rdf_detail["SDCA"].dropna().unique()),
                                     default=sorted(rdf_detail["SDCA"].dropna().unique()))
    with fc2:
        cat_filter_col = "4G_Cat" if "4G_Cat" in rdf_detail.columns else None
        if cat_filter_col:
            cat_filter = st.multiselect("Filter 4G Category",
                                        [c for c in CAT_ORDER if c in rdf_detail[cat_filter_col].dropna().unique()],
                                        default=[c for c in CAT_ORDER if c in rdf_detail[cat_filter_col].dropna().unique()])
        else: cat_filter = []
    with fc3:
        min_rev = st.number_input("Min Revenue (Lakhs)", value=0.0, step=0.01)

    rdf_filt = rdf_detail.copy()
    if sdca_filter: rdf_filt = rdf_filt[rdf_filt["SDCA"].isin(sdca_filter)]
    if cat_filter and cat_filter_col: rdf_filt = rdf_filt[rdf_filt[cat_filter_col].isin(cat_filter)]
    rdf_filt = rdf_filt[rdf_filt["REV_LAKH"] >= min_rev]

    st.caption(f"Showing {len(rdf_filt)} sites")
    st.dataframe(
        safe_style(rdf_filt[show_site_cols].sort_values("REV_LAKH", ascending=False).reset_index(drop=True).round(3),
                   lambda v: ("background-color:#d4edda" if isinstance(v,float) and v>=1.0
                              else "background-color:#fff3cd" if isinstance(v,float) and 0<v<0.5
                              else "background-color:#ffcccc" if isinstance(v,float) and v==0.0
                              else ""),
                   ["REV_LAKH"]),
        use_container_width=True)

    st.markdown("---")

    # ── Revenue vs Availability Correlation ───────────────────────────────
    st.subheader("🔗 Revenue vs Network Performance Correlation")
    if df_lat_rev is not None and "REV_LAKH" in df_lat_rev.columns:
        perf_cols = [c for c in ["Nw Avail (2G)","Nw Avail (3G)","Nw Avail (4G TCS)",
                                  "Erl Total","Data GB Total"] if c in df_lat_rev.columns]
        rev_corr_cols = ["REV_LAKH","TOT_REV","TRAFFIC_REV","DATA_REV"]
        rev_corr_cols = [c for c in rev_corr_cols if c in df_lat_rev.columns]

        if perf_cols and rev_corr_cols:
            corr_rows = []
            for pc in perf_cols:
                for rc in rev_corr_cols:
                    sub = df_lat_rev[[pc,rc]].dropna()
                    if len(sub) > 5:
                        r = sub.corr().iloc[0,1]
                        corr_rows.append({"Performance Metric":pc,"Revenue Metric":rc,
                                          "Correlation (r)":round(r,3),
                                          "Strength":("Strong" if abs(r)>=0.5 else "Moderate" if abs(r)>=0.3 else "Weak"),
                                          "Direction":("Positive" if r>0 else "Negative")})
            if corr_rows:
                corr_df = pd.DataFrame(corr_rows)
                st.dataframe(corr_df, use_container_width=True, hide_index=True)

                # Scatter: availability vs revenue
                sc_col = st.selectbox("X-axis (Performance)", perf_cols)
                rc_col = st.selectbox("Y-axis (Revenue)", rev_corr_cols)
                scatter_df = df_lat_rev[[sc_col, rc_col, "BTS Name","SDCA","Site Type",
                                         "2G_Cat" if "2G_Cat" in df_lat_rev.columns else sc_col]].dropna()
                st.plotly_chart(
                    px.scatter(scatter_df, x=sc_col, y=rc_col,
                               color="SDCA" if "SDCA" in scatter_df.columns else None,
                               hover_name="BTS Name", trendline="ols",
                               title=f"{sc_col}  vs  {rc_col}  ({latest_month.upper()} vs {rev_m_sel.upper()})"),
                    use_container_width=True)

    st.markdown("---")

    # ── Incharge-wise Revenue ──────────────────────────────────────────────
    if ref_df_state is not None:
        st.subheader("👷 Revenue by Incharge")
        for ic_col, ic_lbl in [("incharge","Incharge"),("JTO INCHARGE","JTO Incharge")]:
            if ic_col not in rdf_detail.columns: continue
            ic_rev = rdf_detail.groupby(ic_col).agg(
                Sites=("BTSIPID","nunique"),
                Total_Rev_Lakh=("REV_LAKH","sum"),
                Avg_Rev_Lakh=("REV_LAKH","mean"),
                Zero_Sites=("REV_LAKH", lambda x: (x==0).sum()),
            ).round(3).reset_index().sort_values("Total_Rev_Lakh", ascending=False)
            ic_rev["Rev/Site"] = (ic_rev["Total_Rev_Lakh"]/ic_rev["Sites"]).round(3)
            col_ic1, col_ic2 = st.columns([1.6,1])
            with col_ic1:
                fig_icr = px.bar(ic_rev, x=ic_col, y="Total_Rev_Lakh", color="Total_Rev_Lakh",
                                 color_continuous_scale="Blues", text="Total_Rev_Lakh",
                                 title=f"Total Revenue by {ic_lbl} — {rev_m_sel.upper()}")
                fig_icr.update_traces(texttemplate="₹%{text:.2f}L", textposition="outside")
                fig_icr.update_layout(xaxis_tickangle=-40, coloraxis_showscale=False)
                st.plotly_chart(fig_icr, use_container_width=True)
            with col_ic2:
                st.markdown(f"**{ic_lbl} Revenue Summary**")
                st.dataframe(ic_rev.reset_index(drop=True), use_container_width=True, hide_index=True)

    st.markdown("---")

    # ── MoM Revenue Trend (if multiple months) ─────────────────────────────
    if len(rev_months_sorted) >= 2:
        st.subheader("📈 Revenue Trend — Month over Month")
        mom_rev = pd.DataFrame([
            {"Month": m.upper(),
             "Total Rev (Lakhs)": rev_store[m]["REV_LAKH"].sum().round(2),
             "Sites": rev_store[m]["BTSIPID"].nunique(),
             "Avg Rev/Site": rev_store[m]["REV_LAKH"].mean().round(3),
             "Zero Rev Sites": int((rev_store[m]["REV_LAKH"]==0).sum())}
            for m in rev_months_sorted
        ])
        col_t1, col_t2 = st.columns(2)
        with col_t1:
            st.plotly_chart(px.line(mom_rev, x="Month", y="Total Rev (Lakhs)",
                markers=True, text="Total Rev (Lakhs)", title="Total Revenue Trend (Lakhs)"),
                use_container_width=True)
        with col_t2:
            st.plotly_chart(px.line(mom_rev, x="Month", y="Avg Rev/Site",
                markers=True, title="Avg Revenue per Site Trend"),
                use_container_width=True)
        st.dataframe(mom_rev, use_container_width=True, hide_index=True)

    st.markdown("---")

    # ── Helper: render a good-avail / low-rev site table ──────────────────
    def _render_gal_table(gal_df, avail_thresh, rev_thresh, months_used, rev_month_lbl,
                          avail_col_map_inner, suffix=""):
        """Shared rendering for both single-month and multi-month good-avail reports."""
        gm1, gm2, gm3, gm4 = st.columns(4)
        gm1.metric("Sites Found", len(gal_df))
        gm2.metric("Zero Revenue Sites", int((gal_df["REV_LAKH"] == 0).sum()))
        gm3.metric("Avg Revenue (Lakhs)",
                   f"₹{gal_df['REV_LAKH'].mean():.3f}" if len(gal_df) else "—")
        gm4.metric("Total Revenue (Lakhs)",
                   f"₹{gal_df['REV_LAKH'].sum():.2f}" if len(gal_df) else "—")

        if len(gal_df) == 0:
            st.success(f"✅ No sites with availability ≥{avail_thresh}% AND revenue ≤₹{rev_thresh}L.")
            return

        sdca_col = "SDCA" if "SDCA" in gal_df.columns else None
        if sdca_col:
            gal_sdca = gal_df.groupby(sdca_col).agg(
                Sites=("BTS IP ID","nunique"),
                Avg_Rev=("REV_LAKH","mean"),
                Zero_Rev=("REV_LAKH", lambda x:(x==0).sum()),
            ).round(3).reset_index().sort_values("Sites", ascending=False)
            col_ga1, col_ga2 = st.columns([1.4, 1])
            with col_ga1:
                fig_ga = px.bar(gal_sdca, x=sdca_col, y="Sites",
                                color="Avg_Rev", color_continuous_scale="RdYlGn",
                                text="Sites",
                                title=f"Good Avail (≥{avail_thresh}%) & Low Rev (≤₹{rev_thresh}L) — by SDCA")
                fig_ga.update_traces(textposition="outside")
                fig_ga.update_layout(xaxis_tickangle=-30, coloraxis_showscale=False)
                st.plotly_chart(fig_ga, use_container_width=True)
            with col_ga2:
                st.dataframe(gal_sdca, use_container_width=True, hide_index=True)

        # 4G scatter
        if "Nw Avail (4G TCS)" in gal_df.columns and "REV_LAKH" in gal_df.columns:
            sc_cols = ["BTS Name","SDCA","Nw Avail (4G TCS)","REV_LAKH"]
            for _c in ["4G_Cat","Site Type","Perday_4G_GB","incharge"]:
                if _c in gal_df.columns: sc_cols.append(_c)
            sc_gal = gal_df[list(dict.fromkeys(sc_cols))].dropna(subset=["Nw Avail (4G TCS)","REV_LAKH"])
            st.plotly_chart(
                px.scatter(sc_gal, x="Nw Avail (4G TCS)", y="REV_LAKH",
                           color="4G_Cat" if "4G_Cat" in sc_gal.columns else "SDCA",
                           size="Perday_4G_GB" if "Perday_4G_GB" in sc_gal.columns else None,
                           size_max=25, hover_name="BTS Name",
                           title=f"4G Availability vs Revenue  ({' + '.join(m.upper() for m in months_used)} / {rev_month_lbl.upper()})",
                           color_discrete_map={"VHT":"#1a9641","HT":"#a6d96a",
                                               "MT":"#ffffbf","LT":"#fdae61","VLT":"#d7191c"}),
                use_container_width=True)

        # 4G category breakdown
        if "4G_Cat" in gal_df.columns:
            agg_dict = {"Sites": ("BTS IP ID","nunique"),
                        "Avg_Rev_Lakh": ("REV_LAKH","mean")}
            if "Nw Avail (4G TCS)" in gal_df.columns:
                agg_dict["Avg_4G_Avail"] = ("Nw Avail (4G TCS)","mean")
            if "Perday_4G_GB" in gal_df.columns:
                agg_dict["Avg_Perday_4G_GB"] = ("Perday_4G_GB","mean")
            cat_ga = gal_df.groupby("4G_Cat").agg(**agg_dict)\
                .reindex([c for c in CAT_ORDER if c in gal_df["4G_Cat"].dropna().unique()])\
                .round(2).reset_index()
            st.markdown("**4G Category breakdown**")
            st.dataframe(cat_ga, use_container_width=True, hide_index=True)

        # Full site table
        avail_display_cols = [c for c in avail_col_map_inner.values() if c in gal_df.columns]
        month_avail_cols   = [c for c in gal_df.columns if c.startswith("Avail_")]
        all_avail_cols     = avail_display_cols + month_avail_cols
        show_gal = [c for c in (["BTS IP ID","BTS Name","SDCA","Site Type","LOCATION"]
                    + month_avail_cols + avail_display_cols
                    + ["Avg_Avail_All_Months"] if "Avg_Avail_All_Months" in gal_df.columns else []
                    + ["REV_LAKH","4G_Cat","3G_Cat","2G_Cat",
                       "Avg_Erl_Total","Avg_Data_GB_Total",
                       "Perday_4G_GB","Perday_3G_GB","Perday_2G_Erl",
                       "incharge","JTO INCHARGE"]) if c in gal_df.columns]

        months_label = " + ".join(m.upper() for m in months_used)
        st.markdown(f"**📋 {len(gal_df)} Actionable Sites — Good Availability (≥{avail_thresh}%, "
                    f"{months_label} perf), Low Revenue (≤₹{rev_thresh}L, {rev_month_lbl.upper()} RBC)**")

        def _ga_colour(val):
            try:
                v = float(val)
                if v >= 95: return "background-color:#d4edda"
                if v >= 90: return "background-color:#fff3cd"
                if v <  90: return "background-color:#ffcccc"
            except Exception:
                pass
            return ""
        st.dataframe(safe_style(gal_df[show_gal].sort_values("REV_LAKH").reset_index(drop=True).round(2),
                                _ga_colour, [c for c in all_avail_cols if c in gal_df.columns]),
                     use_container_width=True)

    # ── Good Availability + Low Revenue — Single Month (with month selector) ─
    st.subheader("🟡 Good Availability but Low Revenue Sites")

    if df_lat_rev is None or "REV_LAKH" not in df_lat_rev.columns:
        st.info("Upload both performance files **and** RBC revenue file to enable this analysis.")
    else:
        AVAIL_COL_MAP_GA = {"4G TCS":"Nw Avail (4G TCS)","2G":"Nw Avail (2G)","3G":"Nw Avail (3G)"}

        # ── Controls row ──────────────────────────────────────────────────
        gc1, gc2, gc3, gc4 = st.columns(4)
        with gc1:
            avail_thresh_ga = st.slider("Min Availability % (Good)", 90, 100, 95, 1, key="ga_avail")
        with gc2:
            rev_thresh_ga = st.slider("Max Revenue (Lakhs)", 0.05, 2.0, 0.5, 0.05, key="ga_rev")
        with gc3:
            tech_ga = st.selectbox("Technology", ["4G TCS","2G","3G","All"], key="ga_tech")
        with gc4:
            # Month selector — user can pick which perf month to use for availability
            ga_month_opts = months_sorted   # all uploaded perf months
            ga_sel_months = st.multiselect(
                "Perf Month(s) for Availability",
                ga_month_opts,
                default=[latest_month],
                key="ga_sel_months",
                help="Select one month for single-point check, or multiple months to require good availability in ALL selected months."
            )

        if not ga_sel_months:
            st.warning("Select at least one performance month above.")
        else:
            # Build per-site availability for selected months only
            ga_site_avail = {}  # site_id → {month: avg_avail}
            for m in ga_sel_months:
                dm = df_all[df_all["Month_Label"] == m]
                for _, row in dm.iterrows():
                    sid = str(row.get("BTS IP ID",""))
                    if tech_ga == "All":
                        vals = [pd.to_numeric(row.get(ac, np.nan), errors="coerce")
                                for ac in AVAIL_COL_MAP_GA.values() if ac in dm.columns]
                        v = max([x for x in vals if not np.isnan(x)], default=np.nan)
                    else:
                        ac = AVAIL_COL_MAP_GA.get(tech_ga,"Nw Avail (4G TCS)")
                        v  = pd.to_numeric(row.get(ac, np.nan), errors="coerce")
                    ga_site_avail.setdefault(sid, {})[m] = v

            # Site must appear in ALL selected months and be ≥ threshold in every one
            good_ids_ga = {
                sid for sid, mv in ga_site_avail.items()
                if len(mv) == len(ga_sel_months)
                and all(not np.isnan(v) and v >= avail_thresh_ga for v in mv.values())
            }

            # Base from latest selected month's data joined with revenue
            latest_ga_month = sorted(ga_sel_months, key=month_sort_key)[-1]
            gal_base = df_all[df_all["Month_Label"] == latest_ga_month].copy()
            gal_base  = gal_base[gal_base["BTS IP ID"].astype(str).isin(good_ids_ga)]
            # Join revenue
            gal_base = gal_base.merge(
                rev_lat[["BTSIPID","REV_LAKH","Perday_4G_GB","Perday_3G_GB","Perday_2G_Erl",
                          "2G_Cat","3G_Cat","4G_Cat"]],
                left_on="BTS IP ID", right_on="BTSIPID", how="left", suffixes=("","_rbc")
            )
            # Add per-month avail columns for display
            for m in ga_sel_months:
                col_lbl = f"Avail_{m.upper()}"
                ac = AVAIL_COL_MAP_GA.get(tech_ga,"Nw Avail (4G TCS)") if tech_ga != "All" else "Nw Avail (4G TCS)"
                if ac in df_all.columns:
                    m_avail = df_all[df_all["Month_Label"]==m].groupby("BTS IP ID")[ac].mean()
                    gal_base[col_lbl] = gal_base["BTS IP ID"].map(m_avail)

            rev_mask_ga = pd.to_numeric(gal_base["REV_LAKH"], errors="coerce") <= rev_thresh_ga
            gal_df = gal_base[rev_mask_ga].copy()

            months_label_ga = " + ".join(m.upper() for m in ga_sel_months)
            st.caption(f"Availability: **{months_label_ga}** perf data (must be ≥{avail_thresh_ga}% in ALL selected months) · "
                       f"Revenue: **{latest_rev_month.upper()}** RBC")

            _render_gal_table(gal_df, avail_thresh_ga, rev_thresh_ga,
                              ga_sel_months, latest_rev_month, AVAIL_COL_MAP_GA, suffix="single")

    st.markdown("---")

    # ── Consistent Good Availability + Poor Revenue (multi-month) ─────────
    st.subheader("🌟 Consistently Good Availability & Poor Revenue — Multi-Month")
    st.caption("Sites that maintained good availability across ALL selected performance months yet remain low revenue. "
               "Traffic & Data shown as averages across those months.")

    if len(months_sorted) < 2:
        st.info("Upload at least **2 months** of performance data to use this report.")
    elif not has_revenue:
        st.info("Upload an RBC revenue file to enable this report.")
    else:
        AVAIL_COL_MAP_CGA = {"4G TCS":"Nw Avail (4G TCS)","2G":"Nw Avail (2G)","3G":"Nw Avail (3G)"}

        cg1, cg2, cg3, cg4 = st.columns(4)
        with cg1:
            cga_thresh = st.slider("Min Availability % (Consistent Good)",
                                   80, 100, 90, 1, key="cga_thresh")
        with cg2:
            cga_rev    = st.slider("Max Revenue (Lakhs) — Poor Rev",
                                   0.05, 2.0, 0.5, 0.05, key="cga_rev")
        with cg3:
            cga_tech   = st.selectbox("Technology", ["4G TCS","2G","3G","All"],
                                      key="cga_tech")
        with cg4:
            cga_months = st.multiselect(
                "Performance Months to Check",
                months_sorted,
                default=months_sorted[-min(3, len(months_sorted)):],
                key="cga_months",
                help="Site must be ≥ threshold in ALL selected months."
            )

        if len(cga_months) < 2:
            st.warning("Select at least 2 performance months.")
        else:
            # Compute per-site, per-month availability
            cga_av_col_map = {"4G TCS":"Nw Avail (4G TCS)","2G":"Nw Avail (2G)","3G":"Nw Avail (3G)"}

            # Average availability per site per month
            cga_site_month = {}  # site → {month → avg_avail}
            traffic_cols_cga = [c for c in ["Erl Total","Data GB Total","Erl (2g)","Erl (3g)","Erl (4g)",
                                             "Data GB (2g)","Data GB (3g)","Data GB (4g)"] if c in df_all.columns]
            site_traffic = {}   # site → {col → [monthly_avgs]}

            for m in cga_months:
                dm = df_all[df_all["Month_Label"] == m]
                for _, row in dm.iterrows():
                    sid = str(row.get("BTS IP ID",""))
                    # Availability
                    if cga_tech == "All":
                        vals = [pd.to_numeric(row.get(ac, np.nan), errors="coerce")
                                for ac in cga_av_col_map.values() if ac in dm.columns]
                        v = max([x for x in vals if not np.isnan(x)], default=np.nan)
                    else:
                        ac = cga_av_col_map.get(cga_tech,"Nw Avail (4G TCS)")
                        v  = pd.to_numeric(row.get(ac, np.nan), errors="coerce")
                    cga_site_month.setdefault(sid, {})[m] = v
                    # Traffic & Data
                    for tc in traffic_cols_cga:
                        tv = pd.to_numeric(row.get(tc, np.nan), errors="coerce")
                        if not np.isnan(tv):
                            site_traffic.setdefault(sid, {}).setdefault(tc, []).append(tv)

            # Sites consistently good in ALL selected months
            cga_good_ids = {
                sid for sid, mv in cga_site_month.items()
                if len(mv) == len(cga_months)
                and all(not np.isnan(v) and v >= cga_thresh for v in mv.values())
            }

            # Build result dataframe from latest selected month meta
            latest_cga_month = sorted(cga_months, key=month_sort_key)[-1]
            cga_meta = df_all[df_all["Month_Label"] == latest_cga_month].copy()
            cga_meta  = cga_meta[cga_meta["BTS IP ID"].astype(str).isin(cga_good_ids)].copy()

            # Add per-month availability columns
            for m in cga_months:
                col_lbl = f"Avail_{m.upper()}"
                if cga_tech != "All":
                    ac = cga_av_col_map.get(cga_tech,"Nw Avail (4G TCS)")
                else:
                    ac = "Nw Avail (4G TCS)"
                if ac in df_all.columns:
                    m_avail = df_all[df_all["Month_Label"]==m].groupby("BTS IP ID")[ac].mean()
                    cga_meta[col_lbl] = cga_meta["BTS IP ID"].map(m_avail)

            # Avg availability across all selected months
            avail_month_cols = [f"Avail_{m.upper()}" for m in cga_months if f"Avail_{m.upper()}" in cga_meta.columns]
            if avail_month_cols:
                cga_meta["Avg_Avail_All_Months"] = cga_meta[avail_month_cols].mean(axis=1).round(2)

            # Add avg traffic & data across months
            for tc in traffic_cols_cga:
                cga_meta[f"Avg_{tc.replace(' ','_')}"] = cga_meta["BTS IP ID"].astype(str).map(
                    {sid: round(np.mean(vals), 2) for sid, d in site_traffic.items()
                     for col, vals in d.items() if col == tc}
                )
            # Rename for clarity
            rename_traf = {"Avg_Erl_Total":"Avg_Erl_Total", "Avg_Data_GB_Total":"Avg_Data_GB_Total"}

            # Join revenue
            cga_meta = cga_meta.merge(
                rev_lat[["BTSIPID","REV_LAKH","Perday_4G_GB","Perday_3G_GB","Perday_2G_Erl",
                          "2G_Cat","3G_Cat","4G_Cat"]],
                left_on="BTS IP ID", right_on="BTSIPID", how="left", suffixes=("","_rbc")
            )

            # Apply revenue filter
            cga_meta["REV_LAKH"] = pd.to_numeric(cga_meta["REV_LAKH"], errors="coerce")
            cga_df = cga_meta[cga_meta["REV_LAKH"] <= cga_rev].copy()

            cga_months_lbl = " + ".join(m.upper() for m in cga_months)
            st.caption(f"**{len(cga_good_ids)}** sites consistently ≥{cga_thresh}% in "
                       f"**{cga_months_lbl}** → **{len(cga_df)}** also have revenue ≤₹{cga_rev}L")

            # Summary KPIs
            ck1, ck2, ck3, ck4, ck5 = st.columns(5)
            ck1.metric("Consistently Good Sites", len(cga_good_ids))
            ck2.metric("Good + Poor Revenue",     len(cga_df))
            ck3.metric("Zero Revenue",             int((cga_df["REV_LAKH"]==0).sum()) if len(cga_df) else 0)
            if len(cga_df):
                ck4.metric("Avg Revenue (Lakhs)",  f"₹{cga_df['REV_LAKH'].mean():.3f}")
                if "Avg_Avail_All_Months" in cga_df.columns:
                    ck5.metric(f"Avg Avail ({cga_tech})", f"{cga_df['Avg_Avail_All_Months'].mean():.1f}%")

            if len(cga_df) > 0:
                # SDCA bar
                if "SDCA" in cga_df.columns:
                    cga_sdca = cga_df.groupby("SDCA").agg(
                        Sites=("BTS IP ID","nunique"),
                        Avg_Rev=("REV_LAKH","mean"),
                        **({} if "Avg_Avail_All_Months" not in cga_df.columns else
                           {"Avg_Avail": ("Avg_Avail_All_Months","mean")}),
                    ).round(3).reset_index().sort_values("Sites", ascending=False)
                    col_cg1, col_cg2 = st.columns([1.4,1])
                    with col_cg1:
                        st.plotly_chart(px.bar(cga_sdca, x="SDCA", y="Sites",
                            color="Avg_Rev", color_continuous_scale="RdYlGn", text="Sites",
                            title=f"Consistent Good Avail (≥{cga_thresh}%, {cga_months_lbl}) & Low Rev (≤₹{cga_rev}L)"),
                            use_container_width=True)
                    with col_cg2:
                        st.dataframe(cga_sdca, use_container_width=True, hide_index=True)

                # Traffic & Data summary chart (averages across months)
                avg_erl_col  = "Avg_Erl_Total" if "Avg_Erl_Total" in cga_df.columns else None
                avg_data_col = "Avg_Data_GB_Total" if "Avg_Data_GB_Total" in cga_df.columns else None
                if avg_erl_col and "SDCA" in cga_df.columns:
                    traf_sdca = cga_df.groupby("SDCA").agg(
                        **{avg_erl_col:  (avg_erl_col,"sum")},
                        **({avg_data_col: (avg_data_col,"sum")} if avg_data_col else {}),
                    ).round(1).reset_index()
                    td_melt = traf_sdca.melt("SDCA", var_name="Metric", value_name="Value")
                    td_melt["Metric"] = td_melt["Metric"].str.replace("Avg_","Avg ").str.replace("_"," ")
                    st.plotly_chart(px.bar(td_melt, x="SDCA", y="Value", color="Metric",
                        barmode="group",
                        title=f"Avg Traffic & Data (across {cga_months_lbl}) for Good-Avail / Low-Rev Sites",
                        color_discrete_map={"Avg Erl Total":"#636EFA","Avg Data GB Total":"#EF553B"}),
                        use_container_width=True)

                # Full site table
                show_cga = [c for c in (["BTS IP ID","BTS Name","SDCA","Site Type","LOCATION"]
                            + avail_month_cols
                            + (["Avg_Avail_All_Months"] if "Avg_Avail_All_Months" in cga_df.columns else [])
                            + (["Avg_Erl_Total","Avg_Data_GB_Total"] if avg_erl_col else [])
                            + ["REV_LAKH","4G_Cat","Perday_4G_GB","incharge","JTO INCHARGE"])
                            if c in cga_df.columns]

                st.markdown(f"**📋 {len(cga_df)} Sites — Consistently Good Availability, Poor Revenue**")
                def _cga_colour(val):
                    try:
                        v = float(val)
                        if v >= 95: return "background-color:#d4edda"
                        if v >= 90: return "background-color:#fff3cd"
                        if v <  90: return "background-color:#ffcccc"
                    except Exception:
                        pass
                    return ""
                colour_cols_cga = [c for c in avail_month_cols + ["Avg_Avail_All_Months"] if c in cga_df.columns]
                st.dataframe(
                    safe_style(cga_df[show_cga].sort_values("REV_LAKH").reset_index(drop=True).round(2),
                               _cga_colour, colour_cols_cga),
                    use_container_width=True)

                # Incharge summary
                if "incharge" in cga_df.columns:
                    st.markdown("**👷 Incharge-wise Breakdown**")
                    ic_cga = cga_df.groupby("incharge").agg(
                        Sites=("BTS IP ID","nunique"),
                        Avg_Rev=("REV_LAKH","mean"),
                        **({} if "Avg_Avail_All_Months" not in cga_df.columns else
                           {"Avg_Avail": ("Avg_Avail_All_Months","mean")}),
                    ).round(3).reset_index().sort_values("Sites", ascending=False)
                    st.dataframe(ic_cga, use_container_width=True, hide_index=True)
            else:
                st.success(f"✅ No sites consistently ≥{cga_thresh}% in all months AND revenue ≤₹{cga_rev}L.")

    st.markdown("---")

    # ── Low / Zero Revenue Sites ───────────────────────────────────────────
    st.subheader("🔴 Low & Zero Revenue Sites")
    low_thresh = st.slider("Low Revenue threshold (Lakhs)", 0.0, 1.0, 0.1, 0.01, key="low_rev_thresh")
    low_rev_df = rdf_detail[rdf_detail["REV_LAKH"] <= low_thresh].sort_values("REV_LAKH")
    zero_df    = rdf_detail[rdf_detail["REV_LAKH"] == 0]
    st.metric("Sites ≤ threshold",  len(low_rev_df))
    st.metric("Zero Revenue Sites", len(zero_df))

    if "SDCA" in low_rev_df.columns:
        fig_lr = px.bar(low_rev_df.groupby("SDCA")["BTSIPID"].count().reset_index(),
                        x="SDCA", y="BTSIPID", text="BTSIPID",
                        title=f"Low Revenue Sites (≤₹{low_thresh}L) by SDCA",
                        color_discrete_sequence=["#d62728"])
        fig_lr.update_traces(textposition="outside")
        st.plotly_chart(fig_lr, use_container_width=True)

    low_show = [c for c in ["BTSIPID","SITENAME","SDCA","LOCATION","REV_LAKH","4G_Cat","2G_Cat",
                              "incharge","JTO INCHARGE"] if c in low_rev_df.columns]
    st.dataframe(low_rev_df[low_show].reset_index(drop=True), use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 10 – Revenue Per Day
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[9]:
    st.header("📅 Revenue Per Day Analysis")

    if not has_revenue:
        st.info("👆 Upload RBC Revenue file(s) from the sidebar to enable this tab.")
        st.stop()

    pd_m_sel = st.selectbox("Select Month", rev_months_sorted,
                             index=len(rev_months_sorted)-1,
                             format_func=lambda x: x.upper(), key="pd_month")
    rdf_pd = rev_store[pd_m_sel].copy()

    perday_cols = [c for c in ["Perday_2G_Erl","Perday_3G_GB","Perday_4G_GB"] if c in rdf_pd.columns]

    # ── KPI ────────────────────────────────────────────────────────────────
    st.subheader(f"Per-Day Averages  ·  {pd_m_sel.upper()}")
    if perday_cols:
        k_cols = st.columns(len(perday_cols))
        labels = {"Perday_2G_Erl":"Avg 2G (Erl/day)","Perday_3G_GB":"Avg 3G (GB/day)","Perday_4G_GB":"Avg 4G (GB/day)"}
        for i, pc in enumerate(perday_cols):
            k_cols[i].metric(labels.get(pc,pc),
                             f"{rdf_pd[pc].mean():.2f}", f"Max: {rdf_pd[pc].max():.2f}")

    # ── Per-day by SDCA ────────────────────────────────────────────────────
    st.subheader("Per-Day Traffic & Data by SDCA")
    if perday_cols and "SDCA" in rdf_pd.columns:
        pd_sdca = rdf_pd.groupby("SDCA")[perday_cols].mean().round(2).reset_index()
        short_labels = {"Perday_2G_Erl":"2G Erl/day","Perday_3G_GB":"3G GB/day","Perday_4G_GB":"4G GB/day"}
        pd_sdca_m = pd_sdca.melt("SDCA", var_name="Metric", value_name="Value")
        pd_sdca_m["Metric"] = pd_sdca_m["Metric"].map(short_labels)
        st.plotly_chart(px.bar(pd_sdca_m, x="SDCA", y="Value", color="Metric", barmode="group",
                               text="Value", title=f"Avg Per-Day Traffic & Data by SDCA — {pd_m_sel.upper()}"),
                        use_container_width=True)
        st.dataframe(pd_sdca.rename(columns=short_labels), use_container_width=True, hide_index=True)

    # ── Per-day distribution histograms ───────────────────────────────────
    st.subheader("Per-Day Distribution")
    if perday_cols:
        hist_cols = st.columns(len(perday_cols))
        for i, pc in enumerate(perday_cols):
            with hist_cols[i]:
                st.plotly_chart(
                    px.histogram(rdf_pd[rdf_pd[pc]>0], x=pc, nbins=20,
                                 title=labels.get(pc,pc), color_discrete_sequence=["#636EFA"]),
                    use_container_width=True)

    # ── Per-day vs Revenue scatter ─────────────────────────────────────────
    st.subheader("Per-Day Traffic vs Revenue")
    if perday_cols and "REV_LAKH" in rdf_pd.columns:
        sc_pd_col = st.selectbox("Per-day metric (X)", perday_cols, key="pd_scatter")
        scatter_pd = rdf_pd[[sc_pd_col,"REV_LAKH","SDCA","SITENAME","LOCATION",
                              "2G_Cat","4G_Cat"]].dropna()
        st.plotly_chart(
            px.scatter(scatter_pd, x=sc_pd_col, y="REV_LAKH",
                       color="SDCA", size="REV_LAKH", size_max=20,
                       hover_name="SITENAME",
                       hover_data=["LOCATION","2G_Cat","4G_Cat"],
                       trendline="ols",
                       title=f"{labels.get(sc_pd_col,sc_pd_col)}  vs  Revenue (Lakhs)  — {pd_m_sel.upper()}"),
            use_container_width=True)

    # ── Per-day by Traffic Category ────────────────────────────────────────
    st.subheader("Per-Day Traffic by Category")
    cat_pd_tabs = st.tabs(["2G","3G","4G"])
    for cti, (cat_col, pd_col, tech) in enumerate([
            ("2G_Cat","Perday_2G_Erl","2G"),
            ("3G_Cat","Perday_3G_GB","3G"),
            ("4G_Cat","Perday_4G_GB","4G")]):
        with cat_pd_tabs[cti]:
            if cat_col not in rdf_pd.columns or pd_col not in rdf_pd.columns: continue
            cat_pd = rdf_pd.groupby(cat_col).agg(
                Sites=("BTSIPID","nunique"),
                Avg_Perday=(pd_col,"mean"),
                Total_Rev=("REV_LAKH","sum"),
            ).reindex([c for c in CAT_ORDER if c in rdf_pd[cat_col].dropna().unique()]).round(3).reset_index()
            col_pd1, col_pd2 = st.columns(2)
            with col_pd1:
                st.plotly_chart(px.bar(cat_pd, x=cat_col, y="Avg_Perday",
                    color=cat_col, text="Avg_Perday",
                    title=f"{tech} Avg Per-Day by Category",
                    color_discrete_sequence=["#1a9641","#a6d96a","#ffffbf","#fdae61","#d7191c"]),
                    use_container_width=True)
            with col_pd2:
                st.plotly_chart(px.bar(cat_pd, x=cat_col, y="Total_Rev",
                    color=cat_col, text="Total_Rev",
                    title=f"{tech} Total Revenue by Category",
                    color_discrete_sequence=px.colors.qualitative.Set2),
                    use_container_width=True)
            st.dataframe(cat_pd, use_container_width=True, hide_index=True)

    # ── MoM Per-Day Trend ─────────────────────────────────────────────────
    if len(rev_months_sorted) >= 2:
        st.subheader("📈 Per-Day Trend — Month over Month")
        if perday_cols:
            pd_mom = pd.DataFrame([
                {"Month": m.upper(),
                 **{labels.get(pc,pc): rev_store[m][pc].mean().round(2)
                    for pc in perday_cols if pc in rev_store[m].columns}}
                for m in rev_months_sorted
            ])
            st.plotly_chart(
                px.line(pd_mom.melt("Month",var_name="Metric",value_name="Value"),
                        x="Month", y="Value", color="Metric", markers=True,
                        title="Per-Day Avg Traffic & Data Trend — MoM"),
                use_container_width=True)
            st.dataframe(pd_mom, use_container_width=True, hide_index=True)

    # ── Site-level Per-Day Detail ──────────────────────────────────────────
    st.subheader("📋 Site-level Per-Day Detail")
    ref_df_state_pd = st.session_state.ref_df
    if ref_df_state_pd is not None:
        rdf_pd2 = rdf_pd.merge(ref_df_state_pd[["BTSIPID","incharge","JTO INCHARGE"]],
                                on="BTSIPID", how="left")
    else:
        rdf_pd2 = rdf_pd.copy()

    pd_show = [c for c in ["BTSIPID","SITENAME","SDCA","LOCATION",
               "Perday_2G_Erl","Perday_3G_GB","Perday_4G_GB",
               "REV_LAKH","2G_Cat","3G_Cat","4G_Cat","incharge","JTO INCHARGE"]
               if c in rdf_pd2.columns]
    sdca_pd_flt = st.multiselect("Filter SDCA", sorted(rdf_pd2["SDCA"].dropna().unique()),
                                  default=sorted(rdf_pd2["SDCA"].dropna().unique()), key="pd_sdca_flt")
    rdf_pd_filt = rdf_pd2[rdf_pd2["SDCA"].isin(sdca_pd_flt)] if sdca_pd_flt else rdf_pd2
    st.dataframe(rdf_pd_filt[pd_show].sort_values("Perday_4G_GB", ascending=False).reset_index(drop=True).round(2),
                 use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 11 – Executive Report
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[10]:
    st.header("Executive Report Card")
    df_exec = df_lat.copy()
    st.subheader(f"Vendor–Technology Matrix  ·  {latest_month}")

    v2g = df_exec.groupby("Vendor_2G_Derived").agg(
        Sites=("BTS IP ID","nunique"), Avg_Avail_2G=("Nw Avail (2G)","mean"),
        Total_Erl_2G=("Erl (2g)","sum")).reset_index().rename(columns={"Vendor_2G_Derived":"Vendor"})
    v2g.insert(0,"Technology","2G")

    v3g = df_exec.groupby("Vendor_3G_Derived").agg(
        Sites=("BTS IP ID","nunique"), Avg_Avail_3G=("Nw Avail (3G)","mean"),
        Total_Erl_3G=("Erl (3g)","sum")).reset_index().rename(columns={"Vendor_3G_Derived":"Vendor"})
    v3g.insert(0,"Technology","3G")

    phys_4g_exec = df_exec[df_exec["Has_4G_Physical"]]["BTS IP ID"].nunique()
    v4g = pd.DataFrame([{
        "Technology":"4G","Vendor":"Tejas",
        "Sites (Physical)": phys_4g_exec,
        "4G 700MHz":  int(df_exec["BTS Site ID (700)"].notna().sum())  if "BTS Site ID (700)"  in df_exec.columns else 0,
        "4G 2100MHz": int(df_exec["BTS Site ID (2100)"].notna().sum()) if "BTS Site ID (2100)" in df_exec.columns else 0,
        "4G 2500MHz": int(df_exec["BTS Site ID (2500)"].notna().sum()) if "BTS Site ID (2500)" in df_exec.columns else 0,
        "Avg_Avail_4G": round(df_exec[df_exec["Has_4G_Physical"]]["Nw Avail (4G TCS)"].mean(), 2)
                         if "Nw Avail (4G TCS)" in df_exec.columns else "N/A",
    }])

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("**2G Vendor Matrix**")
        st.dataframe(v2g.round(2), use_container_width=True)
    with col2:
        st.markdown("**3G Vendor Matrix**")
        st.dataframe(v3g.round(2), use_container_width=True)
    with col3:
        st.markdown("**4G — Tejas (Physical)**")
        st.dataframe(v4g, use_container_width=True)
        st.caption(f"Physical 4G: **{phys_4g_exec}** unique sites (≥1 of 700/2100/2500 bands)")

    # Site Type summary
    st.markdown("---")
    st.subheader("Site Type Summary")
    if "Site Type" in df_exec.columns:
        exec_stc = df_exec["Site Type"].value_counts().reset_index()
        exec_stc.columns = ["Site Type","Count"]
        exec_stc["% Share"] = (exec_stc["Count"]/exec_stc["Count"].sum()*100).round(1)
        exec_avail_st = [c for c in ["Nw Avail (2G)","Nw Avail (3G)","Nw Avail (4G TCS)"] if c in df_exec.columns]
        if exec_avail_st:
            av = df_exec.groupby("Site Type")[exec_avail_st].mean().round(2)
            av.columns = [c.replace("Nw Avail ","Avail ") for c in exec_avail_st]
            exec_stc = exec_stc.merge(av.reset_index(), on="Site Type", how="left")
        tot_st = exec_stc.select_dtypes(include="number").sum()
        tot_st["Site Type"] = "TOTAL"
        exec_stc_disp = pd.concat([exec_stc, pd.DataFrame([tot_st])], ignore_index=True)
        st.dataframe(exec_stc_disp.fillna("—").reset_index(drop=True), use_container_width=True)

        if "SDCA" in df_exec.columns:
            st.markdown("**SDCA × Site Type Matrix**")
            exec_piv = df_exec.groupby(["SDCA","Site Type"])["BTS IP ID"].nunique().unstack(fill_value=0)
            exec_piv["Total"] = exec_piv.sum(axis=1)
            exec_piv.loc["TOTAL"] = exec_piv.sum()
            st.dataframe(exec_piv, use_container_width=True)

    # Incharge summary in exec report
    if has_incharge or has_jto_incharge:
        st.markdown("---")
        st.subheader("Incharge Summary")
        for ic_col in ["incharge","JTO INCHARGE"]:
            if ic_col not in df_exec.columns: continue
            ic_s = df_exec.groupby(ic_col)["BTS IP ID"].nunique().reset_index()
            ic_s.columns = [ic_col,"Sites"]
            ic_s["% Network"] = (ic_s["Sites"]/ic_s["Sites"].sum()*100).round(1)
            if "Nw Avail (4G TCS)" in df_exec.columns:
                ic_av = df_exec.groupby(ic_col)["Nw Avail (4G TCS)"].mean().round(2).rename("Avg 4G Avail %")
                ic_s = ic_s.merge(ic_av.reset_index(), on=ic_col, how="left")
            ic_s.loc[len(ic_s)] = {ic_col:"TOTAL", "Sites":ic_s["Sites"].sum(),
                                    "% Network":100.0,
                                    **{c:ic_s[c].mean() for c in ic_s.columns if c not in [ic_col,"Sites","% Network"]}}
            st.markdown(f"**{ic_col}**")
            st.dataframe(ic_s.round(2).reset_index(drop=True), use_container_width=True)

    # ── Worst Sites by Technology — Latest Month ───────────────────────────
    st.markdown("---")
    st.subheader(f"📉 Worst Availability Sites by Technology  ·  {latest_month.upper()}  ({df_exec['MONTH'].iloc[0] if 'MONTH' in df_exec.columns else ''} {df_exec['YEAR'].iloc[0] if 'YEAR' in df_exec.columns else ''})")
    st.caption(f"Data for latest month: **{latest_month.upper()}**. Showing Top 10 worst sites per technology.")

    exec_base_cols = ["BTS IP ID","BTS Name","SDCA","Site Type"]
    if has_incharge:     exec_base_cols.append("incharge")
    if has_jto_incharge: exec_base_cols.append("JTO INCHARGE")

    def _colour(val):
        """Red <90, orange 90-95, else clear. Safe for non-numeric."""
        try:
            v = float(val)
            if v < 90: return "background-color:#ffcccc"
            if v < 95: return "background-color:#fff3cd"
        except Exception:
            pass
        return ""

    for tech, col, thresh_display in [("2G","Nw Avail (2G)",95),("3G","Nw Avail (3G)",95),("4G TCS","Nw Avail (4G TCS)",95)]:
        if col not in df_exec.columns: continue
        sc = exec_base_cols + [col]
        worst = df_exec[df_exec[col].notna()].nsmallest(10, col)[sc].reset_index(drop=True)
        n_below = int((df_exec[col] < thresh_display).sum())
        with st.expander(f"📡 {tech} — Top 10 Worst Sites  ({latest_month.upper()})  |  {n_below} sites below {thresh_display}%"):
            st.dataframe(safe_style(worst.round(2), _colour, [col]),
                         use_container_width=True)
            # Mini bar chart
            fig_w = px.bar(worst.reset_index(drop=True), x=col, y="BTS Name", orientation="h",
                           color=col, color_continuous_scale="RdYlGn",
                           range_color=[75, 100], text=col,
                           title=f"Worst 10 {tech} Sites — {latest_month.upper()}")
            fig_w.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig_w.update_layout(yaxis={"categoryorder":"total ascending"}, height=400)
            st.plotly_chart(fig_w, use_container_width=True)

    # ── Worst Sites by Incharge — Latest Month ─────────────────────────────
    if has_incharge or has_jto_incharge:
        st.markdown("---")
        st.subheader(f"👷 Worst Sites by Incharge  ·  {latest_month.upper()}")
        st.caption("Top 5 worst-performing sites for each incharge officer, grouped by technology.")

        for tech, col in [("2G","Nw Avail (2G)"),("3G","Nw Avail (3G)"),("4G TCS","Nw Avail (4G TCS)")]:
            if col not in df_exec.columns: continue
            with st.expander(f"📡 {tech} — Worst Sites by Incharge  ({latest_month.upper()})"):
                for ic_col, ic_lbl in ([("incharge","Incharge")] if has_incharge else []) + \
                                      ([("JTO INCHARGE","JTO Incharge")] if has_jto_incharge else []):
                    if ic_col not in df_exec.columns: continue
                    st.markdown(f"**{ic_lbl}**")
                    ic_worst_rows = []
                    for ic_val in sorted(df_exec[ic_col].dropna().unique()):
                        sub = df_exec[(df_exec[ic_col]==ic_val) & df_exec[col].notna()]
                        if len(sub)==0: continue
                        # Build col list without ic_col to avoid duplicate insert
                        row_cols = [c for c in exec_base_cols if c != ic_col] + [col]
                        top5 = sub.nsmallest(5, col)[row_cols].copy()
                        top5.insert(0, ic_col, ic_val)
                        ic_worst_rows.append(top5)
                    if ic_worst_rows:
                        ic_worst_df = pd.concat(ic_worst_rows, ignore_index=True).reset_index(drop=True)
                        st.dataframe(safe_style(ic_worst_df.round(2), _colour, [col]),
                                     use_container_width=True)

    # ── Consistent Poor Performers Summary in Exec Report ─────────────────
    if len(months_sorted) >= 2:
        st.markdown("---")
        st.subheader(f"🔴 Consistent Poor Performers — {', '.join(m.upper() for m in months_sorted)}")
        st.caption(f"Sites below 95% availability in ALL {len(months_sorted)} uploaded months.")
        exec_poor_thresh = 95
        for tech_lbl, avail_col in [("2G","Nw Avail (2G)"),("3G","Nw Avail (3G)"),("4G TCS","Nw Avail (4G TCS)")]:
            if avail_col not in df_all.columns: continue
            poor_sets_exec = []
            for m in months_sorted:
                dm = df_all[df_all["Month_Label"]==m]
                poor_sets_exec.append(set(dm[dm[avail_col]<exec_poor_thresh]["BTS IP ID"].dropna().astype(str)))
            if not poor_sets_exec: continue
            cpids = poor_sets_exec[0]
            for ps in poor_sets_exec[1:]: cpids = cpids & ps

            rows_exec = []
            for sid in sorted(cpids):
                mr_df = df_all[(df_all["BTS IP ID"].astype(str)==sid) & (df_all["Month_Label"]==latest_month)]
                if len(mr_df)==0:
                    mr_df = df_all[df_all["BTS IP ID"].astype(str)==sid].head(1)
                if len(mr_df)==0: continue
                mr = mr_df.iloc[0]
                row = {"BTS IP ID":sid, "BTS Name":mr.get("BTS Name",""),
                       "SDCA":mr.get("SDCA",""), "Site Type":mr.get("Site Type","")}
                if has_incharge:     row["incharge"]     = mr.get("incharge","")
                if has_jto_incharge: row["JTO INCHARGE"] = mr.get("JTO INCHARGE","")
                vals = []
                for m in months_sorted:
                    dm = df_all[(df_all["BTS IP ID"].astype(str)==sid) & (df_all["Month_Label"]==m)]
                    v = round(float(dm[avail_col].mean()), 2) if len(dm) and dm[avail_col].notna().any() else None
                    row[m.upper()] = v
                    if v is not None: vals.append(v)
                row["Avg"] = round(float(np.mean(vals)), 2) if vals else None
                rows_exec.append(row)

            if rows_exec:
                cp_exec_df = pd.DataFrame(rows_exec).sort_values("Avg").reset_index(drop=True)
                with st.expander(f"📡 {tech_lbl} — {len(cp_exec_df)} Consistent Poor Sites"):
                    month_cols_e = [m.upper() for m in months_sorted if m.upper() in cp_exec_df.columns]
                    style_cols_e = [c for c in month_cols_e + ["Avg"] if c in cp_exec_df.columns]
                    st.dataframe(safe_style(cp_exec_df.round(2), _colour, style_cols_e),
                              use_container_width=True)

    # KPI Summary
    st.markdown("---")
    st.subheader("KPI Summary")
    exec_total = df_exec["BTS IP ID"].nunique()
    exec_st_kpi = df_exec["Site Type"].value_counts().to_dict() if "Site Type" in df_exec.columns else {}
    kpis = {
        "Total Unique Sites":           exec_total,
        "2G Active Sites":              int((df_exec["2G cnt"]>0).sum()) if "2G cnt" in df_exec.columns else "N/A",
        "3G Active Sites":              int((df_exec["3G cnt"]>0).sum()) if "3G cnt" in df_exec.columns else "N/A",
        "4G Physical Sites":            phys_4g_exec,
        "  4G 700MHz":                  int(df_exec["BTS Site ID (700)"].notna().sum()) if "BTS Site ID (700)" in df_exec.columns else "N/A",
        "  4G 2100MHz":                 int(df_exec["BTS Site ID (2100)"].notna().sum()) if "BTS Site ID (2100)" in df_exec.columns else "N/A",
        "  4G 2500MHz":                 int(df_exec["BTS Site ID (2500)"].notna().sum()) if "BTS Site ID (2500)" in df_exec.columns else "N/A",
        "Avg 2G Availability %":        round(df_exec["Nw Avail (2G)"].mean(),2) if "Nw Avail (2G)" in df_exec.columns else "N/A",
        "Avg 3G Availability %":        round(df_exec["Nw Avail (3G)"].mean(),2) if "Nw Avail (3G)" in df_exec.columns else "N/A",
        "Avg 4G Availability %":        round(df_exec[df_exec["Has_4G_Physical"]]["Nw Avail (4G TCS)"].mean(),2) if "Nw Avail (4G TCS)" in df_exec.columns else "N/A",
        "Total Traffic (Erlangs)":      round(df_exec["Erl Total"].sum(),0) if "Erl Total" in df_exec.columns else "N/A",
        "Total Data (GB)":              round(df_exec["Data GB Total"].sum(),0) if "Data GB Total" in df_exec.columns else "N/A",
        "Unique Incharge Officers":     df_exec["incharge"].nunique() if has_incharge else "N/A",
        "Unique JTO Incharge Units":    df_exec["JTO INCHARGE"].nunique() if has_jto_incharge else "N/A",
    }
    for stype, cnt in sorted(exec_st_kpi.items()):
        kpis[f"Site Type {stype}"] = f"{cnt}  ({round(cnt/exec_total*100,1)}%)"
    st.table(pd.DataFrame(list(kpis.items()), columns=["KPI","Value"]))