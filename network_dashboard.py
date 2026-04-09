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

    # ── Always guarantee SDCA column exists ───────────────────────────────
    # SDCA must come from ref file (via BTSIPID join) or perf file directly.
    # NEVER fall back to SSA/SSAID — those are SSA-level names, not SDCA.
    if "SDCA" not in df.columns:
        df["SDCA"] = "Unknown"
    df["SDCA"] = df["SDCA"].str.strip().str.title().fillna("Unknown")

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
if "rev_df" not in st.session_state:         # dict: month_label → DataFrame (KKD only)
    st.session_state.rev_df = {}
if "rev_df_full" not in st.session_state:    # dict: month_label → DataFrame (all SSAs)
    st.session_state.rev_df_full = {}

# ── SSAID ↔ SSACODE mapping (TN Circle) ─────────────────────────────────────
# SSAID  : column in Monthly Performance File  (e.g. TNKAR)
# SSACODE: column in RBC Revenue File          (e.g. KKD)
# Both identify the same SSA/OA. Join key: BTS IP ID (perf) = BTSIPID (RBC).
# Confirmed mappings (user-verified):
#   TNKUM → CRDA  (not NGC — user confirmed CRDA = TNKUM)
#   TNNAG → NGC   (Nagercoil — NAG suffix)
#   TNCRDA removed (TNKUM is the correct SSAID for CRDA)
SSAID_TO_CODE = {
    "TNCOI":"CBE",  "TNCOO":"CON",  "TNCUD":"CDL",  "TNDHA":"DPI",
    "TNERO":"ERD",  "TNKAR":"KKD",  "TNKUM":"CRDA", "TNMAD":"MA",
    "TNNAG":"NGC",  "TNPON":"PY",   "TNSAL":"SLM",  "TNTHA":"TNJ",
    "TNTIR":"TVL",  "TNTRI":"TR",   "TNTUT":"TT",   "TNVEL":"VLR",
    "TNVIR":"VGR",
}
# Reverse: SSACODE → SSAID (one-to-one after correction)
CODE_TO_SSAID = {
    "CBE":"TNCOI",  "CON":"TNCOO",  "CDL":"TNCUD",  "DPI":"TNDHA",
    "ERD":"TNERO",  "KKD":"TNKAR",  "CRDA":"TNKUM", "MA":"TNMAD",
    "NGC":"TNNAG",  "PY":"TNPON",   "SLM":"TNSAL",  "TNJ":"TNTHA",
    "TVL":"TNTIR",  "TR":"TNTRI",   "TT":"TNTUT",   "VLR":"TNVEL",
    "VGR":"TNVIR",
}
# Friendly display name for each SSACODE (used throughout the dashboard)
SSA_DISPLAY = {
    "CBE":"Coimbatore",   "CON":"Coonoor",      "CDL":"Cuddalore",
    "DPI":"Dharmapuri",   "ERD":"Erode",        "KKD":"Karaikudi",
    "NGC":"Nagercoil",    "MA":"Madurai",        "PY":"Pondicherry",
    "SLM":"Salem",        "TNJ":"Thanjavur",    "TVL":"Tirunelveli",
    "TT":"Tuticorin",     "VLR":"Vellore",      "VGR":"Virudhunagar",
    "TR":"Trichy",        "CRDA":"CRDA",
}
# All 17 OA display names in alphabetical order (used in filters)
ALL_OA_DISPLAY = sorted(SSA_DISPLAY.values())

# ─────────────────────────── SIDEBAR ──────────────────────────────────────────

with st.sidebar:
    st.title("📡 TN Circle Dashboard")

    # ── Reference file (OPTIONAL — only needed for Incharge Analysis tab) ───
    with st.expander("① Upload Reference File (optional — for Incharge Analysis only)"):
        st.caption("BTSIPID_PKEY1 file adds incharge officer data. "
                   "Not required for availability, revenue, or OA analysis.")
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
                rdf_raw = pd.read_excel(rf, sheet_name=sheet)

                # ── Clean & deduplicate ──────────────────────────────────
                rev_num_cols = ["2G_Traffic","2G_Data","3G_Traffic","3G_Data","4G_Traffic","4G_Data",
                                "TOT_TRAFFIC","TOT_DATA","TRAFFIC_REV","DATA_REV","TOT_REV","REV_LAKH",
                                "2g_rev","3g_rev","4g_rev","Perday_2G_Erl","Perday_3G_GB","Perday_4G_GB"]
                for c in rev_num_cols:
                    if c in rdf_raw.columns:
                        rdf_raw[c] = pd.to_numeric(rdf_raw[c], errors="coerce")
                rdf_raw["BTSIPID"] = rdf_raw["BTSIPID"].astype(str).str.strip()

                # Remove invalid BTSIPID (blank/0/nan = IBS micro sites with no network ID)
                rdf_raw = rdf_raw[~rdf_raw["BTSIPID"].isin(["0","nan","","NaN","None"])].copy()

                # Deduplicate: same BTSIPID billed across multiple rows (split billing)
                # → keep first row's metadata, SUM all revenue/traffic columns
                meta_cols_rbc = [c for c in ["PKEY","SSACODE","SSANAME","SDCANAME","SITENAME",
                                              "LOCATION","2G_Cat","3G_Cat","4G_Cat",
                                              "2G TECH","3G TECH","4G TECH"] if c in rdf_raw.columns]
                sum_cols_rbc  = [c for c in rev_num_cols if c in rdf_raw.columns]
                agg_rbc = {c: "first" for c in meta_cols_rbc}
                agg_rbc.update({c: "sum" for c in sum_cols_rbc})
                rdf_clean = rdf_raw.groupby("BTSIPID", sort=False).agg(agg_rbc).reset_index()

                # SDCA from RBC SDCANAME (billing-authoritative)
                if "SDCANAME" in rdf_clean.columns:
                    rdf_clean["SDCA"] = (rdf_clean["SDCANAME"].str.strip().str.title()
                                         .str.replace("Tirupathur","Tirupattur",regex=False))
                rdf_clean["SDCA"] = rdf_clean.get("SDCA", pd.Series(dtype=str)).fillna("Unknown")
                rdf_clean["SSA_Label"] = rdf_clean["SSACODE"].map(SSA_DISPLAY).fillna(rdf_clean["SSACODE"]) \
                    if "SSACODE" in rdf_clean.columns else ""
                rdf_clean["Rev_Month"] = m_lbl

                raw_rows  = len(rdf_raw) + (rdf_raw["BTSIPID"].duplicated().sum())
                dup_fixed = len(rdf_raw) - len(rdf_clean)
                st.session_state.rev_df_full[m_lbl] = rdf_clean   # full circle (all SSAs, clean)

                # KKD-filtered for existing tabs
                rdf_kkd = rdf_clean[rdf_clean["SSACODE"].astype(str).str.strip() == "KKD"].copy()
                st.session_state.rev_df[m_lbl] = rdf_kkd

                st.success(f"✅ {m_lbl.upper()}: **{len(rdf_kkd)}** KKD sites · "
                           f"**{len(rdf_clean)}** Circle sites · "
                           f"{dup_fixed} billing splits merged · "
                           f"{rdf_clean['SSACODE'].nunique()} SSAs loaded")
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

        # ── OA / SSA filter — uses SSACODE as universal key ─────────────────
        # Collect all known SSACODEs: from perf (via SSAID→SSACODE) + from RBC
        _perf_ssacodes = set()
        if "SSAID" in st.session_state.master_df.columns:
            for _ssaid in st.session_state.master_df["SSAID"].dropna().unique():
                _sc = SSAID_TO_CODE.get(str(_ssaid).strip(), None)
                if _sc: _perf_ssacodes.add(_sc)
        _rbc_ssacodes = set()
        if st.session_state.rev_df_full:
            for _rdf in st.session_state.rev_df_full.values():
                if "SSACODE" in _rdf.columns:
                    _rbc_ssacodes.update(_rdf["SSACODE"].dropna().astype(str).str.strip().unique())
        # Union: all known OAs; default to KKD if nothing loaded
        all_known_codes = sorted((_perf_ssacodes | _rbc_ssacodes) or {"KKD"},
                                  key=lambda c: SSA_DISPLAY.get(c, c))
        # Format: "Karaikudi (KKD)"
        def _oa_label(code):
            return f"{SSA_DISPLAY.get(code, code)}  ({code})"
        _default_code = "KKD" if "KKD" in all_known_codes else all_known_codes[0]
        sel_ssacode = st.selectbox(
            "🔍 OA / SSA Filter (applies to all tabs)",
            all_known_codes,
            index=all_known_codes.index(_default_code),
            format_func=_oa_label,
            key="sel_ssa_perf",
        )
        sel_ssa = CODE_TO_SSAID.get(sel_ssacode, sel_ssacode)  # SSACODE→SSAID for perf filter

        # Re-apply reference if ref loaded after perf files
        if ref_upload and st.session_state.ref_df is not None:
            if "incharge" not in st.session_state.master_df.columns:
                st.info("Re-upload performance files to apply reference enrichment.")
    else:
        sel_months   = []
        sel_ssacode  = "KKD"
        sel_ssa      = "TNKAR"

if st.session_state.master_df is None:
    st.info("👆  **Step 1:** Upload the reference file (BTSIPID_PKEY1_excel.xlsx)\n\n"
            "👆  **Step 2:** Upload monthly performance files (Jan CSV, Dec XLSX, …)")
    st.stop()

# ── OA filter: sel_ssacode set in sidebar; derive SSAID set for perf filter ───
# sel_ssacode: SSACODE string e.g. "KKD"  (set by sidebar selectbox)
# sel_ssa    : primary SSAID e.g. "TNKAR" (fallback for single-SSAID filter)
# For perf filter, match SSAID→SSACODE mapping (TNKUM→CRDA, TNNAG→NGC)
df_all = st.session_state.master_df.copy()
if sel_months:
    df_all = df_all[df_all["Month_Label"].isin(sel_months)]
# Filter perf by SSACODE (via SSAID map) when multiple SSAs loaded
if "SSAID" in df_all.columns:
    df_all["_ssacode_tmp"] = df_all["SSAID"].map(SSAID_TO_CODE).fillna(df_all["SSAID"])
    if df_all["_ssacode_tmp"].nunique() > 1:
        df_all = df_all[df_all["_ssacode_tmp"] == sel_ssacode].copy()
    df_all.drop(columns=["_ssacode_tmp"], inplace=True, errors="ignore")

months_sorted = sorted(df_all["Month_Label"].unique(), key=month_sort_key)
latest_month  = months_sorted[-1]    # ← always the chronologically latest
prev_month    = months_sorted[-2] if len(months_sorted) >= 2 else None

AVAIL_MAP      = {"2G": "Nw Avail (2G)", "3G": "Nw Avail (3G)", "4G TCS": "Nw Avail (4G TCS)"}
existing_avail = {k: v for k, v in AVAIL_MAP.items() if v in df_all.columns}

has_incharge     = "incharge"      in df_all.columns
has_jto_incharge = "JTO INCHARGE" in df_all.columns
has_location     = "LOCATION"     in df_all.columns
has_sitename     = "SITENAME"     in df_all.columns

# ── Latest-month dataframe ────────────────────────────────────────────────────
df_lat = df_all[df_all["Month_Label"] == latest_month].copy()

# ── STEP 1: OA / Circle revenue globals (all SSAs) ───────────────────────────
rev_store_full = st.session_state.rev_df_full       # dict month→df, all 17 SSAs
has_oa_revenue = bool(rev_store_full)
if has_oa_revenue:
    oa_rev_months_sorted = sorted(rev_store_full.keys(), key=month_sort_key)
    oa_latest_month      = oa_rev_months_sorted[-1]
    oa_rev_all           = pd.concat(rev_store_full.values(), ignore_index=True)
    oa_rev_lat           = rev_store_full[oa_latest_month].copy()
else:
    oa_rev_months_sorted = []; oa_latest_month = None
    oa_rev_all = None; oa_rev_lat = None

# ── STEP 2: Build rev_store filtered to selected SSA ─────────────────────────
# Must be defined BEFORE SDCA remap so we can use it as the SDCA source.
_rev_raw = st.session_state.rev_df               # KKD-only fallback from load time
if rev_store_full and sel_ssacode:
    rev_store = {}
    for _m, _rdf_full in rev_store_full.items():
        _f = _rdf_full[_rdf_full["SSACODE"].astype(str).str.strip() == sel_ssacode].copy()
        if len(_f):
            rev_store[_m] = _f
    if not rev_store:                            # SSA not in RBC — fall back
        rev_store = _rev_raw
else:
    rev_store = _rev_raw

# ── Add SSA_Label to perf data from SSAID (no ref needed) ───────────────────
if "SSAID" in df_all.columns:
    df_all["SSA_Code"]  = df_all["SSAID"].map(SSAID_TO_CODE).fillna(df_all["SSAID"])
    df_all["SSA_Label"] = df_all["SSA_Code"].map(SSA_DISPLAY).fillna(df_all["SSA_Code"])
    df_lat["SSA_Code"]  = df_lat["SSAID"].map(SSAID_TO_CODE).fillna(df_lat["SSAID"])
    df_lat["SSA_Label"] = df_lat["SSA_Code"].map(SSA_DISPLAY).fillna(df_lat["SSA_Code"])

# ── STEP 3: Remap SDCA in perf data from latest rev_store SDCANAME ───────────
# RBC SDCANAME is the billing-authoritative SDCA for the selected SSA.
# Fallback for perf sites absent from RBC: use the perf file SDCA column.
if rev_store:
    _sdca_src = rev_store[sorted(rev_store.keys(), key=month_sort_key)[-1]]
    if "SDCANAME" in _sdca_src.columns:
        _sdca_lkp = (
            _sdca_src[["BTSIPID","SDCANAME"]]
            .dropna(subset=["SDCANAME"])
            .drop_duplicates("BTSIPID")
            .set_index("BTSIPID")["SDCANAME"]
            .str.strip().str.title()
            .str.replace("Tirupathur", "Tirupattur", regex=False)
        )
        for _df in [df_all, df_lat]:
            _mapped = _df["BTS IP ID"].map(_sdca_lkp)
            _df["SDCA"] = _mapped.fillna(
                _df["SDCA"].str.strip().str.title()
                if "SDCA" in _df.columns else pd.Series("Unknown", index=_df.index)
            ).fillna("Unknown")
    else:
        for _df in [df_all, df_lat]:
            if "SDCA" not in _df.columns: _df["SDCA"] = "Unknown"
            _df["SDCA"] = _df["SDCA"].str.strip().str.title().fillna("Unknown")
else:
    # No RBC at all — use perf SDCA directly
    for _df in [df_all, df_lat]:
        if "SDCA" not in _df.columns: _df["SDCA"] = "Unknown"
        _df["SDCA"] = _df["SDCA"].str.strip().str.title().fillna("Unknown")

# ── STEP 4: Revenue globals derived from rev_store ───────────────────────────
has_revenue = bool(rev_store)
if has_revenue:
    rev_all           = pd.concat(rev_store.values(), ignore_index=True)
    rev_months_sorted = sorted(rev_store.keys(), key=month_sort_key)
    latest_rev_month  = rev_months_sorted[-1]
    rev_lat           = rev_store[latest_rev_month].copy()
    # Ensure SDCA on rev_lat from SDCANAME
    if "SDCANAME" in rev_lat.columns:
        rev_lat["SDCA"] = (rev_lat["SDCANAME"].str.strip().str.title()
                           .str.replace("Tirupathur","Tirupattur",regex=False))
    rev_lat["SDCA"]   = rev_lat.get("SDCA", pd.Series(dtype=str)).fillna("Unknown")
    # Merge latest perf + latest rev
    _rev_merge_cols = [c for c in ["BTSIPID","REV_LAKH","TOT_REV","TRAFFIC_REV","DATA_REV",
                 "2G_Traffic","2G_Data","3G_Traffic","3G_Data","4G_Traffic","4G_Data",
                 "TOT_TRAFFIC","TOT_DATA","2g_rev","3g_rev","4g_rev",
                 "Perday_2G_Erl","Perday_3G_GB","Perday_4G_GB",
                 "2G_Cat","3G_Cat","4G_Cat","2G TECH","3G TECH","4G TECH"]
                 if c in rev_lat.columns]
    df_lat_rev = df_lat.merge(rev_lat[_rev_merge_cols],
                              left_on="BTS IP ID", right_on="BTSIPID",
                              how="left", suffixes=("","_rbc"))
    CAT_ORDER = ["VHT","HT","MT","LT","VLT"]
else:
    rev_all = None; rev_lat = None; df_lat_rev = None
    latest_rev_month = None; rev_months_sorted = []; CAT_ORDER = []

# ── Active SSA banner ─────────────────────────────────────────────────────────
_ssa_name_display = SSA_DISPLAY.get(sel_ssacode, sel_ssacode)
_rev_site_count   = (rev_store[rev_months_sorted[-1]]["BTSIPID"].nunique()
                     if has_revenue else 0)
st.info(f"🔍 **Active SSA: {sel_ssa}  ({_ssa_name_display})**  —  "
        f"Perf: {df_lat['BTS IP ID'].nunique()} sites  |  "
        f"Revenue: {_rev_site_count} sites"
        if has_revenue else
        f"🔍 **Active SSA: {sel_ssa}  ({_ssa_name_display})**  —  "
        f"Perf: {df_lat['BTS IP ID'].nunique()} sites  |  No revenue data loaded")

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
    "🌐 OA / Circle View",
    "📶 Circle Availability",
    "📊 Period Summary",
    "🏭 Vendor Availability",
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
    _tab7_ok = has_incharge or has_jto_incharge
    if not _tab7_ok:
        st.warning("Incharge data not available. Upload the reference file (BTSIPID_PKEY1_excel.xlsx) "
                   "before uploading monthly performance files.")
    if _tab7_ok:
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
    if has_revenue:
        rev_m_sel = st.selectbox("Select Revenue Month", rev_months_sorted,
                              index=len(rev_months_sorted)-1,
                              format_func=lambda x: x.upper())
    rdf_sel = rev_store[rev_m_sel].copy()
    # SDCA from RBC SDCANAME (authoritative billing SDCA); normalize spelling
    if "SDCANAME" in rdf_sel.columns:
        rdf_sel["SDCA"] = (rdf_sel["SDCANAME"].str.strip().str.title()
                           .str.replace("Tirupathur","Tirupattur",regex=False))
    if "SDCA" not in rdf_sel.columns:
        rdf_sel["SDCA"] = "Unknown"
    rdf_sel["SDCA"] = rdf_sel["SDCA"].fillna("Unknown")
    # Add SSA display label
    rdf_sel["SSA_Label"] = rdf_sel["SSACODE"].map(SSA_DISPLAY).fillna(
        rdf_sel["SSACODE"]) if "SSACODE" in rdf_sel.columns else _ssa_name_display

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

    # ── Technology-wise KPI strip ──────────────────────────────────────────
    tech_rev_map = {"2G": "2g_rev", "3G": "3g_rev", "4G": "4g_rev"}
    tech_site_map = {"2G": "2G TECH", "3G": "3G TECH", "4G": "4G TECH"}
    tech_traffic_map = {"2G": ("2G_Traffic","2G_Data"), "3G": ("3G_Traffic","3G_Data"), "4G": ("4G_Traffic","4G_Data")}
    available_techs = [t for t,c in tech_rev_map.items() if c in rdf_sel.columns]

    if available_techs:
        st.markdown("**Technology-wise Revenue Breakdown**")
        tw_cols = st.columns(len(available_techs))
        for i, tech in enumerate(available_techs):
            rc  = tech_rev_map[tech]
            vc  = tech_site_map.get(tech)
            trc, drc = tech_traffic_map.get(tech, (None, None))
            t_rev_lakh = rdf_sel[rc].sum() / 100000
            t_sites    = int(rdf_sel[rdf_sel[vc].notna()]["BTSIPID"].nunique()) if vc and vc in rdf_sel.columns else "—"
            t_share    = (rdf_sel[rc].sum() / rdf_sel[tech_rev_map["2G"]].add(rdf_sel[tech_rev_map["3G"]]).add(rdf_sel[tech_rev_map["4G"]]).sum() * 100) if all(c in rdf_sel.columns for c in tech_rev_map.values()) else None
            traf_sum   = rdf_sel[trc].sum() if trc and trc in rdf_sel.columns else None
            data_sum   = rdf_sel[drc].sum() if drc and drc in rdf_sel.columns else None
            with tw_cols[i]:
                share_str = f"  ({t_share:.1f}% of total)" if t_share is not None else ""
                st.metric(f"{tech} Revenue (Lakhs)", f"₹{t_rev_lakh:.2f}{share_str}")
                if traf_sum is not None:
                    unit = "Erl" if tech == "2G" else "GB"
                    st.caption(f"Traffic: {traf_sum:,.0f} {unit}  |  Data: {data_sum:,.0f} GB  |  Sites: {t_sites}")

    st.markdown("---")

    # ── Revenue by SDCA ────────────────────────────────────────────────────
    st.subheader(f"📍 Revenue by SDCA  ·  {rev_m_sel.upper()}")

    # Build SDCA summary with tech breakdown columns
    sdca_agg = dict(
        Sites=("BTSIPID","nunique"),
        Total_Rev_Lakh=("REV_LAKH","sum"),
        Avg_Rev_Lakh=("REV_LAKH","mean"),
        Max_Rev_Lakh=("REV_LAKH","max"),
        Zero_Sites=("REV_LAKH", lambda x: (x==0).sum()),
        Traffic_Rev=("TRAFFIC_REV","sum"),
        Data_Rev=("DATA_REV","sum"),
    )
    for tech, rc in tech_rev_map.items():
        if rc in rdf_sel.columns:
            rdf_sel[f"{tech}_Rev_Lakh"] = rdf_sel[rc] / 100000
            sdca_agg[f"{tech}_Rev_Lakh"] = (f"{tech}_Rev_Lakh", "sum")
    sdca_rev = rdf_sel.groupby("SDCA").agg(**sdca_agg).round(3).reset_index().sort_values("Total_Rev_Lakh", ascending=False)

    col_r1, col_r2 = st.columns([1.4, 1])
    with col_r1:
        fig_srev = px.bar(sdca_rev, x="SDCA", y="Total_Rev_Lakh", color="Total_Rev_Lakh",
                          color_continuous_scale="Greens", text="Total_Rev_Lakh",
                          title=f"Total Revenue (Lakhs) by SDCA — {rev_m_sel.upper()}")
        fig_srev.update_traces(texttemplate="₹%{text:.2f}L", textposition="outside")
        fig_srev.update_layout(xaxis_tickangle=-30, coloraxis_showscale=False)
        st.plotly_chart(fig_srev, use_container_width=True)
    with col_r2:
        st.markdown("**SDCA Revenue Table (with 2G / 3G / 4G split)**")
        st.dataframe(sdca_rev.reset_index(drop=True), use_container_width=True, hide_index=True)

    # Stacked bar: 2G + 3G + 4G revenue by SDCA
    tech_sdca_cols = [c for c in ["2G_Rev_Lakh","3G_Rev_Lakh","4G_Rev_Lakh"] if c in sdca_rev.columns]
    if tech_sdca_cols:
        sdca_tech_melt = sdca_rev.melt("SDCA", tech_sdca_cols, var_name="Technology", value_name="Rev_Lakh")
        sdca_tech_melt["Technology"] = sdca_tech_melt["Technology"].str.replace("_Rev_Lakh","")
        st.plotly_chart(px.bar(sdca_tech_melt, x="SDCA", y="Rev_Lakh", color="Technology",
                               barmode="stack",
                               title=f"2G / 3G / 4G Revenue by SDCA (Lakhs) — {rev_m_sel.upper()}",
                               color_discrete_map={"2G":"#636EFA","3G":"#EF553B","4G":"#00CC96"},
                               text="Rev_Lakh").update_traces(texttemplate="%{text:.2f}", textposition="inside"),
                        use_container_width=True)

    # Traffic vs Data revenue split by SDCA
    sdca_rev_melt = sdca_rev.melt("SDCA", ["Traffic_Rev","Data_Rev"], var_name="Type", value_name="Rev")
    sdca_rev_melt["Type"] = sdca_rev_melt["Type"].map({"Traffic_Rev":"Traffic","Data_Rev":"Data"})
    st.plotly_chart(px.bar(sdca_rev_melt, x="SDCA", y="Rev", color="Type", barmode="stack",
                           title="Traffic vs Data Revenue by SDCA",
                           color_discrete_map={"Traffic":"#636EFA","Data":"#EF553B"}),
                    use_container_width=True)

    # Technology × SDCA heatmap
    if tech_sdca_cols and "SDCA" in sdca_rev.columns:
        st.markdown("**Technology Revenue Heatmap — SDCA × Technology (Lakhs)**")
        heat_df = sdca_rev.set_index("SDCA")[tech_sdca_cols].rename(
            columns={c: c.replace("_Rev_Lakh","") for c in tech_sdca_cols})
        import plotly.graph_objects as _go  # noqa: already imported as go at top
        fig_heat = go.Figure(data=go.Heatmap(
            z=heat_df.values.tolist(),
            x=heat_df.columns.tolist(),
            y=heat_df.index.tolist(),
            colorscale="Greens",
            text=[[f"₹{v:.2f}L" for v in row] for row in heat_df.values.tolist()],
            texttemplate="%{text}",
            hoverongaps=False,
        ))
        fig_heat.update_layout(title=f"Revenue Heatmap: SDCA × Technology — {rev_m_sel.upper()}",
                               height=400)
        st.plotly_chart(fig_heat, use_container_width=True)

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
    if has_revenue:
        pd_m_sel = st.selectbox("Select Month", rev_months_sorted,
                             index=len(rev_months_sorted)-1,
                             format_func=lambda x: x.upper(), key="pd_month")
    rdf_pd = rev_store[pd_m_sel].copy()
    if "SDCANAME" in rdf_pd.columns:
        rdf_pd["SDCA"] = (rdf_pd["SDCANAME"].str.strip().str.title()
                          .str.replace("Tirupathur","Tirupattur",regex=False))
    if "SDCA" not in rdf_pd.columns:
        rdf_pd["SDCA"] = "Unknown"
    rdf_pd["SDCA"] = rdf_pd["SDCA"].fillna("Unknown")

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
# TAB 11 – OA / Circle View
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[10]:
    st.header("🌐 OA / Circle View — TN Circle Revenue Analysis")

    if not has_oa_revenue:
        st.info("👆 Upload an RBC revenue file (③ in sidebar) to enable Circle-level analysis.\n\n"
                "**Important:** Re-upload the RBC file after updating the code to populate Circle data.")
    if has_oa_revenue:
        # Month selector
        oa_m_sel = st.selectbox("Revenue Month", oa_rev_months_sorted,
                             index=len(oa_rev_months_sorted)-1,
                             format_func=lambda x: x.upper(), key="oa_month_sel")
    oa_df = rev_store_full[oa_m_sel].copy()
    if "SDCANAME" in oa_df.columns:
        oa_df["SDCA"] = (oa_df["SDCANAME"].str.strip().str.title()
                         .str.replace("Tirupathur","Tirupattur",regex=False))
    if "SDCA" not in oa_df.columns:
        oa_df["SDCA"] = "Unknown"
    oa_df["SDCA"] = oa_df["SDCA"].fillna("Unknown")

    # Ensure SSA_Label exists
    oa_df["SSA_Label"] = oa_df["SSACODE"].map(SSA_DISPLAY).fillna(oa_df["SSACODE"])

    # ── OA Aggregation helper ──────────────────────────────────────────────
    def oa_agg(df):
        tech_rev_map_oa = {"2G":"2g_rev","3G":"3g_rev","4G":"4g_rev"}
        agg = dict(
            SSA_Name=("SSA_Label","first"),
            Sites=("BTSIPID","nunique"),
            Total_Rev_Lakh=("REV_LAKH","sum"),
            Avg_Rev_Lakh=("REV_LAKH","mean"),
            Max_Rev_Lakh=("REV_LAKH","max"),
            Zero_Sites=("REV_LAKH", lambda x:(x==0).sum()),
        )
        for tech, rc in tech_rev_map_oa.items():
            if rc in df.columns:
                df[f"__{tech}_L"] = df[rc] / 100000
                agg[f"{tech}_Rev_Lakh"] = (f"__{tech}_L","sum")
        for tc in ["TOT_TRAFFIC","TOT_DATA","TRAFFIC_REV","DATA_REV"]:
            if tc in df.columns: agg[tc] = (tc,"sum")
        result = df.groupby("SSACODE").agg(**agg).round(3).reset_index()
        result["vs_Circle_Avg_%"] = (
            (result["Avg_Rev_Lakh"] - result["Avg_Rev_Lakh"].mean())
            / result["Avg_Rev_Lakh"].mean() * 100
        ).round(1)
        return result.sort_values("Total_Rev_Lakh", ascending=False)

    oa_ssa = oa_agg(oa_df)

    # ── Circle KPIs ────────────────────────────────────────────────────────
    st.subheader(f"📊 Circle Summary — {oa_m_sel.upper()}")
    ck1, ck2, ck3, ck4, ck5 = st.columns(5)
    ck1.metric("Circle Total Revenue (Lakhs)", f"₹{oa_df['REV_LAKH'].sum():.2f}")
    ck2.metric("Total Sites (unique)",          oa_df["BTSIPID"].nunique())
    ck3.metric("SSAs",                          oa_df["SSACODE"].nunique())
    ck4.metric("Avg Rev/Site (Lakhs)",          f"₹{oa_df['REV_LAKH'].mean():.3f}")
    ck5.metric("Zero Revenue Sites",            int((oa_df["REV_LAKH"]==0).sum()))

    tech_rev_oa = {"2G":"2g_rev","3G":"3g_rev","4G":"4g_rev"}
    tw1, tw2, tw3 = st.columns(3)
    tot_oa = sum(oa_df[rc].sum() for rc in tech_rev_oa.values() if rc in oa_df.columns)
    for col_tw, (tech, rc) in zip([tw1,tw2,tw3], tech_rev_oa.items()):
        if rc in oa_df.columns:
            v     = oa_df[rc].sum() / 100000
            share = v / (tot_oa/100000) * 100 if tot_oa else 0
            col_tw.metric(f"{tech} Revenue (Lakhs)", f"₹{v:.2f}  ({share:.1f}%)")
    st.markdown("---")

    # ── Revenue by SSA — bar + table ───────────────────────────────────────
    st.subheader("📍 Revenue by SSA")
    col_oa1, col_oa2 = st.columns([1.5, 1])
    with col_oa1:
        fig_oa = px.bar(oa_ssa, x="SSA_Name", y="Total_Rev_Lakh",
                        color="Total_Rev_Lakh", color_continuous_scale="Greens",
                        text="Total_Rev_Lakh",
                        title=f"Total Revenue by SSA — {oa_m_sel.upper()}")
        fig_oa.update_traces(texttemplate="₹%{text:.2f}L", textposition="outside")
        fig_oa.update_layout(xaxis_tickangle=-35, coloraxis_showscale=False)
        st.plotly_chart(fig_oa, use_container_width=True)
    with col_oa2:
        show_ssa_cols = [c for c in ["SSA_Name","Sites","Total_Rev_Lakh","Avg_Rev_Lakh",
                                      "Max_Rev_Lakh","Zero_Sites","vs_Circle_Avg_%",
                                      "2G_Rev_Lakh","3G_Rev_Lakh","4G_Rev_Lakh"] if c in oa_ssa.columns]
        def _ssa_style(val):
            try:
                v = float(val)
                if v > 0:  return "background-color:#d4edda;color:#155724"
                if v < 0:  return "background-color:#f8d7da;color:#721c24"
            except: pass
            return ""
        st.dataframe(safe_style(oa_ssa[show_ssa_cols].reset_index(drop=True), _ssa_style,
                                ["vs_Circle_Avg_%"]),
                     use_container_width=True, hide_index=True)

    # Stacked bar 2G/3G/4G
    tech_ssa_cols = [c for c in ["2G_Rev_Lakh","3G_Rev_Lakh","4G_Rev_Lakh"] if c in oa_ssa.columns]
    if tech_ssa_cols:
        oa_tech_melt = oa_ssa.melt("SSA_Name", tech_ssa_cols, var_name="Technology", value_name="Rev_Lakh")
        oa_tech_melt["Technology"] = oa_tech_melt["Technology"].str.replace("_Rev_Lakh","")
        st.plotly_chart(px.bar(oa_tech_melt, x="SSA_Name", y="Rev_Lakh", color="Technology",
                               barmode="stack",
                               title=f"2G / 3G / 4G Revenue Split by SSA — {oa_m_sel.upper()}",
                               color_discrete_map={"2G":"#636EFA","3G":"#EF553B","4G":"#00CC96"},
                               text="Rev_Lakh").update_traces(texttemplate="%{text:.1f}",
                                                              textposition="inside"),
                        use_container_width=True)

    # Avg revenue/site — normalised comparison
    st.plotly_chart(px.bar(oa_ssa.sort_values("Avg_Rev_Lakh", ascending=False),
                           x="SSA_Name", y="Avg_Rev_Lakh",
                           color="vs_Circle_Avg_%", color_continuous_scale="RdYlGn",
                           text="Avg_Rev_Lakh",
                           title="Avg Revenue/Site by SSA — Normalised (green = above circle avg)"),
                    use_container_width=True)

    # Heatmap: SSA × Technology
    if tech_ssa_cols:
        st.markdown("**SSA × Technology Revenue Heatmap (Lakhs)**")
        heat_oa = oa_ssa.set_index("SSA_Name")[tech_ssa_cols].rename(
            columns={c: c.replace("_Rev_Lakh","") for c in tech_ssa_cols})
        fig_oa_heat = go.Figure(data=go.Heatmap(
            z=heat_oa.values.tolist(), x=heat_oa.columns.tolist(), y=heat_oa.index.tolist(),
            colorscale="YlGn",
            text=[[f"₹{v:.2f}L" for v in row] for row in heat_oa.values.tolist()],
            texttemplate="%{text}", hoverongaps=False,
        ))
        fig_oa_heat.update_layout(title=f"Revenue Heatmap: SSA × Technology — {oa_m_sel.upper()}", height=520)
        st.plotly_chart(fig_oa_heat, use_container_width=True)
    st.markdown("---")

    # ── Worst Sites — Circle Wide ──────────────────────────────────────────
    st.subheader("🔴 Worst Sites — Circle Wide")
    wc1, wc2, wc3 = st.columns(3)
    with wc1:
        oa_worst_n = st.slider("Number of worst sites", 10, 100, 25, 5, key="oa_worst_n")
    with wc2:
        oa_worst_metric = st.selectbox("Rank by", ["Revenue (Lakhs)","4G Per-Day GB","3G Per-Day GB"],
                                        key="oa_worst_metric")
    with wc3:
        oa_worst_ssa = st.multiselect("Filter SSA", sorted(oa_df["SSA_Label"].dropna().unique()),
                                       default=sorted(oa_df["SSA_Label"].dropna().unique()),
                                       key="oa_worst_ssa")
    metric_col_map = {"Revenue (Lakhs)":"REV_LAKH","4G Per-Day GB":"Perday_4G_GB",
                      "3G Per-Day GB":"Perday_3G_GB"}
    worst_col = metric_col_map[oa_worst_metric]
    oa_worst_df = oa_df[oa_df["SSA_Label"].isin(oa_worst_ssa)].copy() if oa_worst_ssa else oa_df.copy()
    if worst_col not in oa_worst_df.columns:
        st.info(f"Column {worst_col} not available in this RBC file.")
    else:
        worst_sites = oa_worst_df.nsmallest(oa_worst_n, worst_col)
        worst_show = [c for c in ["BTSIPID","SITENAME","SSACODE","SSA_Label","SDCA",
                                   "REV_LAKH","2G_Cat","3G_Cat","4G_Cat",
                                   "Perday_4G_GB","Perday_3G_GB","Perday_2G_Erl",
                                   "LOCATION","2G TECH","3G TECH","4G TECH"] if c in worst_sites.columns]
        # Bar chart: worst sites
        st.plotly_chart(px.bar(worst_sites.sort_values(worst_col), x="BTSIPID", y=worst_col,
                               color="SSA_Label", title=f"Bottom {oa_worst_n} Sites by {oa_worst_metric}",
                               labels={"BTSIPID":"Site", worst_col:oa_worst_metric,"SSA_Label":"SSA"}),
                        use_container_width=True)
        st.dataframe(worst_sites[worst_show].reset_index(drop=True), use_container_width=True, hide_index=True)

    # Per-SSA worst sites expander
    with st.expander("📂 Per-SSA Worst 5 Sites"):
        for scode in sorted(oa_df["SSACODE"].dropna().unique()):
            sdf = oa_df[oa_df["SSACODE"]==scode]
            sname = SSA_DISPLAY.get(scode, scode)
            worst5 = sdf.nsmallest(5,"REV_LAKH")[["BTSIPID","SITENAME","SDCA","REV_LAKH","4G_Cat"]]
            st.markdown(f"**{sname} ({scode})** — worst 5 revenue sites")
            st.dataframe(worst5.reset_index(drop=True), use_container_width=True, hide_index=True)
    st.markdown("---")

    # ── Revenue Concentration & Distribution ──────────────────────────────
    st.subheader("📊 Revenue Distribution & Concentration")
    dist_c1, dist_c2 = st.columns(2)
    with dist_c1:
        # Circle-level revenue histogram
        st.plotly_chart(px.histogram(oa_df, x="REV_LAKH", color="SSA_Label", nbins=50,
                                     title="Revenue Distribution — All Sites (Circle)",
                                     labels={"REV_LAKH":"Revenue (Lakhs)","SSA_Label":"SSA"}),
                        use_container_width=True)
    with dist_c2:
        # 4G category distribution by SSA
        if "4G_Cat" in oa_df.columns:
            cat_oa = oa_df.groupby(["SSA_Label","4G_Cat"])["BTSIPID"].count().reset_index()
            cat_oa.columns = ["SSA","4G_Cat","Sites"]
            cat_oa = cat_oa[cat_oa["4G_Cat"].notna()]
            st.plotly_chart(px.bar(cat_oa, x="SSA", y="Sites", color="4G_Cat",
                                   barmode="stack", title="4G Traffic Category by SSA",
                                   color_discrete_map={"VHT":"#1a9641","HT":"#a6d96a",
                                                       "MT":"#ffffbf","LT":"#fdae61","VLT":"#d7191c"},
                                   category_orders={"4G_Cat":["VHT","HT","MT","LT","VLT"]}),
                            use_container_width=True)

    # Top-N sites concentration — what % of sites drive 80% revenue?
    oa_sorted = oa_df.sort_values("REV_LAKH", ascending=False).reset_index(drop=True)
    oa_sorted["Cumulative_Rev"] = oa_sorted["REV_LAKH"].cumsum()
    total_oa_rev = oa_sorted["REV_LAKH"].sum()
    oa_sorted["Cumulative_%"] = (oa_sorted["Cumulative_Rev"] / total_oa_rev * 100).round(2)
    sites_80 = int((oa_sorted["Cumulative_%"] <= 80).sum())
    pct_sites_80 = round(sites_80 / len(oa_sorted) * 100, 1)
    st.info(f"📌 **{sites_80} sites ({pct_sites_80}% of all sites)** generate **80%** of Circle revenue — "
            f"top {sites_80} sites average ₹{oa_sorted.head(sites_80)['REV_LAKH'].mean():.2f}L each.")
    st.plotly_chart(px.line(oa_sorted.head(min(500, len(oa_sorted))),
                            x=oa_sorted.head(min(500,len(oa_sorted))).index,
                            y="Cumulative_%", color="SSA_Label",
                            title="Cumulative Revenue % — Top 500 Sites by SSA",
                            labels={"x":"Site rank","Cumulative_%":"Cumulative Revenue %"}),
                    use_container_width=True)
    st.markdown("---")

    # ── Underperforming SSAs — Below Circle Average ────────────────────────
    st.subheader("⚠️ Underperforming SSAs — Below Circle Average")
    circle_avg_rev = oa_df["REV_LAKH"].mean()
    under_ssa = oa_ssa[oa_ssa["Avg_Rev_Lakh"] < circle_avg_rev].copy()
    if len(under_ssa):
        st.caption(f"Circle avg revenue/site = ₹{circle_avg_rev:.3f}L. "
                   f"{len(under_ssa)} SSAs are below this benchmark.")
        st.plotly_chart(px.bar(under_ssa.sort_values("vs_Circle_Avg_%"),
                               x="SSA_Name", y="vs_Circle_Avg_%",
                               color="vs_Circle_Avg_%", color_continuous_scale="Reds_r",
                               text="vs_Circle_Avg_%",
                               title="Underperforming SSAs — % Below Circle Avg Revenue/Site"),
                        use_container_width=True)

        # Revenue gap — how much revenue is lost vs if they performed at circle avg?
        under_ssa["Rev_Gap_Lakh"] = ((circle_avg_rev - under_ssa["Avg_Rev_Lakh"]) * under_ssa["Sites"]).round(2)
        st.markdown("**Revenue gap vs Circle average (uplift potential):**")
        st.dataframe(under_ssa[["SSA_Name","Sites","Avg_Rev_Lakh","vs_Circle_Avg_%","Rev_Gap_Lakh"]]
                     .sort_values("Rev_Gap_Lakh", ascending=False).reset_index(drop=True),
                     use_container_width=True, hide_index=True)
        total_gap = under_ssa["Rev_Gap_Lakh"].sum()
        st.warning(f"💡 If underperforming SSAs matched circle average, "
                   f"potential additional revenue = **₹{total_gap:.2f} Lakhs/month**")
    st.markdown("---")

    # ── Traffic vs Revenue by SSA ──────────────────────────────────────────
    st.subheader("📶 Traffic vs Revenue Analysis")
    if "TOT_TRAFFIC" in oa_ssa.columns:
        tr_sc = oa_ssa.copy()
        tr_sc["Avg_Traffic/Site"] = (tr_sc["TOT_TRAFFIC"] / tr_sc["Sites"]).round(1)
        st.plotly_chart(px.scatter(tr_sc, x="Avg_Traffic/Site", y="Avg_Rev_Lakh",
                                   size="Sites", color="SSA_Name",
                                   text="SSA_Name",
                                   title="Avg Traffic per Site vs Avg Revenue per Site — Circle SSAs",
                                   labels={"Avg_Traffic/Site":"Avg Traffic/Site",
                                           "Avg_Rev_Lakh":"Avg Revenue/Site (Lakhs)"},
                                   trendline="ols"),
                        use_container_width=True)
    st.markdown("---")

    # ── SSA Drill-down ─────────────────────────────────────────────────────
    st.subheader("🔍 SSA Drill-down — SDCA & Site Level")
    ssa_opts = sorted(oa_ssa["SSA_Name"].dropna().unique())
    ssa_sel_oa = st.selectbox("Select SSA", ssa_opts,
                               index=ssa_opts.index("Karaikudi") if "Karaikudi" in ssa_opts else 0,
                               key="oa_ssa_drill")
    sel_code_oa = oa_ssa[oa_ssa["SSA_Name"]==ssa_sel_oa]["SSACODE"].iloc[0]         if len(oa_ssa[oa_ssa["SSA_Name"]==ssa_sel_oa]) else None

    if sel_code_oa:
        ssa_detail = oa_df[oa_df["SSACODE"]==sel_code_oa].copy()
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("Sites",              ssa_detail["BTSIPID"].nunique())
        d2.metric("Total Revenue",      f"₹{ssa_detail['REV_LAKH'].sum():.2f}L")
        d3.metric("Avg Rev/Site",       f"₹{ssa_detail['REV_LAKH'].mean():.3f}L")
        d4.metric("Zero Rev Sites",     int((ssa_detail["REV_LAKH"]==0).sum()))

        if "SDCA" in ssa_detail.columns:
            sdca_oa = ssa_detail.groupby("SDCA").agg(
                Sites=("BTSIPID","nunique"),
                Total_Rev=("REV_LAKH","sum"),
                Avg_Rev=("REV_LAKH","mean"),
                Zero_Sites=("REV_LAKH", lambda x:(x==0).sum()),
            ).round(3).reset_index().sort_values("Total_Rev", ascending=False)
            da1, da2 = st.columns([1.4,1])
            with da1:
                st.plotly_chart(px.bar(sdca_oa, x="SDCA", y="Total_Rev",
                    color="Total_Rev", color_continuous_scale="Greens", text="Total_Rev",
                    title=f"{ssa_sel_oa} — Revenue by SDCA").update_traces(
                        texttemplate="₹%{text:.2f}L", textposition="outside"),
                    use_container_width=True)
            with da2:
                st.dataframe(sdca_oa, use_container_width=True, hide_index=True)

        show_oa_cols = [c for c in ["BTSIPID","SITENAME","SDCA","LOCATION",
                        "REV_LAKH","2G_Cat","3G_Cat","4G_Cat",
                        "Perday_2G_Erl","Perday_3G_GB","Perday_4G_GB"] if c in ssa_detail.columns]
        st.markdown(f"**{ssa_sel_oa} — {ssa_detail['BTSIPID'].nunique()} Sites**")
        st.dataframe(ssa_detail[show_oa_cols].sort_values("REV_LAKH", ascending=False)
                     .reset_index(drop=True).round(3), use_container_width=True, hide_index=True)
    st.markdown("---")

    # ── MoM Circle Trend ──────────────────────────────────────────────────
    if len(oa_rev_months_sorted) >= 2:
        st.subheader("📈 Circle Revenue Trend — Month on Month")
        trend_rows = []
        for m in oa_rev_months_sorted:
            mdf = rev_store_full[m]
            row = {"Month": m.upper(), "Total_Rev": round(mdf["REV_LAKH"].sum(),2),
                   "Sites": mdf["BTSIPID"].nunique()}
            for tech, rc in tech_rev_oa.items():
                if rc in mdf.columns:
                    row[f"{tech}_Rev"] = round(mdf[rc].sum()/100000, 2)
            trend_rows.append(row)
        trend_df = pd.DataFrame(trend_rows)
        st.plotly_chart(px.line(trend_df, x="Month", y="Total_Rev", markers=True,
                                text="Total_Rev", title="Circle Total Revenue Trend (Lakhs)"),
                        use_container_width=True)
        tech_trend_cols = [c for c in ["2G_Rev","3G_Rev","4G_Rev"] if c in trend_df.columns]
        if tech_trend_cols:
            tt_melt = trend_df.melt("Month", tech_trend_cols, var_name="Tech", value_name="Rev_Lakh")
            tt_melt["Tech"] = tt_melt["Tech"].str.replace("_Rev","")
            st.plotly_chart(px.line(tt_melt, x="Month", y="Rev_Lakh", color="Tech", markers=True,
                                    title="2G / 3G / 4G Revenue Trend — Circle",
                                    color_discrete_map={"2G":"#636EFA","3G":"#EF553B","4G":"#00CC96"}),
                            use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════
# TAB 12 – Circle Availability (OA-wise Intelligence)
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[11]:
    st.header("📶 Circle Availability — OA-wise Intelligence Report")
    st.caption(
        "Availability analysis across all uploaded performance months. "
        "OA (SSA) dimension is derived from the SSAID column in the perf file — "
        "no reference file needed. Upload perf files for multiple SSAs to enable cross-OA comparison."
    )

    # ── This tab uses ALL uploaded perf data — NOT the global OA filter ─────
    # The global sidebar OA filter (sel_ssacode) applies to all other tabs.
    # This tab has its OWN OA multiselect, so we start from the full master_df.
    # Filter only by month selection (sel_months) so the month filter still works.
    _av_df = st.session_state.master_df.copy()
    if sel_months:
        _av_df = _av_df[_av_df["Month_Label"].isin(sel_months)]
    # Map SSA_Code and SSA_Label from SSAID (always present in perf file)
    if "SSAID" in _av_df.columns:
        _av_df["SSA_Code"]  = _av_df["SSAID"].map(SSAID_TO_CODE).fillna(_av_df["SSAID"])
        _av_df["SSA_Label"] = _av_df["SSA_Code"].map(SSA_DISPLAY).fillna(_av_df["SSA_Code"])
    else:
        _av_df["SSA_Code"]  = sel_ssacode
        _av_df["SSA_Label"] = SSA_DISPLAY.get(sel_ssacode, sel_ssacode)
    # Normalise availability columns
    for _ac in ["Nw Avail (2G)", "Nw Avail (3G)", "Nw Avail (4G TCS)"]:
        if _ac in _av_df.columns:
            _av_df[_ac] = pd.to_numeric(_av_df[_ac], errors="coerce")

    AVAIL_TECH    = {"2G": "Nw Avail (2G)", "3G": "Nw Avail (3G)", "4G": "Nw Avail (4G TCS)"}
    avail_present = {t: c for t, c in AVAIL_TECH.items() if c in _av_df.columns}
    months_avail  = sorted(_av_df["Month_Label"].unique(), key=month_sort_key)
    latest_av_m   = months_avail[-1]
    _av_lat       = _av_df[_av_df["Month_Label"] == latest_av_m]

    if not avail_present:
        st.warning("No availability columns found in the uploaded performance files.")
    # ── Build OA list from perf SSAID column (full master_df) + RBC SSACODE ──
    # _perf_codes: every SSACODE found in the FULL uploaded perf data
    _perf_codes = set()
    if "SSAID" in _av_df.columns:
        for _sid in _av_df["SSAID"].dropna().unique():
            _sc = SSAID_TO_CODE.get(str(_sid).strip())
            if _sc:
                _perf_codes.add(_sc)
    # Also add any SSACODE directly in perf file (some files use SSACODE not SSAID)
    if "SSACODE" in _av_df.columns:
        _perf_codes.update(_av_df["SSACODE"].dropna().astype(str).str.strip().unique())
    _rbc_codes = set()
    if rev_store_full:
        for _rdf_av in rev_store_full.values():
            if "SSACODE" in _rdf_av.columns:
                _rbc_codes.update(_rdf_av["SSACODE"].dropna().astype(str).str.strip().unique())
    # All known OAs (union of perf + RBC). Sort by display name.
    all_codes_av = sorted(_perf_codes | _rbc_codes or {"KKD"},
                          key=lambda c: SSA_DISPLAY.get(c, c))
    if not all_codes_av:
        all_codes_av = list(SSA_DISPLAY.keys())

    # ── Controls row 1: sliders ────────────────────────────────────────────
    cc1, cc2, cc3 = st.columns(3)
    with cc1:
        av_thr     = st.slider("Poor availability threshold (%)", 80, 98, 90, 1, key="av_thr2")
    with cc2:
        av_worst_n = st.slider("Worst sites to show per OA", 5, 30, 10, 5, key="av_worst_n2")
    with cc3:
        av_rep_n   = st.slider("Chronic: bottom-N per month", 5, 30, 10, 5, key="av_rep_n2")

    # ── Controls row 2: OA multiselect with ALL checkbox ──────────────────
    oa_fc1, oa_fc2 = st.columns([5, 1])
    with oa_fc2:
        select_all_oa = st.checkbox("ALL", value=True, key="av_oa_all",
                                    help="Select all 17 OAs")
    with oa_fc1:
        # Options: "Karaikudi (KKD)" — code is embedded for exact matching
        av_opts = [f"{SSA_DISPLAY.get(c,c)}  ({c})" for c in all_codes_av]
        av_opts_perf_only = [f"{SSA_DISPLAY.get(c,c)}  ({c})"
                             for c in all_codes_av if c in _perf_codes]
        if select_all_oa:
            av_sel_opts = st.multiselect(
                "🔍 OA / SSA Filter — all 17 OAs listed; ⚙️ uncheck ALL to pick specific OAs",
                av_opts, default=av_opts, key="av_oa_sel", disabled=True)
            av_sel_opts = av_opts   # force all
        else:
            av_sel_opts = st.multiselect(
                "🔍 OA / SSA Filter — select one or more OAs",
                av_opts,
                default=av_opts_perf_only if av_opts_perf_only else av_opts[:1],
                key="av_oa_sel")
            if not av_sel_opts:
                st.warning("Please select at least one OA.")
                av_sel_opts = av_opts  # fall back to all

    # Extract SSACODE from option string e.g. "Karaikudi  (KKD)" → "KKD"
    import re as _re
    av_sel_codes = [_re.search(r'\((\w+)\)$', o.strip()).group(1)
                    for o in av_sel_opts if _re.search(r'\((\w+)\)\s*$', o)]
    av_codes_perf = [c for c in av_sel_codes if c in _perf_codes]
    av_codes_noperf = [c for c in av_sel_codes if c not in _perf_codes]

    # Filter _av_df to selected OAs that have perf data
    # Match via SSAID→SSACODE (TNKUM→CRDA, TNNAG→NGC)
    if "SSAID" in _av_df.columns:
        _av_df["_tmp_code"] = _av_df["SSAID"].map(SSAID_TO_CODE).fillna(_av_df["SSAID"])
        if av_codes_perf:
            _av_df = _av_df[_av_df["_tmp_code"].isin(av_codes_perf)].copy()
        _av_df.drop(columns=["_tmp_code"], inplace=True, errors="ignore")
    _av_lat = _av_df[_av_df["Month_Label"] == latest_av_m]

    # Set SSA_Label from SSACODE for display
    if "SSAID" in _av_df.columns:
        _av_df["SSA_Code"]  = _av_df["SSAID"].map(SSAID_TO_CODE).fillna(_av_df["SSAID"])
        _av_df["SSA_Label"] = _av_df["SSA_Code"].map(SSA_DISPLAY).fillna(_av_df["SSA_Code"])
        _av_lat["SSA_Code"]  = _av_lat["SSAID"].map(SSAID_TO_CODE).fillna(_av_lat["SSAID"])
        _av_lat["SSA_Label"] = _av_lat["SSA_Code"].map(SSA_DISPLAY).fillna(_av_lat["SSA_Code"])
    else:
        _av_df["SSA_Label"]  = SSA_DISPLAY.get(sel_ssacode, sel_ssacode)
        _av_lat["SSA_Label"] = SSA_DISPLAY.get(sel_ssacode, sel_ssacode)

    multi_oa = len(av_codes_perf) > 1
    av_oa_perf = [SSA_DISPLAY.get(c, c) for c in av_codes_perf]

    # Warnings / status
    if av_codes_noperf:
        st.warning(
            f"⚠️ **{len(av_codes_noperf)} OA(s) selected have no perf file uploaded:** "
            f"{', '.join(SSA_DISPLAY.get(c,c) for c in av_codes_noperf)}. "
            f"Upload their monthly performance files to include them here."
        )
    if not av_codes_perf:
        st.warning("No OAs with performance data selected. Showing all available OAs.")
        av_codes_perf = list(_perf_codes) if _perf_codes else []
    if av_codes_perf:
        st.info(
            f"📊 **{len(av_codes_perf)} OA(s) with perf data:** "
            f"{', '.join(SSA_DISPLAY.get(c,c)+' ('+c+')' for c in av_codes_perf)}"
            + (f"  |  Revenue data available for all 17 OAs in RBC tab." if _rbc_codes else "")
        )

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════
    # ══════════════════════════════════════════════════════════════════════
    # SECTION 0 – Node Count & Availability Full Summary
    # ══════════════════════════════════════════════════════════════════════
    st.subheader("📡 Node Count & Availability — Full Summary")
    st.caption("Physical sites = unique BTS IP ID. "
               "Radio nodes = 2G cnt + 3G cnt + 4G cnt (a site can have multiple). "
               "Technology presence = non-null BTS Site ID for that band.")

    # Helper: is BTS Site ID column populated (physical node present)?
    def _has_sid(df, col):
        if col not in df.columns:
            return pd.Series(False, index=df.index)
        s = df[col].astype(str).str.strip()
        return ~s.isin(["", "nan", "NaN", "None", "0", "<NA>", "nan"])

    # ── Per-month summary ──────────────────────────────────────────────────
    node_rows = []
    for _m in months_avail:
        _mdf = _av_df[_av_df["Month_Label"] == _m].copy()
        for _c in ["2G cnt","3G cnt","4G cnt","Total cnt"]:
            if _c in _mdf.columns:
                _mdf[_c] = pd.to_numeric(_mdf[_c], errors="coerce")

        _h2g  = _has_sid(_mdf, "BTS Site ID (2G)")
        _h3g  = _has_sid(_mdf, "BTS Site ID (3G)")
        _h700 = _has_sid(_mdf, "BTS Site ID (700)")
        _h2100= _has_sid(_mdf, "BTS Site ID (2100)")
        _h2500= _has_sid(_mdf, "BTS Site ID (2500)")
        _h4g  = _h700 | _h2100 | _h2500

        row = {"Month": _m.upper()}
        row["Physical Sites"]       = int(_mdf["BTS IP ID"].nunique())
        row["Total Radio Nodes"]    = int(_mdf["Total cnt"].sum()) if "Total cnt" in _mdf.columns else 0
        row["2G Nodes (cnt)"]       = int(_mdf["2G cnt"].sum())    if "2G cnt"    in _mdf.columns else 0
        row["3G Nodes (cnt)"]       = int(_mdf["3G cnt"].sum())    if "3G cnt"    in _mdf.columns else 0
        row["4G Nodes (cnt)"]       = int(_mdf["4G cnt"].sum())    if "4G cnt"    in _mdf.columns else 0
        row["2G Sites (Site ID)"]   = int(_h2g.sum())
        row["3G Sites (Site ID)"]   = int(_h3g.sum())
        row["4G Sites (any band)"]  = int(_h4g.sum())
        row["700 MHz Sites"]        = int(_h700.sum())
        row["2100 MHz Sites"]       = int(_h2100.sum())
        row["2500 MHz Sites"]       = int(_h2500.sum())
        row["2G+3G+4G Sites"]       = int((_h2g & _h3g & _h4g).sum())

        for _tech, _col_av in avail_present.items():
            _av = _mdf[_col_av].dropna()
            row[f"{_tech} Avg%"]    = round(_av.mean(), 2) if len(_av) else None
            row[f"{_tech} Min%"]    = round(_av.min(),  2) if len(_av) else None
            row[f"{_tech} Max%"]    = round(_av.max(),  2) if len(_av) else None
            row[f"{_tech} =100%"]   = int((_av == 100).sum())
            row[f"{_tech} ≥99%"]    = int((_av >= 99).sum())
            row[f"{_tech} ≥95%"]    = int((_av >= 95).sum())
            row[f"{_tech} ≥90%"]    = int((_av >= 90).sum())
            row[f"{_tech} <90%"]    = int((_av <  90).sum())
            row[f"{_tech} <80%"]    = int((_av <  80).sum())
            row[f"{_tech} <70%"]    = int((_av <  70).sum())
            _phys_col = "BTS Site ID (2G)" if _tech=="2G" else                         "BTS Site ID (3G)" if _tech=="3G" else None
            _phys_mask = _has_sid(_mdf, _phys_col) if _phys_col else _h4g
            row[f"{_tech} Nodes w/ Data"] = int(_phys_mask.sum())
            row[f"{_tech} No Avail Data"] = int(_phys_mask.sum() - len(_av))
        node_rows.append(row)

    node_df = pd.DataFrame(node_rows)

    # ── KPI strip for latest month ──────────────────────────────────────────
    _lat_row = node_df[node_df["Month"] == latest_av_m.upper()].iloc[0]         if len(node_df) else {}
    k0,k1,k2,k3,k4 = st.columns(5)
    k0.metric("Physical Sites",     _lat_row.get("Physical Sites","—"))
    k1.metric("Total Radio Nodes",  _lat_row.get("Total Radio Nodes","—"))
    k2.metric("2G Nodes",           _lat_row.get("2G Nodes (cnt)","—"))
    k3.metric("3G Nodes",           _lat_row.get("3G Nodes (cnt)","—"))
    k4.metric("4G Nodes",           _lat_row.get("4G Nodes (cnt)","—"))

    kk1,kk2,kk3,kk4,kk5 = st.columns(5)
    kk1.metric("2G Sites (physical)", _lat_row.get("2G Sites (Site ID)","—"))
    kk2.metric("3G Sites (physical)", _lat_row.get("3G Sites (Site ID)","—"))
    kk3.metric("4G Sites (any band)", _lat_row.get("4G Sites (any band)","—"))
    kk4.metric("2G+3G+4G Sites",      _lat_row.get("2G+3G+4G Sites","—"))
    kk5.metric("700 MHz / 2100 / 2500",
               f"{_lat_row.get('700 MHz Sites','—')} / "
               f"{_lat_row.get('2100 MHz Sites','—')} / "
               f"{_lat_row.get('2500 MHz Sites','—')}")

    st.markdown("---")

    # ── Full node + availability summary table ─────────────────────────────
    st.markdown("**📋 Month-wise Node Count & Availability Table**")
    # Show in two parts: node counts, then availability
    node_count_cols = ["Month","Physical Sites","Total Radio Nodes",
                       "2G Nodes (cnt)","3G Nodes (cnt)","4G Nodes (cnt)",
                       "2G Sites (Site ID)","3G Sites (Site ID)","4G Sites (any band)",
                       "700 MHz Sites","2100 MHz Sites","2500 MHz Sites","2G+3G+4G Sites"]
    avail_cols_table = ["Month"]
    for _tech in avail_present:
        avail_cols_table += [f"{_tech} Nodes w/ Data", f"{_tech} No Avail Data",
                             f"{_tech} Avg%", f"{_tech} Min%", f"{_tech} Max%",
                             f"{_tech} =100%", f"{_tech} ≥99%", f"{_tech} ≥95%",
                             f"{_tech} ≥90%", f"{_tech} <90%", f"{_tech} <80%",
                             f"{_tech} <70%"]
    nc_c, av_c = st.columns(2)
    with nc_c:
        st.markdown("**Node Counts**")
        _nc = node_df[[c for c in node_count_cols if c in node_df.columns]]
        st.dataframe(_nc.reset_index(drop=True), use_container_width=True, hide_index=True)
    with av_c:
        st.markdown("**Availability Buckets**")
        _ac = node_df[[c for c in avail_cols_table if c in node_df.columns]]
        def _bucket_colour(v):
            try:
                fv = float(v)
                if fv < 90:  return "background-color:#f8d7da;color:#721c24"
                if fv < 95:  return "background-color:#fff3cd;color:#856404"
                return "background-color:#d4edda;color:#155724"
            except: return ""
        avg_pct_cols = [f"{t} Avg%" for t in avail_present]
        st.dataframe(safe_style(_ac.reset_index(drop=True), _bucket_colour, avg_pct_cols),
                     use_container_width=True, hide_index=True)

    st.markdown("---")

    # ── Stacked node composition bar ──────────────────────────────────────
    st.markdown("**📊 Radio Node Composition per Month**")
    _bar_data = []
    for _, _r in node_df.iterrows():
        for _tech, _col in [("2G","2G Nodes (cnt)"),("3G","3G Nodes (cnt)"),("4G","4G Nodes (cnt)")]:
            if _col in _r:
                _bar_data.append({"Month":_r["Month"],"Technology":_tech,"Nodes":_r[_col]})
    if _bar_data:
        _bar_df = pd.DataFrame(_bar_data)
        fig_nodes = px.bar(_bar_df, x="Month", y="Nodes", color="Technology",
                           barmode="stack", text="Nodes",
                           title="Radio Nodes by Technology per Month",
                           color_discrete_map={"2G":"#636EFA","3G":"#EF553B","4G":"#00CC96"})
        fig_nodes.update_traces(textposition="inside")
        st.plotly_chart(fig_nodes, use_container_width=True)

    # ── Technology combo donut (latest month) ─────────────────────────────
    _mdf_lat = _av_df[_av_df["Month_Label"] == latest_av_m].copy()
    _h2g_l  = _has_sid(_mdf_lat, "BTS Site ID (2G)")
    _h3g_l  = _has_sid(_mdf_lat, "BTS Site ID (3G)")
    _h700_l = _has_sid(_mdf_lat, "BTS Site ID (700)")
    _h2100_l= _has_sid(_mdf_lat, "BTS Site ID (2100)")
    _h2500_l= _has_sid(_mdf_lat, "BTS Site ID (2500)")
    _h4g_l  = _h700_l | _h2100_l | _h2500_l
    combo_counts = {
        "2G + 3G + 4G": int((_h2g_l & _h3g_l & _h4g_l).sum()),
        "2G + 4G only": int((_h2g_l & ~_h3g_l & _h4g_l).sum()),
        "2G + 3G only": int((_h2g_l & _h3g_l & ~_h4g_l).sum()),
        "4G only":      int((~_h2g_l & ~_h3g_l & _h4g_l).sum()),
        "2G only":      int((_h2g_l & ~_h3g_l & ~_h4g_l).sum()),
        "3G only":      int((~_h2g_l & _h3g_l & ~_h4g_l).sum()),
        "No tech data": int((~_h2g_l & ~_h3g_l & ~_h4g_l).sum()),
    }
    combo_df = pd.DataFrame(list(combo_counts.items()),
                            columns=["Technology Mix","Sites"])
    combo_df = combo_df[combo_df["Sites"] > 0]

    _dc1, _dc2 = st.columns(2)
    with _dc1:
        fig_combo = px.pie(combo_df, names="Technology Mix", values="Sites",
                           hole=0.4,
                           title=f"Technology Mix per Physical Site — {latest_av_m.upper()}",
                           color_discrete_sequence=px.colors.qualitative.Set2)
        st.plotly_chart(fig_combo, use_container_width=True)
    with _dc2:
        st.markdown(f"**Site Technology Breakdown — {latest_av_m.upper()}**")
        st.dataframe(combo_df.reset_index(drop=True), use_container_width=True, hide_index=True)

    # ── 4G Band-wise node + availability breakdown ─────────────────────────
    if "4G" in avail_present:
        st.markdown("**📡 4G Band-wise Breakdown**")
        _col4g = avail_present["4G"]
        band_def = {
            "A — 700 only":       _h700_l & ~_h2100_l & ~_h2500_l,
            "B — 700 + 2100":     _h700_l & _h2100_l  & ~_h2500_l,
            "D — 700+2100+2500":  _h700_l & _h2100_l  & _h2500_l,
            "2100 only":          ~_h700_l & _h2100_l & ~_h2500_l,
            "2500 only":          ~_h700_l & ~_h2100_l & _h2500_l,
        }
        band_rows = []
        for _label, _mask in band_def.items():
            if _mask.sum() == 0: continue
            _avail = _mdf_lat.loc[_mask, _col4g].dropna()
            band_rows.append({
                "Band": _label,
                "Sites": int(_mask.sum()),
                "Avg Avail%": round(_avail.mean(), 2) if len(_avail) else None,
                "Min%": round(_avail.min(), 2) if len(_avail) else None,
                "≥95%": int((_avail >= 95).sum()),
                "<90%": int((_avail <  90).sum()),
                "<70%": int((_avail <  70).sum()),
            })
        if band_rows:
            band_df = pd.DataFrame(band_rows)
            _bd1, _bd2 = st.columns([1, 1.4])
            with _bd1:
                st.dataframe(band_df.reset_index(drop=True),
                             use_container_width=True, hide_index=True)
            with _bd2:
                fig_band2 = px.bar(band_df, x="Band", y="Avg Avail%",
                                   color="Avg Avail%",
                                   color_continuous_scale=[[0,"#d7191c"],[0.5,"#fdae61"],[1,"#1a9641"]],
                                   range_color=[80,100], text="Avg Avail%",
                                   title=f"4G Avg Availability by Band — {latest_av_m.upper()}")
                fig_band2.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
                fig_band2.add_hline(y=av_thr, line_dash="dash", line_color="red")
                fig_band2.update_layout(coloraxis_showscale=False, yaxis_range=[70, 105])
                st.plotly_chart(fig_band2, use_container_width=True)

    # ── OA-wise node + availability summary table ─────────────────────────
    st.markdown(f"**🗺️ OA-wise Node & Availability Summary — {latest_av_m.upper()}**")
    oa_node_rows = []
    for _oa in sorted(_av_lat["SSA_Label"].dropna().unique()):
        _odf = _av_lat[_av_lat["SSA_Label"] == _oa].copy()
        for _c in ["2G cnt","3G cnt","4G cnt","Total cnt"]:
            if _c in _odf.columns:
                _odf[_c] = pd.to_numeric(_odf[_c], errors="coerce")
        _oh2g = _has_sid(_odf,"BTS Site ID (2G)")
        _oh3g = _has_sid(_odf,"BTS Site ID (3G)")
        _oh4g = (_has_sid(_odf,"BTS Site ID (700)") |
                 _has_sid(_odf,"BTS Site ID (2100)") |
                 _has_sid(_odf,"BTS Site ID (2500)"))
        _orow = {"OA": _oa,
                 "Sites": int(_odf["BTS IP ID"].nunique()),
                 "Total Nodes": int(_odf["Total cnt"].sum()) if "Total cnt" in _odf.columns else 0,
                 "2G": int(_odf["2G cnt"].sum()) if "2G cnt" in _odf.columns else 0,
                 "3G": int(_odf["3G cnt"].sum()) if "3G cnt" in _odf.columns else 0,
                 "4G": int(_odf["4G cnt"].sum()) if "4G cnt" in _odf.columns else 0,
                 "2G+3G+4G": int((_oh2g & _oh3g & _oh4g).sum())}
        for _tech, _col_av in avail_present.items():
            _av = _odf[_col_av].dropna()
            _orow[f"{_tech} Avg%"] = round(_av.mean(), 2) if len(_av) else None
            _orow[f"{_tech} <90%"] = int((_av < 90).sum()) if len(_av) else 0
            _orow[f"{_tech} <70%"] = int((_av < 70).sum()) if len(_av) else 0
        oa_node_rows.append(_orow)

    if oa_node_rows:
        oa_node_df = pd.DataFrame(oa_node_rows)
        avg_oa_cols = [f"{t} Avg%" for t in avail_present if f"{t} Avg%" in oa_node_df.columns]
        st.dataframe(safe_style(oa_node_df.reset_index(drop=True), _bucket_colour, avg_oa_cols),
                     use_container_width=True, hide_index=True)

    st.markdown("---")

    # SECTION 1 – OA × Technology Summary Table & KPI Cards
    # ══════════════════════════════════════════════════════════════════════
    st.subheader("📊 OA × Technology Availability Summary")

    # Build summary: OA × Tech, latest month
    oa_tech_rows = []
    for oa in sorted(_av_df["SSA_Label"].dropna().unique()):
        row = {"OA": oa}
        for tech, col in avail_present.items():
            sub = _av_lat[_av_lat["SSA_Label"] == oa][col].dropna()
            row[f"{tech} Avg%"]      = round(sub.mean(), 2) if len(sub) else None
            row[f"{tech} <{av_thr}%"] = int((sub < av_thr).sum()) if len(sub) else 0
            row[f"{tech} Sites"]     = int(len(sub))
        oa_tech_rows.append(row)
    oa_tech_df = pd.DataFrame(oa_tech_rows)

    # KPI cards — one per tech
    kpi_c = st.columns(len(avail_present))
    for ki, (tech, col) in enumerate(avail_present.items()):
        s = _av_lat[col].dropna()
        kpi_c[ki].metric(
            f"{tech} Circle Avg",
            f"{s.mean():.2f}%" if len(s) else "N/A",
            delta=f"{int((s < av_thr).sum())} sites <{av_thr}%"
        )

    # OA × Tech table with colour
    def _av_cell_colour(v):
        try:
            fv = float(v)
            if fv < 90:  return "background-color:#f8d7da;color:#721c24"
            if fv < 95:  return "background-color:#fff3cd;color:#856404"
            return "background-color:#d4edda;color:#155724"
        except: return ""

    avg_cols = [c for c in oa_tech_df.columns if "Avg%" in c]
    st.dataframe(
        safe_style(oa_tech_df.reset_index(drop=True), _av_cell_colour, avg_cols),
        use_container_width=True, hide_index=True
    )

    # OA comparison bar (multi-OA)
    if multi_oa and avg_cols:
        oa_melt = oa_tech_df.melt("OA", avg_cols, var_name="Technology", value_name="Avg Avail %")
        oa_melt["Technology"] = oa_melt["Technology"].str.replace(" Avg%","")
        fig_oa_bar = px.bar(
            oa_melt, x="OA", y="Avg Avail %", color="Technology",
            barmode="group", text="Avg Avail %",
            title=f"OA-wise Availability by Technology — {latest_av_m.upper()}",
            color_discrete_map={"2G":"#636EFA","3G":"#EF553B","4G":"#00CC96"}
        )
        fig_oa_bar.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_oa_bar.add_hline(y=av_thr, line_dash="dash", line_color="red",
                             annotation_text=f"{av_thr}%")
        fig_oa_bar.update_layout(yaxis_range=[60,105])
        st.plotly_chart(fig_oa_bar, use_container_width=True)

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 2 – OA × Technology × Month Heatmaps
    # ══════════════════════════════════════════════════════════════════════
    st.subheader("🗺️ OA × Technology Availability Heatmap (Month-on-Month)")
    ht1, ht2, ht3 = st.tabs(list(avail_present.keys()))
    for htab, (tech, col) in zip([ht1, ht2, ht3], avail_present.items()):
        with htab:
            pivot = (_av_df.groupby(["SSA_Label","Month_Label"])[col]
                     .mean().round(2).reset_index()
                     .pivot(index="SSA_Label", columns="Month_Label", values=col))
            pivot.columns = [c.upper() for c in pivot.columns]
            if len(pivot) == 0:
                st.info("No data."); continue
            fig_hm = go.Figure(data=go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale=[[0,"#d7191c"],[0.45,"#fdae61"],[0.6,"#ffffbf"],[1,"#1a9641"]],
                zmin=70, zmax=100,
                text=[[f"{v:.1f}%" if pd.notna(v) else "N/A" for v in row]
                      for row in pivot.values.tolist()],
                texttemplate="%{text}", hoverongaps=False,
            ))
            fig_hm.update_layout(
                title=f"{tech} Availability % — OA × Month",
                height=max(350, len(pivot)*55 + 120)
            )
            st.plotly_chart(fig_hm, use_container_width=True)

            # MoM trend lines
            if len(months_avail) >= 2:
                mom_data = (_av_df.groupby(["SSA_Label","Month_Label"])[col]
                            .mean().round(2).reset_index())
                mom_data["Month_Label"] = mom_data["Month_Label"].str.upper()
                fig_line = px.line(
                    mom_data, x="Month_Label", y=col,
                    color="SSA_Label", markers=True,
                    title=f"{tech} Availability Trend by OA",
                    labels={col:"Avg Avail %","Month_Label":"Month","SSA_Label":"OA"}
                )
                fig_line.add_hline(y=av_thr, line_dash="dash", line_color="red",
                                   annotation_text=f"{av_thr}%")
                st.plotly_chart(fig_line, use_container_width=True)

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 3 – Worst Performers per OA per Technology
    # ══════════════════════════════════════════════════════════════════════
    st.subheader(f"🔴 Worst {av_worst_n} Sites per OA — Technology-wise ({latest_av_m.upper()})")
    for tech, col in avail_present.items():
        st.markdown(f"#### {tech}")
        for oa in sorted(_av_df["SSA_Label"].dropna().unique()):
            sub = _av_lat[_av_lat["SSA_Label"] == oa][
                ["BTS IP ID","BTS Name","SSA_Label", col]
            ].dropna(subset=[col])
            if len(sub) == 0: continue
            worst = sub.nsmallest(av_worst_n, col).reset_index(drop=True)
            oa_avg = sub[col].mean()
            below  = (sub[col] < av_thr).sum()

            with st.expander(
                f"**{oa}** — {len(sub)} sites · Avg {oa_avg:.1f}% · "
                f"**{below} sites <{av_thr}%** {'⚠️' if below>0 else '✅'}",
                expanded=(below > 0)
            ):
                fig_w = px.bar(
                    worst, x="BTS IP ID", y=col,
                    color=col,
                    color_continuous_scale=[[0,"#d7191c"],[0.4,"#fdae61"],[1,"#1a9641"]],
                    range_color=[40,100],
                    text=col, hover_data=["BTS Name"],
                    title=f"Worst {av_worst_n} {tech} — {oa}"
                )
                fig_w.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
                fig_w.add_hline(y=av_thr, line_dash="dash", line_color="red")
                fig_w.update_layout(coloraxis_showscale=False, yaxis_range=[0,105])
                st.plotly_chart(fig_w, use_container_width=True)
                st.dataframe(worst, use_container_width=True, hide_index=True)

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 4 – Chronic Poor Performers per OA (all months)
    # ══════════════════════════════════════════════════════════════════════
    st.subheader(f"🔁 Chronic Poor Performers — Repeated Worst across All Months")
    if len(months_avail) < 2:
        st.info("Upload ≥ 2 months of performance data to identify chronic offenders.")
    else:
        for tech, col in avail_present.items():
            st.markdown(f"#### {tech} — Chronic Offenders")
            any_chronic = False
            for oa in sorted(_av_df["SSA_Label"].dropna().unique()):
                sets = []
                for m in months_avail:
                    sub = _av_df[
                        (_av_df["Month_Label"]==m) & (_av_df["SSA_Label"]==oa)
                    ][["BTS IP ID", col]].dropna(subset=[col])
                    # Only sites ACTUALLY below threshold qualify as poor performers
                    # Without this filter, nsmallest(N) returns N rows even if all are 100%
                    sub_poor = sub[sub[col] < av_thr]
                    if len(sub_poor) >= av_rep_n:
                        sets.append(set(sub_poor.nsmallest(av_rep_n, col)["BTS IP ID"]))
                    elif len(sub_poor) > 0:
                        # Fewer than N poor sites — use all of them
                        sets.append(set(sub_poor["BTS IP ID"]))
                    else:
                        # No poor sites this month for this OA — cannot be chronic
                        sets.append(set())
                if len(sets) < len(months_avail): continue
                # Must have appeared in poor list in EVERY month
                chronic_ids = set.intersection(*sets)
                if not chronic_ids: continue
                any_chronic = True

                rows = []
                for sid in sorted(chronic_ids):
                    row = {"BTS IP ID": sid}
                    sname = _av_df[_av_df["BTS IP ID"]==sid]["BTS Name"].iloc[0] \
                        if sid in _av_df["BTS IP ID"].values else ""
                    row["BTS Name"] = sname
                    row["OA"]       = oa
                    for m in months_avail:
                        v = _av_df[
                            (_av_df["BTS IP ID"]==sid) & (_av_df["Month_Label"]==m)
                        ][col]
                        row[m.upper()] = round(v.values[0], 2) if len(v) else None
                    row["Avg All"] = round(
                        _av_df[_av_df["BTS IP ID"]==sid][col].mean(), 2)
                    rows.append(row)
                chronic_df = pd.DataFrame(rows).sort_values("Avg All")
                mcols = [m.upper() for m in months_avail]

                st.warning(
                    f"⚠️ **{oa}** — **{len(chronic_ids)} sites** appear in the worst "
                    f"{av_rep_n} {tech} sites in every month ({', '.join(m.upper() for m in months_avail)})"
                )
                st.dataframe(
                    safe_style(chronic_df.reset_index(drop=True), _av_cell_colour,
                               mcols + ["Avg All"]),
                    use_container_width=True, hide_index=True
                )
                fig_ch = px.bar(
                    chronic_df, x="BTS IP ID", y="Avg All",
                    color="Avg All",
                    color_continuous_scale=[[0,"#d7191c"],[0.5,"#fdae61"],[1,"#a6d96a"]],
                    range_color=[40,95], text="Avg All",
                    hover_data=["BTS Name","OA"],
                    title=f"{tech} Chronic Offenders — {oa} (avg across {len(months_avail)} months)"
                )
                fig_ch.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
                fig_ch.update_layout(coloraxis_showscale=False, yaxis_range=[0,100])
                st.plotly_chart(fig_ch, use_container_width=True)

            if not any_chronic:
                st.success(f"✅ No chronic {tech} offenders found across {', '.join(m.upper() for m in months_avail)}.")

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 5 – Cross-OA Comparison (multi-OA only)
    # ══════════════════════════════════════════════════════════════════════
    if multi_oa:
        st.subheader("🆚 Cross-OA Availability Ranking")
        for tech, col in avail_present.items():
            rank_rows = []
            for oa in sorted(_av_df["SSA_Label"].dropna().unique()):
                sub = _av_lat[_av_lat["SSA_Label"] == oa][col].dropna()
                if len(sub) == 0: continue
                rank_rows.append({
                    "OA": oa, "Sites": len(sub),
                    "Avg Avail%": round(sub.mean(),2),
                    f"<{av_thr}% Sites": int((sub<av_thr).sum()),
                    "<90% Sites": int((sub<90).sum()),
                    "<95% Sites": int((sub<95).sum()),
                    "Min%": round(sub.min(),2),
                    "Worst Site": sub.idxmin() if hasattr(sub,'idxmin') else ""
                })
            if not rank_rows: continue
            rank_df = pd.DataFrame(rank_rows).sort_values("Avg Avail%")
            st.markdown(f"**{tech} OA Ranking (worst → best)**")
            st.dataframe(
                safe_style(rank_df.reset_index(drop=True), _av_cell_colour, ["Avg Avail%"]),
                use_container_width=True, hide_index=True
            )
            st.plotly_chart(
                px.bar(rank_df.sort_values("Avg Avail%"),
                       x="OA", y="Avg Avail%",
                       color="Avg Avail%",
                       color_continuous_scale=[[0,"#d7191c"],[0.5,"#fdae61"],[1,"#1a9641"]],
                       range_color=[85,100], text="Avg Avail%",
                       title=f"{tech} OA Ranking — {latest_av_m.upper()}"
                       ).update_traces(texttemplate="%{text:.1f}%", textposition="outside"),
                use_container_width=True
            )
        st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 6 – Vendor-wise Availability per OA
    # ══════════════════════════════════════════════════════════════════════
    st.subheader("🏭 Vendor-wise Availability")
    vendor_map_av = {"2G": "Vendor 2g", "3G": "Vendor 3g"}
    for tech, col in avail_present.items():
        ven_col = vendor_map_av.get(tech)
        if not ven_col or ven_col not in _av_df.columns: continue
        vd = (_av_df.groupby([ven_col, "SSA_Label", "Month_Label"])[col]
              .mean().round(2).reset_index())
        vd["Month_Label"] = vd["Month_Label"].str.upper()
        fig_vd = px.bar(
            vd, x=ven_col, y=col, color="Month_Label",
            facet_col="SSA_Label" if multi_oa else None,
            barmode="group", text=col,
            title=f"{tech} Availability by Vendor" + (" & OA" if multi_oa else ""),
            labels={col:"Avg Avail %"}
        )
        fig_vd.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_vd.add_hline(y=av_thr, line_dash="dash", line_color="red")
        fig_vd.update_layout(yaxis_range=[75,102])
        st.plotly_chart(fig_vd, use_container_width=True)

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 7 – Urban vs Rural per OA
    # ══════════════════════════════════════════════════════════════════════
    if "BTS Area" in _av_df.columns:
        st.subheader("🌆 Urban vs Rural Availability by OA")
        for tech, col in avail_present.items():
            ur = (_av_df.groupby(["SSA_Label","BTS Area","Month_Label"])[col]
                  .mean().round(2).reset_index())
            ur["Month_Label"] = ur["Month_Label"].str.upper()
            ur["Group"] = ur["SSA_Label"] + " · " + ur["BTS Area"]
            fig_ur = px.bar(
                ur, x="Month_Label", y=col, color="BTS Area",
                facet_col="SSA_Label" if multi_oa else None,
                barmode="group", text=col,
                title=f"{tech} Urban vs Rural — by OA",
                color_discrete_map={"Urban":"#636EFA","Rural":"#EF553B"},
                labels={col:"Avg Avail %"}
            )
            fig_ur.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig_ur.add_hline(y=av_thr, line_dash="dash", line_color="red")
            fig_ur.update_layout(yaxis_range=[75,102])
            st.plotly_chart(fig_ur, use_container_width=True)
        st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 8 – Band-wise Availability (4G)
    # ══════════════════════════════════════════════════════════════════════
    if "Band category" in _av_df.columns and "4G" in avail_present:
        st.subheader("📡 4G Band-wise Availability by OA")
        col4g = avail_present["4G"]
        bd = (_av_df.groupby(["SSA_Label","Band category","Month_Label"])[col4g]
              .agg(Avg="mean", Sites="count").round(2).reset_index())
        bd["Month_Label"] = bd["Month_Label"].str.upper()
        fig_bd = px.bar(
            bd, x="Band category", y="Avg", color="Month_Label",
            facet_col="SSA_Label" if multi_oa else None,
            barmode="group", text="Avg",
            title="4G Availability by Band Category & OA",
            hover_data=["Sites"], labels={"Avg":"Avg 4G Avail %"}
        )
        fig_bd.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_bd.add_hline(y=av_thr, line_dash="dash", line_color="red")
        fig_bd.update_layout(yaxis_range=[75,102])
        st.plotly_chart(fig_bd, use_container_width=True)
        st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 9 – Multi-Technology Poor Sites per OA
    # ══════════════════════════════════════════════════════════════════════
    st.subheader(f"⚠️ Multi-Technology Poor Sites per OA (<{av_thr}% in 2+ technologies)")
    for oa in sorted(_av_df["SSA_Label"].dropna().unique()):
        sub = _av_lat[_av_lat["SSA_Label"] == oa].copy()
        flags  = {}
        for tech, col in avail_present.items():
            if col in sub.columns:
                sub[f"Poor_{tech}"] = sub[col] < av_thr
                flags[tech] = f"Poor_{tech}"
        if flags:
            sub["Poor_Count"] = sub[list(flags.values())].sum(axis=1)
            multi = sub[sub["Poor_Count"] >= 2]
            show_c = ["BTS IP ID","BTS Name","Poor_Count"] + list(avail_present.values())
            show_c = [c for c in show_c if c in multi.columns]
            if len(multi):
                st.warning(f"⚠️ **{oa}** — {len(multi)} sites below {av_thr}% in 2+ technologies")
                st.dataframe(multi[show_c].sort_values("Poor_Count",ascending=False)
                             .reset_index(drop=True).round(2),
                             use_container_width=True, hide_index=True)
            else:
                st.success(f"✅ **{oa}** — No multi-technology poor sites.")

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 10 – MoM Deterioration per OA
    # ══════════════════════════════════════════════════════════════════════
    if len(months_avail) >= 2:
        st.subheader("📉 MoM Availability Change per OA")
        mom_tech_sel = st.selectbox("Technology", list(avail_present.keys()), key="av_mom2")
        mom_col  = avail_present[mom_tech_sel]
        m_p, m_c = months_avail[-2], months_avail[-1]

        for oa in sorted(_av_df["SSA_Label"].dropna().unique()):
            oa_sub = _av_df[_av_df["SSA_Label"] == oa]
            prev   = oa_sub[oa_sub["Month_Label"]==m_p].set_index("BTS IP ID")[mom_col]
            curr   = oa_sub[oa_sub["Month_Label"]==m_c].set_index("BTS IP ID")[mom_col]
            chg    = (curr - prev).dropna().reset_index()
            chg.columns = ["BTS IP ID","Change%"]
            chg = chg.merge(
                _av_lat[["BTS IP ID","BTS Name"]].drop_duplicates(), on="BTS IP ID", how="left")
            if len(chg) == 0: continue

            with st.expander(
                f"**{oa}** — {mom_tech_sel} change {m_p.upper()}→{m_c.upper()} "
                f"| Deteriorated: {(chg['Change%']<0).sum()} | Improved: {(chg['Change%']>0).sum()}",
                expanded=False
            ):
                dc1, dc2 = st.columns(2)
                with dc1:
                    st.markdown(f"**🔴 Top 10 Deteriorated**")
                    st.dataframe(chg.nsmallest(10,"Change%").reset_index(drop=True),
                                 use_container_width=True, hide_index=True)
                with dc2:
                    st.markdown(f"**🟢 Top 10 Improved**")
                    st.dataframe(chg.nlargest(10,"Change%").reset_index(drop=True),
                                 use_container_width=True, hide_index=True)
                fig_chg = px.histogram(chg, x="Change%", nbins=25,
                                       title=f"{mom_tech_sel} Avail Change — {oa}",
                                       color_discrete_sequence=["#636EFA"])
                fig_chg.add_vline(x=0, line_dash="solid", line_color="black")
                st.plotly_chart(fig_chg, use_container_width=True)

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 11 – Availability Distribution Buckets per OA
    # ══════════════════════════════════════════════════════════════════════
    st.subheader("📊 Availability Distribution Buckets per OA")
    bk_tech = st.selectbox("Technology", list(avail_present.keys()), key="av_bucket2")
    bk_col  = avail_present[bk_tech]

    bucket_rows = []
    for oa in sorted(_av_df["SSA_Label"].dropna().unique()):
        s = _av_lat[_av_lat["SSA_Label"]==oa][bk_col].dropna()
        if len(s) == 0: continue
        bucket_rows.append({
            "OA": oa, "Total": len(s),
            "Critical (<70%)":    int((s<70).sum()),
            "Very Poor (70-80%)": int(((s>=70)&(s<80)).sum()),
            "Poor (80-90%)":      int(((s>=80)&(s<90)).sum()),
            "Fair (90-95%)":      int(((s>=90)&(s<95)).sum()),
            "Good (95-99%)":      int(((s>=95)&(s<99)).sum()),
            "Excellent (≥99%)":   int((s>=99).sum()),
        })
    if bucket_rows:
        bk_df = pd.DataFrame(bucket_rows)
        bk_cat = ["Critical (<70%)","Very Poor (70-80%)","Poor (80-90%)",
                  "Fair (90-95%)","Good (95-99%)","Excellent (≥99%)"]
        bk_melt = bk_df.melt("OA", bk_cat, var_name="Bucket", value_name="Sites")
        fig_bk = px.bar(
            bk_melt, x="OA", y="Sites", color="Bucket",
            barmode="stack", title=f"{bk_tech} Availability Buckets by OA — {latest_av_m.upper()}",
            color_discrete_map={
                "Critical (<70%)":"#d7191c","Very Poor (70-80%)":"#f46d43",
                "Poor (80-90%)":"#fdae61","Fair (90-95%)":"#e0f3f8",
                "Good (95-99%)":"#a6d96a","Excellent (≥99%)":"#1a9641"},
            category_orders={"Bucket": bk_cat}
        )
        st.plotly_chart(fig_bk, use_container_width=True)
        st.dataframe(bk_df, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 13 – Period Summary Report
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[12]:
    st.header("📊 Period Summary Report")
    st.caption(
        "Select the performance months and revenue months you want to analyse. "
        "Availability and revenue are computed only for the selected periods. "
        "SDCA is pulled from the latest uploaded RBC file."
    )

    # ── Month selectors ────────────────────────────────────────────────────────
    all_perf_months = sorted(st.session_state.master_df["Month_Label"].unique(),
                             key=month_sort_key) if st.session_state.master_df is not None else []
    all_rev_months  = sorted(st.session_state.rev_df.keys(), key=month_sort_key)                       if st.session_state.rev_df else []

    ps_c1, ps_c2 = st.columns(2)
    with ps_c1:
        ps_perf_sel = st.multiselect(
            "📅 Select Performance Month(s)",
            all_perf_months,
            default=all_perf_months[-2:] if len(all_perf_months) >= 2 else all_perf_months,
            key="ps_perf_months"
        )
    with ps_c2:
        ps_rev_sel = st.multiselect(
            "💰 Select Revenue Month(s)",
            all_rev_months,
            default=all_rev_months[-2:] if len(all_rev_months) >= 2 else all_rev_months,
            key="ps_rev_months"
        )

    if not ps_perf_sel and not ps_rev_sel:
        st.warning("Please select at least one performance month or revenue month.")
    else:
      if True:
        # ── Filtered perf data ─────────────────────────────────────────────────
        ps_perf_df = pd.DataFrame()
        if ps_perf_sel and st.session_state.master_df is not None:
            ps_perf_df = st.session_state.master_df[
                st.session_state.master_df["Month_Label"].isin(ps_perf_sel)
            ].copy()
            # Remap SDCA from latest RBC (most authoritative)
            if rev_store:
                _ps_sdca_src = rev_store[sorted(rev_store.keys(), key=month_sort_key)[-1]]
                if "SDCANAME" in _ps_sdca_src.columns:
                    _ps_lkp = (
                        _ps_sdca_src[["BTSIPID","SDCANAME"]]
                        .dropna(subset=["SDCANAME"]).drop_duplicates("BTSIPID")
                        .set_index("BTSIPID")["SDCANAME"]
                        .str.strip().str.title()
                        .str.replace("Tirupathur","Tirupattur",regex=False)
                    )
                    ps_perf_df["SDCA"] = ps_perf_df["BTS IP ID"].map(_ps_lkp).fillna(
                        ps_perf_df["SDCA"].str.strip().str.title()
                        if "SDCA" in ps_perf_df.columns
                        else pd.Series("Unknown", index=ps_perf_df.index)
                    ).fillna("Unknown")

        # ── Filtered revenue data ──────────────────────────────────────────────
        ps_rev_dfs = []
        if ps_rev_sel:
            for _m in ps_rev_sel:
                if _m in rev_store:
                    _rdf = rev_store[_m].copy()
                    _rdf["Rev_Month"] = _m
                    if "SDCANAME" in _rdf.columns:
                        _rdf["SDCA"] = (_rdf["SDCANAME"].str.strip().str.title()
                                        .str.replace("Tirupathur","Tirupattur",regex=False))
                    ps_rev_dfs.append(_rdf)
        ps_rev_df = pd.concat(ps_rev_dfs, ignore_index=True) if ps_rev_dfs else pd.DataFrame()

        st.markdown("---")

        # ══════════════════════════════════════════════════════════════════════
        # SECTION 1 – Availability Summary
        # ══════════════════════════════════════════════════════════════════════
        if not ps_perf_df.empty:
            st.subheader(f"📶 Availability Summary — {', '.join(m.upper() for m in ps_perf_sel)}")

            AVAIL_MAP_PS = {"2G":"Nw Avail (2G)","3G":"Nw Avail (3G)","4G TCS":"Nw Avail (4G TCS)"}
            avail_ps = {t:c for t,c in AVAIL_MAP_PS.items() if c in ps_perf_df.columns}

            # Overall KPI cards
            kpi_cols_ps = st.columns(len(avail_ps) * 2)
            ki = 0
            for tech, col in avail_ps.items():
                s = ps_perf_df[col].dropna()
                kpi_cols_ps[ki].metric(f"{tech} Avg Avail",
                                       f"{s.mean():.2f}%" if len(s) else "N/A")
                kpi_cols_ps[ki+1].metric(f"{tech} <90% Sites",
                                         f"{(s<90).sum()}" if len(s) else "N/A")
                ki += 2

            # Month × Technology availability table
            st.markdown("**Month-wise Availability Summary**")
            mom_ps_rows = []
            for _m in ps_perf_sel:
                _sub = ps_perf_df[ps_perf_df["Month_Label"]==_m]
                row = {"Month": _m.upper(),
                       "Sites": int(_sub["BTS IP ID"].nunique())}
                for tech, col in avail_ps.items():
                    s = _sub[col].dropna()
                    row[f"{tech} Avg%"]  = round(s.mean(),2) if len(s) else None
                    row[f"{tech} <90%"]  = int((s<90).sum()) if len(s) else 0
                    row[f"{tech} <95%"]  = int((s<95).sum()) if len(s) else 0
                    row[f"{tech} =100%"] = int((s==100).sum()) if len(s) else 0
                mom_ps_rows.append(row)
            mom_ps_df = pd.DataFrame(mom_ps_rows)

            def _ps_colour(v):
                try:
                    fv = float(v)
                    if fv < 90:  return "background-color:#f8d7da;color:#721c24"
                    if fv < 95:  return "background-color:#fff3cd;color:#856404"
                    return "background-color:#d4edda;color:#155724"
                except: return ""

            avg_ps_cols = [f"{t} Avg%" for t in avail_ps]
            st.dataframe(safe_style(mom_ps_df, _ps_colour, avg_ps_cols),
                         use_container_width=True, hide_index=True)

            # Trend chart
            if len(ps_perf_sel) >= 2:
                fig_ps_trend = px.line(
                    mom_ps_df.melt("Month", avg_ps_cols, var_name="Tech", value_name="Avg%"),
                    x="Month", y="Avg%", color="Tech", markers=True,
                    title="Availability Trend — Selected Months",
                    color_discrete_map={"2G Avg%":"#636EFA","3G Avg%":"#EF553B","4G TCS Avg%":"#00CC96"}
                )
                fig_ps_trend.add_hline(y=90, line_dash="dash", line_color="red",
                                       annotation_text="90%")
                st.plotly_chart(fig_ps_trend, use_container_width=True)

            # SDCA × Technology availability table (from selected months, SDCA from latest RBC)
            st.markdown("**SDCA-wise Availability — Selected Period**")
            if "SDCA" in ps_perf_df.columns:
                sdca_ps_agg = {"Sites": ("BTS IP ID","nunique")}
                for tech, col in avail_ps.items():
                    sdca_ps_agg[f"{tech} Avg%"] = (col,"mean")
                    sdca_ps_agg[f"{tech} <90%"] = (col, lambda x: (x<90).sum())
                sdca_ps = ps_perf_df.groupby("SDCA").agg(**sdca_ps_agg).round(2).reset_index()
                sdca_ps = sdca_ps.sort_values("Sites", ascending=False)
                st.dataframe(safe_style(sdca_ps.reset_index(drop=True), _ps_colour, avg_ps_cols),
                             use_container_width=True, hide_index=True)

                # Heatmap: SDCA × Tech for selected period
                hm_cols = [c for c in avg_ps_cols if c in sdca_ps.columns]
                if hm_cols:
                    hm_df = sdca_ps.set_index("SDCA")[hm_cols].rename(
                        columns={c:c.replace(" Avg%","") for c in hm_cols})
                    fig_ps_hm = go.Figure(data=go.Heatmap(
                        z=hm_df.values.tolist(), x=hm_df.columns.tolist(),
                        y=hm_df.index.tolist(),
                        colorscale=[[0,"#d7191c"],[0.5,"#ffffbf"],[1,"#1a9641"]],
                        zmin=70, zmax=100,
                        text=[[f"{v:.1f}%" if pd.notna(v) else "N/A" for v in row]
                              for row in hm_df.values.tolist()],
                        texttemplate="%{text}", hoverongaps=False))
                    fig_ps_hm.update_layout(
                        title=f"SDCA × Technology Availability — {', '.join(m.upper() for m in ps_perf_sel)}",
                        height=420)
                    st.plotly_chart(fig_ps_hm, use_container_width=True)

            st.markdown("---")

        # ══════════════════════════════════════════════════════════════════════
        # SECTION 2 – Revenue Summary
        # ══════════════════════════════════════════════════════════════════════
        if not ps_rev_df.empty:
            st.subheader(f"💰 Revenue Summary — {', '.join(m.upper() for m in ps_rev_sel)}")

            # Overall KPIs
            rv1, rv2, rv3, rv4, rv5 = st.columns(5)
            rv1.metric("Total Revenue (Lakhs)", f"₹{ps_rev_df['REV_LAKH'].sum():.2f}")
            rv2.metric("Unique Sites", ps_rev_df["BTSIPID"].nunique())
            rv3.metric("Months",        len(ps_rev_sel))
            rv4.metric("Avg Rev/Site",  f"₹{ps_rev_df.groupby('BTSIPID')['REV_LAKH'].sum().mean():.3f}L")
            rv5.metric("Zero Rev Sites",
                       ps_rev_df.groupby("BTSIPID")["REV_LAKH"].sum().eq(0).sum())

            # Month-wise revenue table
            st.markdown("**Month-wise Revenue Summary**")
            rev_mom_rows = []
            for _m in ps_rev_sel:
                _rdf = rev_store.get(_m, pd.DataFrame())
                if _rdf.empty: continue
                row = {"Month": _m.upper(),
                       "Sites": int(_rdf["BTSIPID"].nunique()),
                       "Total Rev (Lakhs)": round(_rdf["REV_LAKH"].sum(),2),
                       "Avg Rev/Site": round(_rdf["REV_LAKH"].mean(),3),
                       "Zero Rev Sites": int((_rdf["REV_LAKH"]==0).sum())}
                for tech, rc in [("2G","2g_rev"),("3G","3g_rev"),("4G","4g_rev")]:
                    if rc in _rdf.columns:
                        row[f"{tech} Rev (Lakhs)"] = round(_rdf[rc].sum()/100000,2)
                rev_mom_rows.append(row)
            if rev_mom_rows:
                rev_mom_df = pd.DataFrame(rev_mom_rows)
                st.dataframe(rev_mom_df.reset_index(drop=True),
                             use_container_width=True, hide_index=True)

                # Bar chart: revenue by month
                if len(ps_rev_sel) >= 2:
                    fig_rev_mom = px.bar(rev_mom_df, x="Month", y="Total Rev (Lakhs)",
                                        color="Month", text="Total Rev (Lakhs)",
                                        title="Revenue by Selected Month")
                    fig_rev_mom.update_traces(texttemplate="₹%{text:.2f}L",
                                              textposition="outside")
                    st.plotly_chart(fig_rev_mom, use_container_width=True)

            # SDCA Revenue summary from RBC
            st.markdown("**SDCA-wise Revenue — Selected Period**")
            if "SDCA" in ps_rev_df.columns:
                sdca_rev_ps = ps_rev_df.groupby("SDCA").agg(
                    Sites=("BTSIPID","nunique"),
                    Total_Rev=("REV_LAKH","sum"),
                    Avg_Rev=("REV_LAKH","mean"),
                    Zero_Sites=("REV_LAKH", lambda x:(x==0).sum()),
                ).round(3).reset_index().sort_values("Total_Rev", ascending=False)
                for tech, rc in [("2G","2g_rev"),("3G","3g_rev"),("4G","4g_rev")]:
                    if rc in ps_rev_df.columns:
                        ps_rev_df[f"_{tech}_L"] = ps_rev_df[rc]/100000
                        sdca_rev_ps = sdca_rev_ps.merge(
                            ps_rev_df.groupby("SDCA")[f"_{tech}_L"].sum().round(2).rename(f"{tech} Rev(L)"),
                            on="SDCA", how="left")

                srev_c1, srev_c2 = st.columns([1.4, 1])
                with srev_c1:
                    fig_srev_ps = px.bar(sdca_rev_ps, x="SDCA", y="Total_Rev",
                                         color="Total_Rev", color_continuous_scale="Greens",
                                         text="Total_Rev",
                                         title="Revenue by SDCA — Selected Period")
                    fig_srev_ps.update_traces(texttemplate="₹%{text:.2f}L",
                                              textposition="outside")
                    fig_srev_ps.update_layout(coloraxis_showscale=False)
                    st.plotly_chart(fig_srev_ps, use_container_width=True)
                with srev_c2:
                    st.dataframe(sdca_rev_ps.reset_index(drop=True),
                                 use_container_width=True, hide_index=True)

            st.markdown("---")

        # ══════════════════════════════════════════════════════════════════════
        # SECTION 3 – Site Discrepancy Report
        # ══════════════════════════════════════════════════════════════════════
        st.subheader("⚠️ Site Discrepancy Report — Perf vs Revenue")
        st.caption(
            "Sites present in performance files but missing from revenue files, and vice-versa. "
            "Based on BTS IP ID (perf) matched against BTSIPID (revenue)."
        )

        if ps_perf_df.empty or ps_rev_df.empty:
            st.info("Select both performance and revenue months to see discrepancy report.")
        else:
            disc_c1, disc_c2 = st.columns(2)

            perf_sites_ps = set(ps_perf_df["BTS IP ID"].astype(str).str.strip().unique())
            rev_sites_ps  = set(ps_rev_df["BTSIPID"].astype(str).str.strip().unique())

            with disc_c1:
                only_perf = sorted(perf_sites_ps - rev_sites_ps)
                st.markdown(f"**🔵 In Perf but NOT in Revenue: {len(only_perf)} sites**")
                st.caption("These sites report performance data but have no billing record.")
                if only_perf:
                    op_df = ps_perf_df[ps_perf_df["BTS IP ID"].isin(only_perf)]                        .drop_duplicates("BTS IP ID")                        [["BTS IP ID"] + [c for c in ["BTS Name","SDCA","SSAID","BTS Area","Band category"]
                                          if c in ps_perf_df.columns]]                        .reset_index(drop=True)
                    st.dataframe(op_df, use_container_width=True, hide_index=True)

            with disc_c2:
                only_rev = sorted(rev_sites_ps - perf_sites_ps)
                st.markdown(f"**🟡 In Revenue but NOT in Perf: {len(only_rev)} sites**")
                st.caption("These sites have billing records but no performance data uploaded.")
                if only_rev:
                    or_df = ps_rev_df[ps_rev_df["BTSIPID"].isin(only_rev)]                        .drop_duplicates("BTSIPID")                        [["BTSIPID"] + [c for c in ["SITENAME","SDCA","SSACODE","LOCATION"]
                                        if c in ps_rev_df.columns]]                        .reset_index(drop=True)
                    st.dataframe(or_df, use_container_width=True, hide_index=True)

            # Summary metric
            common = perf_sites_ps & rev_sites_ps
            d1, d2, d3, d4 = st.columns(4)
            d1.metric("Perf Sites",           len(perf_sites_ps))
            d2.metric("Revenue Sites",         len(rev_sites_ps))
            d3.metric("Common (matched)",      len(common))
            d4.metric("Total Discrepancy",     len(only_perf) + len(only_rev),
                      delta=None if (len(only_perf)+len(only_rev))==0 else
                            f"{len(only_perf)} perf-only, {len(only_rev)} rev-only")

            # Per-month discrepancy (if multiple months)
            if len(ps_perf_sel) > 1 or len(ps_rev_sel) > 1:
                st.markdown("**Per-month discrepancy breakdown**")
                disc_rows = []
                for _pm in ps_perf_sel:
                    _p = set(ps_perf_df[ps_perf_df["Month_Label"]==_pm]["BTS IP ID"]
                             .astype(str).str.strip().unique())
                    for _rm in ps_rev_sel:
                        _r = set(rev_store.get(_rm, pd.DataFrame()).get("BTSIPID",
                             pd.Series(dtype=str)).astype(str).str.strip().unique())
                        disc_rows.append({
                            "Perf Month":  _pm.upper(),
                            "Rev Month":   _rm.upper(),
                            "Perf Sites":  len(_p),
                            "Rev Sites":   len(_r),
                            "Only Perf":   len(_p - _r),
                            "Only Rev":    len(_r - _p),
                            "Common":      len(_p & _r),
                        })
                st.dataframe(pd.DataFrame(disc_rows), use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 14 – Vendor-wise Availability
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[13]:
    st.header("🏭 Vendor-wise Availability")
    st.caption(
        "Availability breakdown by equipment vendor for each technology. "
        "2G/3G vendors from perf file columns; 4G vendor is Tejas (all sites)."
    )

    if st.session_state.master_df is None:
        st.info("Upload performance files to enable this tab.")
    else:
      if True:
        # ── Month selector ─────────────────────────────────────────────────────
        va_all_months = sorted(st.session_state.master_df["Month_Label"].unique(),
                               key=month_sort_key)
        va_c1, va_c2 = st.columns([3,1])
        with va_c1:
            va_months = st.multiselect(
                "Select Month(s)",
                va_all_months,
                default=va_all_months,
                key="va_months"
            )
        with va_c2:
            va_thr = st.slider("Poor threshold %", 80, 98, 90, 1, key="va_thr")

        if not va_months:
            st.warning("Select at least one month.")
        else:
          if True:
            va_df = st.session_state.master_df[
                st.session_state.master_df["Month_Label"].isin(va_months)
            ].copy()
            for _c in ["Nw Avail (2G)","Nw Avail (3G)","Nw Avail (4G TCS)"]:
                if _c in va_df.columns:
                    va_df[_c] = pd.to_numeric(va_df[_c], errors="coerce")

            VENDOR_TECH_MAP = {
                "2G": ("Vendor_2G_Derived", "Nw Avail (2G)"),
                "3G": ("Vendor_3G_Derived", "Nw Avail (3G)"),
                "4G": ("Vendor_4G_Derived", "Nw Avail (4G TCS)"),
            }

            st.markdown("---")

            # ══════════════════════════════════════════════════════════════════
            # SECTION 1 – Overall Vendor Summary Table
            # ══════════════════════════════════════════════════════════════════
            st.subheader("📋 Vendor × Technology Summary")
            va_summary_rows = []
            for tech, (ven_col, avail_col) in VENDOR_TECH_MAP.items():
                if ven_col not in va_df.columns or avail_col not in va_df.columns:
                    continue
                for vendor in sorted(va_df[ven_col].dropna().unique()):
                    sub = va_df[va_df[ven_col]==vendor][avail_col].dropna()
                    if len(sub) == 0: continue
                    va_summary_rows.append({
                        "Technology": tech,
                        "Vendor": vendor,
                        "Sites": int(va_df[va_df[ven_col]==vendor]["BTS IP ID"].nunique()),
                        "Avg Avail%": round(sub.mean(), 2),
                        "Min%": round(sub.min(), 2),
                        "Max%": round(sub.max(), 2),
                        f"<{va_thr}% Sites": int((sub < va_thr).sum()),
                        "≥95% Sites": int((sub >= 95).sum()),
                        "=100% Sites": int((sub == 100).sum()),
                    })
            if va_summary_rows:
                va_summ_df = pd.DataFrame(va_summary_rows)

                def _va_colour(v):
                    try:
                        fv = float(v)
                        if fv < 90:  return "background-color:#f8d7da;color:#721c24"
                        if fv < 95:  return "background-color:#fff3cd;color:#856404"
                        return "background-color:#d4edda;color:#155724"
                    except: return ""

                st.dataframe(safe_style(va_summ_df, _va_colour, ["Avg Avail%"]),
                             use_container_width=True, hide_index=True)

                # Grouped bar: avg availability by vendor, facet by technology
                fig_va_bar = px.bar(
                    va_summ_df, x="Vendor", y="Avg Avail%",
                    color="Technology", barmode="group",
                    facet_col="Technology", text="Avg Avail%",
                    title="Avg Availability by Vendor & Technology",
                    color_discrete_map={"2G":"#636EFA","3G":"#EF553B","4G":"#00CC96"}
                )
                fig_va_bar.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
                fig_va_bar.add_hline(y=va_thr, line_dash="dash", line_color="red")
                fig_va_bar.update_layout(yaxis_range=[70,105])
                st.plotly_chart(fig_va_bar, use_container_width=True)

            st.markdown("---")

            # ══════════════════════════════════════════════════════════════════
            # SECTION 2 – Per-Technology Deep Dive (tabs)
            # ══════════════════════════════════════════════════════════════════
            st.subheader("🔬 Per-Technology Vendor Analysis")
            tech_va_tabs = st.tabs(["2G","3G","4G"])
            for vt_tab, (tech, (ven_col, avail_col)) in zip(tech_va_tabs, VENDOR_TECH_MAP.items()):
                with vt_tab:
                    if ven_col not in va_df.columns or avail_col not in va_df.columns:
                        st.info(f"No {tech} vendor/availability data in uploaded files.")
                        continue

                    vendors = sorted(va_df[ven_col].dropna().unique())
                    if not vendors:
                        st.info(f"No {tech} vendor data found.")
                        continue

                    # Month × Vendor availability table
                    st.markdown(f"**{tech} Availability by Vendor × Month**")
                    mv_rows = []
                    for _m in va_months:
                        _sub = va_df[va_df["Month_Label"]==_m]
                        for vendor in vendors:
                            _vs = _sub[_sub[ven_col]==vendor][avail_col].dropna()
                            if len(_vs) == 0: continue
                            mv_rows.append({
                                "Month":   _m.upper(),
                                "Vendor":  vendor,
                                "Sites":   int(_sub[_sub[ven_col]==vendor]["BTS IP ID"].nunique()),
                                "Avg%":    round(_vs.mean(), 2),
                                "Min%":    round(_vs.min(), 2),
                                "Max%":    round(_vs.max(), 2),
                                f"<{va_thr}%": int((_vs < va_thr).sum()),
                                "<90%":    int((_vs < 90).sum()),
                                "<80%":    int((_vs < 80).sum()),
                            })
                    if mv_rows:
                        mv_df = pd.DataFrame(mv_rows)
                        st.dataframe(safe_style(mv_df, _va_colour, ["Avg%"]),
                                     use_container_width=True, hide_index=True)

                        # Line chart: trend by vendor
                        if len(va_months) >= 2:
                            fig_vt_line = px.line(
                                mv_df, x="Month", y="Avg%", color="Vendor", markers=True,
                                title=f"{tech} Availability Trend by Vendor",
                                color_discrete_sequence=px.colors.qualitative.Set1
                            )
                            fig_vt_line.add_hline(y=va_thr, line_dash="dash",
                                                  line_color="red",
                                                  annotation_text=f"{va_thr}%")
                            st.plotly_chart(fig_vt_line, use_container_width=True)

                    # Distribution histogram by vendor
                    st.markdown(f"**{tech} Availability Distribution by Vendor**")
                    fig_vt_hist = px.histogram(
                        va_df[va_df[ven_col].notna()], x=avail_col,
                        color=ven_col, nbins=30, barmode="overlay", opacity=0.7,
                        title=f"{tech} Availability Distribution",
                        labels={avail_col:"Availability %", ven_col:"Vendor"},
                        color_discrete_sequence=px.colors.qualitative.Set1
                    )
                    fig_vt_hist.add_vline(x=va_thr, line_dash="dash", line_color="red",
                                          annotation_text=f"{va_thr}%")
                    st.plotly_chart(fig_vt_hist, use_container_width=True)

                    # Worst sites per vendor
                    st.markdown(f"**{tech} Worst 10 Sites per Vendor**")
                    for vendor in vendors:
                        vend_sub = va_df[
                            (va_df[ven_col]==vendor) &
                            (va_df["Month_Label"]==va_months[-1])
                        ][["BTS IP ID","BTS Name","SDCA",avail_col] if "SDCA" in va_df.columns
                          else ["BTS IP ID","BTS Name",avail_col]].dropna(subset=[avail_col])
                        worst_v = vend_sub.nsmallest(10, avail_col)
                        if len(worst_v) == 0: continue
                        with st.expander(
                            f"{vendor} — worst 10 in {va_months[-1].upper()} "
                            f"(avg {vend_sub[avail_col].mean():.1f}%)",
                            expanded=(worst_v[avail_col].max() < va_thr)
                        ):
                            st.dataframe(worst_v.reset_index(drop=True),
                                         use_container_width=True, hide_index=True)

            st.markdown("---")

            # ══════════════════════════════════════════════════════════════════
            # SECTION 3 – Month-on-Month Vendor Change
            # ══════════════════════════════════════════════════════════════════
            if len(va_months) >= 2:
                st.subheader("📉 Month-on-Month Vendor Availability Change")
                m_prev_va = va_months[-2]
                m_curr_va = va_months[-1]
                st.caption(f"Comparing {m_prev_va.upper()} → {m_curr_va.upper()}")

                for tech, (ven_col, avail_col) in VENDOR_TECH_MAP.items():
                    if ven_col not in va_df.columns or avail_col not in va_df.columns:
                        continue
                    chg_rows = []
                    for vendor in sorted(va_df[ven_col].dropna().unique()):
                        prev_v = va_df[(va_df["Month_Label"]==m_prev_va) &
                                       (va_df[ven_col]==vendor)][avail_col].dropna()
                        curr_v = va_df[(va_df["Month_Label"]==m_curr_va) &
                                       (va_df[ven_col]==vendor)][avail_col].dropna()
                        if len(prev_v) == 0 or len(curr_v) == 0: continue
                        chg_rows.append({
                            "Vendor": vendor,
                            f"{m_prev_va.upper()} Avg%": round(prev_v.mean(),2),
                            f"{m_curr_va.upper()} Avg%": round(curr_v.mean(),2),
                            "Change%": round(curr_v.mean()-prev_v.mean(),2),
                        })
                    if chg_rows:
                        chg_df = pd.DataFrame(chg_rows)
                        st.markdown(f"**{tech}**")

                        def _chg_col(v):
                            try:
                                fv = float(v)
                                if fv < -1: return "background-color:#f8d7da;color:#721c24"
                                if fv > 1:  return "background-color:#d4edda;color:#155724"
                            except: pass
                            return ""

                        st.dataframe(safe_style(chg_df, _chg_col, ["Change%"]),
                                     use_container_width=True, hide_index=True)



# TAB 15 – Executive Report
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[14]:
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
