streamlit==1.38.0
pandas>=2.0.0
plotly>=5.20.0


load_dt,stage_nm,src_nm,src_display_nm,doc_type_cd,doc_subtype_cd,record_cnt,member_cnt,encounter_cnt,success_cnt,error_cnt,avg_latency_sec,p95_latency_sec,load_yyyy_mm
2025-10-01,HOSCDA_SIG,RA01,Region Alpha,CCDA,,160,54,34,150,10,90,130,2025-10
2025-10-01,HOSCDA,RA02,Region Beta,ADT,A01,190,63,42,182,8,80,115,2025-10
2025-10-02,HOSCDA_EXTRACT,RA03,Region Gamma,CCR,,220,71,51,212,8,105,160,2025-10
2025-10-03,HOSCDA_SIG,RA01,Region Alpha,ADT,A02,180,60,41,172,8,95,145,2025-10


import pandas as pd
import streamlit as st
import plotly.express as px
from pathlib import Path

st.set_page_config(page_title="HOSCDA Ingest Volume", page_icon="📊", layout="wide")
st.title("HOSCDA Ingest Volume — Local Python Dashboard")

# --- Load CSV ---
DEFAULT_CSV = Path(__file__).with_name("sample_fact_ingest_volume.csv")
df = pd.read_csv(DEFAULT_CSV, parse_dates=["load_dt"])

# --- Sidebar Filters ---
st.sidebar.header("Filters")
stages  = ["All"] + sorted(df["stage_nm"].unique())
sources = ["All"] + sorted(df["src_nm"].unique())
dtypes  = ["All"] + sorted(df["doc_type_cd"].unique())
subs    = ["All"] + sorted(df["doc_subtype_cd"].fillna("").unique())

sel_stage  = st.sidebar.selectbox("Stage", stages)
sel_source = st.sidebar.selectbox("Source", sources)
sel_dtype  = st.sidebar.selectbox("Doc Type", dtypes)
sel_sub    = st.sidebar.selectbox("Subtype", subs)

mask = (
    ((df["stage_nm"] == sel_stage) | (sel_stage == "All")) &
    ((df["src_nm"] == sel_source)  | (sel_source == "All")) &
    ((df["doc_type_cd"] == sel_dtype) | (sel_dtype == "All")) &
    ((df["doc_subtype_cd"].fillna("") == sel_sub) | (sel_sub == "All"))
)
f = df.loc[mask]

# --- KPIs ---
col1, col2, col3 = st.columns(3)
col1.metric("Total Records", f"{int(f['record_cnt'].sum()):,}")
col2.metric("Members",       f"{int(f['member_cnt'].sum()):,}")
col3.metric("Encounters",    f"{int(f['encounter_cnt'].sum()):,}")

# --- Charts ---
if not f.empty:
    daily = f.groupby(["load_dt","doc_type_cd"], as_index=False)["record_cnt"].sum()
    st.plotly_chart(px.bar(daily, x="load_dt", y="record_cnt", color="doc_type_cd",
                           title="Daily Records by Document Type"), use_container_width=True)

    succ_err = f[["success_cnt","error_cnt"]].sum()
    pie_df = pd.DataFrame({"Status":["Success","Error"],"Count":[succ_err["success_cnt"], succ_err["error_cnt"]]})
    st.plotly_chart(px.pie(pie_df, names="Status", values="Count", hole=0.55, title="Success vs Error"), use_container_width=True)

    monthly = f.groupby(["load_yyyy_mm","src_nm"], as_index=False)["record_cnt"].sum()
    st.plotly_chart(px.line(monthly, x="load_yyyy_mm", y="record_cnt", color="src_nm", markers=True,
                            title="Monthly Records by Source"), use_container_width=True)

st.subheader("Sample Rows")
st.dataframe(f.head(50))


pip install -r requirements.txt


streamlit run hoscda_dashboard.py
