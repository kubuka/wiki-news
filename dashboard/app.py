import streamlit as st
import pandas as pd
from google.cloud import bigquery
import os
import plotly.express as px
from datetime import timedelta

# adc
client = bigquery.Client()

st.set_page_config(
    page_title="Wikipedia Real-Time Monitor", layout="wide", page_icon="🌍"
)
st.title("🌍 Wikipedia Real-Time Trend Monitor")

GCP_PROJECT_ID = "wiki-news-499909"  # zmienic pozniej na env

query_bots_vs_humans = f"""
    SELECT 
        SUM(bot_edits) as total_bot_edits,
        SUM(human_edits) as total_human_edits
    FROM `{GCP_PROJECT_ID}.wikipedia_streaming.wiki_global_stats`
    WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE)
"""


@st.fragment(run_every=timedelta(minutes=1))
def render_pie():
    try:
        df_pie = client.query(query_bots_vs_humans).to_dataframe()
        df_pie = df_pie.fillna(0)
        if not df_pie.empty and (
            df_pie["total_bot_edits"].iloc[0] > 0
            or df_pie["total_human_edits"].iloc[0] > 0
        ):
            pie_data = pd.DataFrame(
                {
                    "Editor": ["👾 Bots", "👨‍💻 Humans"],
                    "Edits": [
                        df_pie["total_bot_edits"].iloc[0],
                        df_pie["total_human_edits"].iloc[0],
                    ],
                }
            )
            fig = px.pie(
                pie_data,
                values="Edits",
                names="Editor",
                title="Humans vs Bots (last hour)",
                hole=0.4,
                color="Editor",
                color_discrete_map={"👾 Bots": "#FF6B6B", "👨‍💻 Humans": "#4ECDC4"},
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(title_x=0.41)
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("no data collected yet")
    except Exception as e:
        st.error(f"Fail to download from BQ: {e}")


render_pie()
st.markdown("---")
