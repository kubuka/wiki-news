import streamlit as st
import pandas as pd
from google.cloud import bigquery
import os
import plotly.express as px
from datetime import timedelta
import dotenv

dotenv.load_dotenv()
gcp_key_path = os.getenv("GCP_CREDS_PATH")

if gcp_key_path and os.path.exists(gcp_key_path):
    client = bigquery.Client.from_service_account_json(gcp_key_path)
else:
    # adc
    client = bigquery.Client()


st.set_page_config(
    page_title="Wikipedia Real-Time Monitor", layout="wide", page_icon="🌍"
)
st.title("🌍 Wikipedia Real-Time Trend Monitor")

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")  # zmienic pozniej na env

query_bots_vs_humans = f"""
    SELECT 
        SUM(bot_edits) as total_bot_edits,
        SUM(human_edits) as total_human_edits
    FROM `{GCP_PROJECT_ID}.wikipedia_streaming.wiki_global_stats`
    WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE)
"""

query_top_wikis = f"""
    SELECT 
        server_name,
        SUM(bot_edits) as total_bot_edits,
        SUM(human_edits) as total_human_edits
    FROM `{GCP_PROJECT_ID}.wikipedia_streaming.wiki_global_stats`
    WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE)
    GROUP BY server_name
    ORDER BY (total_bot_edits + total_human_edits) DESC
    LIMIT 5
"""

query_time_trend = f"""
    SELECT 
        window_start,
        SUM(bot_edits) as total_bot_edits,
        SUM(human_edits) as total_human_edits
    FROM `{GCP_PROJECT_ID}.wikipedia_streaming.wiki_global_stats`
    WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    GROUP BY window_start
    ORDER BY window_start
"""

query_global_hot = f"""
    SELECT 
        server_name, 
        page_title, 
        sum(edit_count) as total_edits_hour
    FROM `{GCP_PROJECT_ID}.wikipedia_streaming.wiki_hot_topics`
    WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE)
    GROUP BY server_name, page_title
    ORDER BY total_edits_hour DESC
    LIMIT 3
"""

query_all_top10 = f"""
    WITH AggregatedTopics AS (
        SELECT 
            server_name, 
            page_title, 
            SUM(edit_count) as total_edits_hour
        FROM `{GCP_PROJECT_ID}.wikipedia_streaming.wiki_hot_topics`
        WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE)
        GROUP BY server_name, page_title
    ),
    RankedTopics AS (
        SELECT 
            server_name, 
            page_title, 
            total_edits_hour,
            ROW_NUMBER() OVER(PARTITION BY server_name ORDER BY total_edits_hour DESC) as rank
        FROM AggregatedTopics
    )
    SELECT server_name, page_title, total_edits_hour
    FROM RankedTopics
    WHERE rank <= 10
"""


def render_section_title(icon, title, color):
    st.markdown(
        f"""
        <h2 style="
            color: {color}; 
            border-bottom: 2px solid {color}; 
            padding-bottom: 10px; 
            margin-top: 40px;
            font-size: 32px;
        ">
            {icon} {title}
        </h2>
    """,
        unsafe_allow_html=True,
    )


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
            fig.update_traces(
                textposition="inside",
                textinfo="percent+label",
                textfont=dict(size=20, weight=700),
                pull=[0.03, 0],
                marker=dict(line=dict(color="#0e1117", width=3)),
            )
            fig.update_layout(height=600, showlegend=False)
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("no data collected yet")
    except Exception as e:
        st.error(f"Fail to download from BQ: {e}")


@st.fragment(run_every=timedelta(minutes=1))
def render_bar_chart():
    try:
        df_bar = client.query(query_top_wikis).to_dataframe()
        df_bar = df_bar.fillna(0)

        if not df_bar.empty:
            df_melted = df_bar.melt(
                id_vars=["server_name"],
                value_vars=["total_bot_edits", "total_human_edits"],
                var_name="Editor",
                value_name="Edits",
            )
            df_melted["Editor"] = df_melted["Editor"].map(
                {"total_bot_edits": "👾 Bots", "total_human_edits": "👨‍💻 Humans"}
            )

            fig = px.bar(
                df_melted,
                x="server_name",
                y="Edits",
                color="Editor",
                title="Top 5 active wikis (last hour)",
                color_discrete_map={"👾 Bots": "#FF6B6B", "👨‍💻 Humans": "#4ECDC4"},
            )
            fig.update_layout(
                barmode="stack",
                height=500,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )
            fig.update_xaxes(title="Wiki Domain")
            fig.update_yaxes(title="Number of Edits")

            st.plotly_chart(fig, width="stretch")
        else:
            st.info("no data collected yet")
    except Exception as e:
        st.error(f"Fail to download from BQ: {e}")


@st.fragment(run_every=timedelta(minutes=1))
def render_line_chart():
    try:
        df_trend = client.query(query_time_trend).to_dataframe()
        df_trend = df_trend.fillna(0)

        if not df_trend.empty:
            df_melted = df_trend.melt(
                id_vars=["window_start"],
                value_vars=["total_bot_edits", "total_human_edits"],
                var_name="Editor",
                value_name="Edits",
            )

            df_melted["Editor"] = df_melted["Editor"].map(
                {"total_bot_edits": "👾 Bots", "total_human_edits": "👨‍💻 Humans"}
            )

            fig = px.line(
                df_melted,
                x="window_start",
                y="Edits",
                color="Editor",
                title="Live Edits Trend",
                markers=False,
                color_discrete_map={"👾 Bots": "#FF6B6B", "👨‍💻 Humans": "#4ECDC4"},
            )

            fig.update_layout(
                height=500,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis_title="Time (Minute by Minute)",
                yaxis_title="Number of Edits",
            )
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("no data collected yet")
    except Exception as e:
        st.error(f"Fail to download from BQ: {e}")


@st.fragment(run_every=timedelta(minutes=1))
def render_global_hot_topics():
    try:
        df_hot = client.query(query_global_hot).to_dataframe()
        df_hot = df_hot.fillna(0)
        if not df_hot.empty:
            cols = st.columns(3)
            medals = ["🥇", "🥈", "🥉"]

            for i, row in df_hot.iterrows():
                with cols[i]:
                    clean_title = row["page_title"].replace("_", " ")
                    st.markdown(
                        f"""
                        <div style="
                            background-color: rgba(78, 205, 196, 0.1);
                            border: 1px solid rgba(78, 205, 196, 0.5);
                            border-radius: 10px;
                            padding: 20px;
                            text-align: center;
                            margin-bottom: 30px;
                            margin-top: 30px;
                        ">
                            <h1 style="font-size: 40px; margin: 0px; color: #FF6B6B;">{medals[i]}</h1>
                            <h3 style="color: #e0e0e0; margin: 10px 0; height: 60px; overflow: hidden;">{clean_title}</h3>
                            <p style="color: #a0a0a0; font-size: 12px; margin: 0 0 10px 0;">{row['server_name']}</p>
                            <h2 style="color: #4ECDC4; margin: 0;">{int(row['total_edits_hour'])} <span style="font-size: 14px; color: #a0a0a0;">edits/h</span></h2>
                        </div>
                    """,
                        unsafe_allow_html=True,
                    )
        else:
            st.info("No hot topics atm. Waiting for hot news...")
    except Exception as e:
        st.error(f"Fail to download from BQ: {e}")


@st.cache_data(ttl=60, show_spinner=False)
def load_all_hot_topics():
    df = client.query(query_all_top10).to_dataframe()
    return df.fillna(0)


@st.fragment(run_every=timedelta(minutes=1))
def render_language_trends():
    try:
        df_all_hot = load_all_hot_topics()
        if not df_all_hot.empty:
            domains = sorted(df_all_hot["server_name"].unique())
            selected_domain = st.selectbox("Choose domain:", domains)

            df_filtered = df_all_hot[
                df_all_hot["server_name"] == selected_domain
            ].copy()

            if not df_filtered.empty:
                df_filtered["clean_title"] = df_filtered["page_title"].str.replace(
                    "_", " "
                )
                fig = px.bar(
                    df_filtered,
                    x="total_edits_hour",
                    y="clean_title",
                    orientation="h",
                    title=f"Top 10 Topics - {selected_domain}",
                    labels={"total_edits_hour": "Edits/h", "clean_title": "Article"},
                )
                fig.update_layout(
                    height=500,
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    yaxis={"categoryorder": "total ascending"},
                )
                fig.update_traces(marker_color="#9b59b6")
                st.plotly_chart(fig, width="stretch")
            else:
                st.info(f"No hot topics for {selected_domain} rn")
        else:
            st.info(f"Retrieving data... ")
    except Exception as e:
        st.error(f"Fail to download from BQ: {e}")


render_section_title("📊", "Bots vs Humans", "#4ECDC4")
render_pie()
render_bar_chart()
render_line_chart()
render_section_title("🔥", "Hot Topics", "#4ECDC4")
render_global_hot_topics()
render_language_trends()
st.markdown("---")
