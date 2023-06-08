import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, date, timedelta 
from time import time

import streamlit as st

host = os.getenv("HETZNER_HOST")
engine = create_engine(f'postgresql://readonly:readonly@{host}:5432/postgres')

# Cache persistence length in seconds
ttl = 7_200


day = date(2023, 6, 2)
day_str = day.isoformat()
#if datetime.now().hour > 8:
#    day = date.today() - timedelta(days=1)
#    day_str = day.isoformat()
#else:
#    day = date.today() - timedelta(days=2)
#    day_str = day.isoformat()

st.set_page_config(layout="wide", page_title="Terrapin Compass", page_icon="https://terrapinfinance.com/logo.webp")

mic_df = pd.read_csv("mic_venue_codes.csv")

@st.cache_data(ttl=ttl)
def get_eligible_venues():
    eligible_venues = pd.read_sql_query(f"""
        SELECT distinct(venue) FROM trades
        WHERE trade_datetime > %(date)s
        AND trade_datetime < %(date)s + interval '1 day'
        AND price_type = 'PERC'
        AND venue is not null
    """, engine, params={"date": day})["venue"].to_list()
    return list(sorted(eligible_venues))

@st.cache_data(ttl=ttl)
def get_most_traded_df(issuer_type):
    return pd.read_sql_query(f"""
        SELECT isin, count(*) how_many FROM trades
        WHERE EXISTS(SELECT 1 FROM bonds WHERE isin = trades.isin AND issuer_type = %(issuer_type)s AND asset_class != 'asset-backed security')
        AND trade_datetime > %(date)s
        AND trade_datetime < %(date)s + interval '1 day'
        GROUP BY isin
        ORDER BY how_many DESC
        LIMIT 10
    """, engine, index_col="isin", params={"date": day, "issuer_type": issuer_type})

@st.cache_data(ttl=ttl)
def get_most_quoted_df():
    return pd.read_sql_query(f"""
        SELECT isin, count(*) how_many FROM quotes 
        WHERE EXISTS(SELECT 1 FROM bonds WHERE isin = quotes.isin AND asset_class != 'asset-backed security')
        AND quote_datetime > %(date)s
        AND quote_datetime < %(date)s + interval '1 day'
        GROUP BY isin
        ORDER BY how_many DESC
        LIMIT 10
    """, engine, index_col="isin", params={"date": day})

@st.cache_data(ttl=ttl*3)
def get_top_level_metrics_df():
    last_day_metrics = pd.read_sql_query(f"""
        SELECT count(distinct(isin)) how_many_isins, count(id) how_many_trades, count(distinct(venue)) how_many_venues FROM trades
        WHERE EXISTS(SELECT 1 FROM bonds WHERE isin = trades.isin AND asset_class != 'asset-backed security')
        AND trade_datetime > %(date)s
        AND trade_datetime < %(date)s + interval '1 day'
    """, engine, params={"date": day}).to_dict(orient="records")[0]

    last_month_metrics = pd.read_sql_query(f"""
        SELECT count(distinct(isin)) how_many_isins, count(id) how_many_trades, count(distinct(venue)) how_many_venues FROM trades
        WHERE EXISTS(SELECT 1 FROM bonds WHERE isin = trades.isin AND asset_class != 'asset-backed security')
        AND trade_datetime > %(date)s - interval '1 month'
        AND trade_datetime < %(date)s + interval '1 day'
    """, engine, params={"date": day}).to_dict(orient="records")[0]

    return last_day_metrics, last_month_metrics

@st.cache_data(ttl=ttl*3)
def get_venue_metrics_df(issuer_type):
    last_month_metrics = pd.read_sql_query(f"""
        SELECT count(distinct(isin)) "Bonds traded", count(*) "Number of trades", venue "Venue MIC"
        FROM trades
        WHERE EXISTS(SELECT 1 FROM bonds WHERE isin = trades.isin AND asset_class != 'asset-backed security' AND issuer_type = %(issuer_type)s)
        AND trade_datetime > %(date)s - interval '1 month'
        AND trade_datetime < %(date)s + interval '1 day'
        GROUP BY venue
    """, engine, params={"date": day, "issuer_type": issuer_type})

    last_month_metrics["Venue name"] = last_month_metrics["Venue MIC"].map(
        {mic: name for mic, name in zip(mic_df["MIC"], mic_df["NAME-INSTITUTION DESCRIPTION"])}
    )

    return last_month_metrics[["Venue MIC", "Venue name", "Bonds traded", "Number of trades"]].sort_values("Venue name")


col1, _, col2 = st.columns([3,1,2])
with col1:
    st.markdown('## Terrapin Compass')
    st.markdown(f'Explore and analyse post-trade flow in European bond venues.<br/>This is a restricted data version. You will only see data for: **{day_str}**.', unsafe_allow_html=True)
    with st.expander("Learn more"):
        st.markdown("""Our software captures pre- and post-trade data from European trading venues (made available as per
    MIFID II regulations), collates and aggregates it, delivering a unified data stream
    and reducing costs by not requiring individual venue licenses. Importantly, we do not “sell”
    the trading data, only the software (we run and manage the infrastruture). The data can be explored via our
    dashboards or easily exported and fed into your internal systems and processes via an API.
    The trading data is combined with our own reference (static) bond information which allows us to serve clients
    in a much simpler manner without additional agreements with third-party data providers. In addition,
    our simple licensing for reference data allows clients to share insights more broadly internally and with clients.
    """)
        st.markdown("""Use cases:
- Observe market depth and detailed
price action across all European
trading venues
- Track intra-day liquidity and historical
patterns over longer periods of time
- Buy side: improved pre-trade models
and ex-post assessment of ‘best
execution’
- Sell side: evaluate and improve
performance by comparing lost RFQ
with executed prices
- Combine with internal data sets to
track exposure and risk statistics
- Enhanced regulatory reporting
""")

with col2:
    last_day_metrics, last_month_metrics = get_top_level_metrics_df()

    st.caption(f'Metrics for post-trade bond data collected on {day_str}')
    col1, col2, col3 = st.columns(3)
    col1.metric(label="Trades", value=last_day_metrics["how_many_trades"])
    col2.metric(label="Instruments", value=last_day_metrics["how_many_isins"])
    col3.metric(label="Venues", value=last_day_metrics["how_many_venues"])

    st.caption(f'Metrics for post-trade bond data collected in the last month')
    col1, col2, col3 = st.columns(3)
    col1.metric(label="Trades", value=last_month_metrics["how_many_trades"])
    col2.metric(label="Instruments", value=last_month_metrics["how_many_isins"])
    col3.metric(label="Venues", value=last_month_metrics["how_many_venues"])


with st.columns([1,4])[0]:
    option = st.selectbox(
        'Choose a dashboard:',
        ["Per-issue view", "Asset class view", "Venue coverage and metrics"])

st.divider()


if option == "Asset class view":
    cols = st.columns(5, gap="medium")
    with cols[0]:
        issuer_type = st.selectbox(
            'Issuer type',
            ('Government', 'Corporate')
        )

    with cols[1]:
        countries = pd.read_sql_query(f"""
            SELECT distinct(country) FROM bonds
            WHERE EXISTS(
                SELECT 1 FROM trades
                WHERE isin = bonds.isin
                LIMIT 1
            )
            ORDER BY country ASC;
        """, engine)["country"].to_list()
        country = st.selectbox('Country', countries, index=countries.index("United Kingdom"))

    with st.columns([2,3])[0]:
        with st.expander("Eligible venues"):
            eligible_venues = get_eligible_venues()
            selected_venues = st.multiselect('Selected venues:', eligible_venues, eligible_venues, label_visibility="collapsed")


    col1, col2 = st.columns(2)

    with col1:
        trades_df = pd.read_sql_query(f"""
            SELECT 1 as i, trade_datetime, GREATEST(quantity, notional_amount) as quantity FROM trades 
            WHERE EXISTS(
                SELECT 1 FROM bonds 
                WHERE country = '{country}'
                AND issuer_type = '{issuer_type.lower()}'
                AND isin = trades.isin
            )
            AND trade_datetime > %(date)s
            AND trade_datetime < %(date)s + interval '1 day'
            AND trade_datetime is not null
            AND venue in %(venues)s
        """, engine, params={"date": day, "venues": tuple(selected_venues)})


        fig = px.histogram(trades_df, 
            x='trade_datetime', y='i',
            labels={"trade_datetime": "Date and time"}
        )
        fig.update_layout(title="Number of trades per time period", bargap=0.1, yaxis_title="Number of trades")
        st.plotly_chart(fig)

        trades_per_venue_df = pd.read_sql_query(f"""
            SELECT venue, count(*) how_many, sum(GREATEST(quantity, notional_amount)) total_quantity FROM trades 
            WHERE EXISTS(
                SELECT 1 FROM bonds 
                WHERE country = '{country}'
                AND issuer_type = '{issuer_type.lower()}'
                AND isin = trades.isin
            )
            AND trade_datetime > %(date)s
            AND trade_datetime < %(date)s + interval '1 day'
            AND trade_datetime is not null
            AND venue in %(venues)s
            GROUP BY venue
        """, engine, params={"date": day, "venues": tuple(selected_venues)})

        fig = px.bar(trades_per_venue_df, x='venue', y='how_many', labels={"how_many": "Number of trades", "venue": "Venue"})
        fig.update_layout(title="Number of trades per venue")
        st.plotly_chart(fig)

    with col2:
        fig = px.histogram(trades_df, x='trade_datetime', y='quantity',
            labels={"trade_datetime": "Date and time"},
            histfunc="sum"
        )
        fig.update_layout(title="Volume traded per time period", bargap=0.1, yaxis_title="Volume")
        st.plotly_chart(fig)

        fig = px.bar(trades_per_venue_df, x='venue', y='total_quantity', labels={"venue": "Venue", "total_quantity": "Volume"})
        fig.update_layout(title="Volume traded per venue")
        st.plotly_chart(fig)

elif option == "Per-issue view":

    col1, col2 = st.columns([1,4])

    with col1:
        most_traded_govies_df = get_most_traded_df("government")
        st.write(f"Most traded govies on {day}")
        st.dataframe(most_traded_govies_df)

        most_traded_corporates_df = get_most_traded_df("corporate")

        st.write(f"Most traded corporates on {day}")
        st.dataframe(most_traded_corporates_df)


        #most_quoted_df = get_most_quoted_df()

        #st.write(f"Most quoted ISINs on {day}")
        #st.dataframe(most_quoted_df)


    with col2: 
        col1, col2 = st.columns([1,1])
        with col1: 
            isin = st.text_input('Input an ISIN to visualise trades and quotes')

            with st.expander("Eligible venues"):
                eligible_venues = get_eligible_venues()
                selected_venues = st.multiselect('Selected venues:', eligible_venues, eligible_venues, label_visibility="collapsed")

        quotes_df = pd.read_sql_query(f"""
            SELECT price, quantity, side, quote_datetime as timestamp, 'DFRA' as venue, source FROM quotes 
            WHERE isin = %(isin)s
            AND price > 0
            AND quote_datetime > %(date)s
            AND quote_datetime < %(date)s + interval '1 day'
        """, engine, params={"date": day, "isin": isin})

        trades_df = pd.read_sql_query(f"""
            SELECT price, GREATEST(quantity, notional_amount) as quantity, 'trade' as side, trade_datetime as timestamp, venue, source FROM trades
            WHERE isin = %(isin)s
            AND trade_datetime > %(date)s
            AND trade_datetime < %(date)s + interval '1 day'
            AND price_type = 'PERC'
            AND venue IN %(venues)s
        """, engine, params={"date": day, "isin": isin, "venues": tuple(selected_venues)})

        trading_venues = trades_df["venue"].unique()

        issue_df = pd.read_sql_query(f"""
            SELECT ticker, issuer, coupon, maturity_date, currency, issuer_type, asset_class, tp_sector FROM bonds
            WHERE isin = '{isin}'
        """, engine, index_col="ticker")

        #df = pd.concat([quotes_df, trades_df])
        df = trades_df

        if len(trades_df) > 0:

            st.dataframe(issue_df)
            st.write(f"For more info on this instrument visit [https://terrapinfinance.com/{isin}](https://terrapinfinance.com/{isin})")
            #st.markdown(f"**Note:** quotes are only from Boerse Frankfurst (DFRA). Trades from all eligible venues are shown.")

            col1, col2 = st.columns(2)
            with col1:
                fig = px.scatter(df, 
                    x='timestamp', y='price', 
                    color="venue", symbol="venue", 
                    hover_data=["timestamp", "price", "quantity", "venue", "source"], 
                    color_discrete_sequence=px.colors.qualitative.Plotly,
                    labels={
                        "price": "Price (pct of face value)",
                        "timestamp": "Date and time"
                    })
                fig.update_xaxes(showgrid=True, gridwidth=1)
                fig.update_layout(title="Trade prices", width=550, height=500)
                st.plotly_chart(fig)

                fig = px.histogram(df, 
                    x='venue', y='timestamp', 
                    histfunc="count", barmode="group", nbins=100,
                    labels={
                        "venue": "Venue"
                    })
                fig.update_xaxes(showgrid=True, gridwidth=1)
                fig.update_layout(title="Trades per venue", yaxis_title="Total number of trades", width=550, height=500)
                st.plotly_chart(fig)

            with col2:
                fig = px.histogram(df, 
                    x='timestamp', y='timestamp', 
                    histfunc="count", barmode="group", nbins=100,
                    labels={
                        "timestamp": "Date and time"
                    })
                fig.update_xaxes(showgrid=True, gridwidth=1)
                fig.update_layout(title="Aggregate trades throughout the day", yaxis_title="Total number of trades", width=550, height=500)
                st.plotly_chart(fig)

                fig = px.histogram(df, 
                    x='venue', y='quantity', 
                    histfunc="sum", barmode="group", nbins=100,
                    labels={
                        "venue": "Venue"
                    })
                fig.update_xaxes(showgrid=True, gridwidth=1)
                fig.update_layout(title="Volume per venue (as reported)", yaxis_title="Total volume of trades", width=550, height=500)
                st.plotly_chart(fig)

        else:
            if len(isin) > 0:
                st.write("No trades found. Please try a different ISIN.")

    
elif option == "Venue coverage and metrics":
    st.write(f"Metrics by issuer type and venues of execution over the last month (including Systematic Internalizers and Off-Exchange)")

    with st.columns([2,1])[0]:
        venue_gov_metrics_df = get_venue_metrics_df("corporate")
        st.write(f"Government bonds:")
        st.table(venue_gov_metrics_df)

    with st.columns([2,1])[0]:
        venue_corp_metrics_df = get_venue_metrics_df("government")
        st.write(f"Corporate bonds:")
        st.table(venue_corp_metrics_df)