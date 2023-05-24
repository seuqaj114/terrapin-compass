import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import date
from time import time

import streamlit as st

host = os.getenv("HETZNER_HOST")
engine = create_engine(f'postgresql://readonly:readonly@{host}:5432/postgres')

st.set_page_config(layout="wide", page_title="Terrapin Compass", page_icon="https://terrapinfinance.com/logo.webp")

st.markdown('## Terrapin Compass')
st.markdown('Explore and analyse pre- and post-trade flow in European bond venues')

with st.columns([1,4])[0]:
    option = st.selectbox(
        'Choose a dashboard:',
        ["Per-issue view", "Asset class view"])

st.divider()

@st.cache_data
def get_eligible_venues(date):
    eligible_venues = pd.read_sql_query(f"""
        SELECT distinct(venue) FROM trades
        WHERE quantity > 0
        AND trade_datetime > %(date)s
        AND price_type = 'PERC'
    """, engine, params={"date": date})["venue"].to_list()
    return list(sorted(eligible_venues))

day = '2023-05-23'

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
            eligible_venues = get_eligible_venues(day)
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
        
    @st.cache_data
    def get_most_traded_df():
        return pd.read_sql_query(f"""
            SELECT isin, count(*) how_many FROM trades
            WHERE EXISTS(SELECT 1 FROM bonds WHERE isin = trades.isin AND asset_class != 'asset-backed security')
            AND trade_datetime > '{day}'
            GROUP BY isin
            ORDER BY how_many DESC
            LIMIT 10
        """, engine, index_col="isin")

    @st.cache_data
    def get_most_quoted_df():
        return pd.read_sql_query(f"""
            SELECT isin, count(*) how_many FROM quotes 
            WHERE EXISTS(SELECT 1 FROM bonds WHERE isin = quotes.isin AND asset_class != 'asset-backed security')
            AND quantity > 0
            AND quote_datetime > '{day}'
            GROUP BY isin
            ORDER BY how_many DESC
            LIMIT 10
        """, engine, index_col="isin")


    col1, col2 = st.columns([1,4])

    with col1:
        most_traded_df = get_most_traded_df()

        st.write(f"Most traded ISINs on {day}")
        st.dataframe(most_traded_df)

        most_quoted_df = get_most_quoted_df()

        st.write(f"Most quoted ISINs on {day}")
        st.dataframe(most_quoted_df)


    with col2: 
        col1, col2 = st.columns([1,1])
        with col1: 
            isin = st.text_input('Input an ISIN to visualise trades and quotes')

            with st.expander("Eligible venues"):
                eligible_venues = get_eligible_venues(day)
                selected_venues = st.multiselect('Selected venues:', eligible_venues, eligible_venues, label_visibility="collapsed")

        quotes_df = pd.read_sql_query(f"""
            SELECT price, quantity, side, quote_datetime as timestamp, 'DFRA' as venue, source FROM quotes 
            WHERE isin = %(isin)s
            AND price > 0
            AND quote_datetime > %(date)s
        """, engine, params={"date": day, "isin": isin})

        trades_df = pd.read_sql_query(f"""
            SELECT price, GREATEST(quantity, notional_amount) as quantity, 'trade' as side, trade_datetime as timestamp, venue, source FROM trades
            WHERE isin = %(isin)s
            AND trade_datetime > %(date)s
            AND price_type = 'PERC'
            AND venue IN %(venues)s
        """, engine, params={"date": day, "isin": isin, "venues": tuple(selected_venues)})

        trading_venues = trades_df["venue"].unique()

        issue_df = pd.read_sql_query(f"""
            SELECT ticker, issuer, coupon, maturity_date, currency, issuer_type, asset_class, tp_sector FROM bonds
            WHERE isin = '{isin}'
        """, engine, index_col="ticker")

        df = pd.concat([quotes_df, trades_df])

        if len(quotes_df) > 0:

            st.dataframe(issue_df)
            st.write(f"For more info on this instrument visit [https://terrapinfinance.com/{isin}](https://terrapinfinance.com/{isin})")
            st.markdown(f"**Note:** quotes are only from Boerse Frankfurst (DFRA). Trades from all eligible venues are shown.")

            col1, col2 = st.columns(2)
            with col1:
                fig = px.scatter(df, 
                    x='timestamp', y='price', 
                    color="side", symbol="side", 
                    hover_data=["timestamp", "price", "quantity", "venue", "source"], 
                    opacity=0.95,
                    labels={
                        "price": "Price (pct of face value)",
                        "timestamp": "Date and time"
                    })
                fig.update_xaxes(showgrid=True, gridwidth=1)
                fig.update_layout(title="Quote and trade prices", width=600, height=500)
                st.plotly_chart(fig)

            with col2:
                fig = px.histogram(df, 
                    x='timestamp', y='quantity', 
                    color="side", barmode="group", nbins=50,
                    labels={
                        "quantity": "Total volume per time interval",
                        "timestamp": "Date and time"
                    })
                fig.update_xaxes(showgrid=True, gridwidth=1)
                fig.update_layout(title="Quote and trade volume", yaxis_title="Total volume per time interval", width=600, height=500)
                st.plotly_chart(fig)

        else:
            if len(isin) > 0:
                st.write("No trades found. Please try a different ISIN.")

    

