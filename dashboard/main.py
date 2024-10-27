from datetime import date
from pathlib import Path

import streamlit as st
import duckdb

WAREHOUSE = "data/warehouse/tablecheck.duckdb"


def check_warehouse():
    if not Path(WAREHOUSE).exists():
        st.error("Warehouse not found. Please run the Airflow DAG first.")
        st.stop()


def fetch_restaurants():
    with duckdb.connect(WAREHOUSE) as con:
        return (
            con.sql(
                """
            SELECT restaurant_name FROM dim__tablecheck_restaurants
            """
            )
            .to_df()["restaurant_name"]
            .tolist()
        )


def fetch_restaurant_visits(restaurant: str):
    filter = "" if restaurant == "All" else f"WHERE restaurant_name = ?"
    params = []
    if restaurant != "All":
        params.append(restaurant)
    with duckdb.connect(WAREHOUSE) as con:
        return con.sql(
            f"""
            SELECT * FROM smry__tablecheck_visitor_count
            {filter}
            """,
            params=params,
        ).to_df()


def fetch_restaurant_earnings(restaurant: str):
    filter = "" if restaurant == "All" else f"WHERE restaurant_name = ?"
    params = []
    if restaurant != "All":
        params.append(restaurant)
    with duckdb.connect(WAREHOUSE) as con:
        return con.sql(
            f"""
            SELECT * FROM smry__tablecheck_restaurant_earnings
            {filter}
            """,
            params=params,
        ).to_df()


def fetch_restaurant_best_sellers(restaurant: str):
    filter = "" if restaurant == "All" else f"WHERE restaurant_name = ?"
    params = []
    if restaurant != "All":
        params.append(restaurant)
    with duckdb.connect(WAREHOUSE) as con:
        return con.sql(
            f"""
            SELECT * FROM smry__tablecheck_best_sellers
            {filter}
            """,
            params=params,
        ).to_df()


def fetch_restaurant_patrons(restaurant: str):
    filter = "" if restaurant == "All" else f"WHERE restaurant_name = ?"
    params = []
    if restaurant != "All":
        params.append(restaurant)
    with duckdb.connect(WAREHOUSE) as con:
        return con.sql(
            f"""
            SELECT * FROM smry__tablecheck_restaurant_visitors
            {filter}
            """,
            params=params,
        ).to_df()


if __name__ == "__main__":
    st.set_page_config(layout="wide")

    check_warehouse()

    "# Restaurant Stats"

    restaurant_filter = st.selectbox(
        label="Select Restaurant", options=["All"] + fetch_restaurants()
    )

    restaurant_visits = fetch_restaurant_visits(restaurant=restaurant_filter)
    restaurant_earnings = fetch_restaurant_earnings(
        restaurant=restaurant_filter
    )
    best_sellers = fetch_restaurant_best_sellers(restaurant=restaurant_filter)
    patrons = fetch_restaurant_patrons(restaurant=restaurant_filter)
    # st.dataframe(restaurant_visits)
    # st.dataframe(restaurant_earnings)
    # st.dataframe(best_sellers)
    best_selling_food = (
        best_sellers.groupby("food_name")
        .sum()
        .sort_values("total_orders", ascending=False)
    )
    best_profitable_food = (
        best_sellers.groupby("food_name")
        .sum()
        .sort_values("total_revenue", ascending=False)
    )
    most_frequent_customer = (
        patrons.groupby("first_name")
        .sum()
        .sort_values("visit_count", ascending=False)
    )
    restaurant_top_stats = [
        {
            "label": "Most Visited",
            "value": restaurant_visits.loc[
                restaurant_visits["visitors_count"].idxmax(), "restaurant_name"
            ],
            "delta": int(restaurant_visits["visitors_count"].max()),
            "delta_color": "off",
        },
        {
            "label": "Most Unique Visitors",
            "value": restaurant_visits.loc[
                restaurant_visits["unique_visitors_count"].idxmax(),
                "restaurant_name",
            ],
            "delta": int(restaurant_visits["unique_visitors_count"].max()),
            "delta_color": "off",
        },
        {
            "label": "Top Earnings",
            "value": restaurant_earnings.loc[
                restaurant_earnings["earnings"].idxmax(), "restaurant_name"
            ],
            "delta": f"${restaurant_earnings['earnings'].max()}",
            "delta_color": "off",
        },
        {
            "label": "Top Selling Food",
            "value": best_selling_food.head(1).index[0],
            "delta": int(best_selling_food.head(1).total_orders[0]),
            "delta_color": "off",
        },
        {
            "label": "Most Profitable Food",
            "value": best_profitable_food.head(1).index[0],
            "delta": f"${best_profitable_food.head(1).total_revenue[0]}",
            "delta_color": "off",
        },
        {
            "label": "Most Frequent Customer",
            "value": most_frequent_customer.head(1).index[0],
            "delta": int(most_frequent_customer.head(1).visit_count[0]),
            "delta_color": "off",
        },
    ]

    restaurant_data = [
        {
            "title": "Total Visits Per Resto",
            "data": restaurant_visits,
            "x": "restaurant_name",
            "y": "visitors_count",
            "y_label": "Total Visits",
            "x_label": "Restaurant",
        },
        {
            "title": "Total Unique Visitors Per Resto",
            "data": restaurant_visits,
            "x": "restaurant_name",
            "y": "unique_visitors_count",
            "y_label": "Total Unique Visitors",
            "x_label": "Restaurant",
        },
        {
            "title": "Total Earnings Per Resto",
            "data": restaurant_earnings,
            "x": "restaurant_name",
            "y": "earnings",
            "y_label": "Total Earnings",
            "x_label": "Restaurant",
        },
        # {
        #     "data": best_sellers,
        #     "x": "restaurant_name",
        #     "y": "total_orders",
        #     "color": "food_name",
        #     "y_label": "Total Orders",
        #     "x_label": "Restaurant",
        # },
    ]

    restaurant_tables = [
        {
            "title": "Top Selling Food Per Resto",
            "data": best_sellers.sort_values(
                "total_orders", ascending=False
            ).drop_duplicates("restaurant_name", keep="first"),
        },
        {
            "title": "Most Profitable Food Per Resto",
            "data": best_sellers.sort_values(
                "total_revenue", ascending=False
            ).drop_duplicates("restaurant_name", keep="first"),
        },
        {
            "title": "Most Frequent Customer Per Resto",
            "data": patrons.sort_values(
                "visit_count", ascending=False
            ).drop_duplicates("restaurant_name", keep="first"),
        },
        {
            "title": "Favorite Resto Per Customer",
            "data": patrons.sort_values(
                "visit_count", ascending=False
            ).drop_duplicates("first_name", keep="first").loc[
                :,
                ["first_name", "restaurant_name", "visit_count"], 
            ],
        },
    ]

    restaurant_metrics = st.columns(len(restaurant_top_stats))
    for col, metric_meta in zip(restaurant_metrics, restaurant_top_stats):
        col.metric(**metric_meta)
    restaurant_bars = st.columns(len(restaurant_data))
    for col, data in zip(restaurant_bars, restaurant_data):
        bar_title = data.pop("title")
        col.subheader(bar_title)
        col.bar_chart(**data)
    restaurant_data_tables = st.columns(len(restaurant_tables))
    for col, data in zip(restaurant_data_tables, restaurant_tables):
        table_title = data.pop("title")
        col.subheader(table_title)
        col.dataframe(data["data"], hide_index=True, height=200)
