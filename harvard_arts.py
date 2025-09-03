import streamlit as st
import pandas as pd
import requests
import time
from sqlalchemy import create_engine

# ----------------- Streamlit Config -----------------
st.set_page_config(page_title="Harvard Art Collection", page_icon=":art:", layout="wide")
st.title("🎨 Harvard’s Artifacts Collection")

# ----------------- Database Setup -----------------
#DB_URL = "mysql+mysqldb://root:2004@localhost:3306/harvard_records"
engine = create_engine("mysql+mysqlconnector://root:2004@localhost:3306/harvard_records")

@st.cache_data
def load_classifications():
    """Load available classifications from DB."""
    try:
        with engine.connect() as conn:
            return pd.read_sql("SELECT * FROM harvard_records.`filtered_harvard_classification`", conn)
    except Exception as e:
        st.error(f"Database Error: {e}")
        return pd.DataFrame()

classification_df = load_classifications()

# ----------------- Helper Functions -----------------
def fetch_harvard_data(classification, api_key, limit=100):
    """Fetch data from Harvard Art Museums API."""
    url = "https://api.harvardartmuseums.org/object"
    records, page = [], 1

    while True:
        params = {
            "apikey": api_key,
            "size": limit,
            "page": page,
            "classification": classification,
        }
        r = requests.get(url, params=params)
        if r.status_code != 200:
            st.error("Error fetching data from API.")
            break

        data = r.json()
        recs = data.get("records", [])
        if not recs:
            break

        for rec in recs:
            rec["classification_name"] = classification
        records.extend(recs)

        if page >= data.get("info", {}).get("pages", 1):
            break

        page += 1
        time.sleep(0.2)

    return records


def process_records(records):
    """Transform raw API records into structured DataFrames."""
    seen, unique = set(), []
    for rec in records:
        obj_id = rec.get("id")
        if obj_id and obj_id not in seen:
            seen.add(obj_id)
            unique.append(rec)

    # Metadata
    meta_df = pd.DataFrame([{
        "id": rec.get("id"),
        "title": rec.get("title"),
        "culture": rec.get("culture"),
        "period": rec.get("period"),
        "century": rec.get("century"),
        "medium": rec.get("medium"),
        "dimensions": rec.get("dimensions"),
        "description": rec.get("description"),
        "department": rec.get("department"),
        "classification": rec.get("classification"),
        "accessionyear": rec.get("accessionyear"),
        "accessionmethod": rec.get("accessionmethod"),
    } for rec in unique])

    # Media
    media_df = pd.DataFrame([{
        "objectid": rec.get("id"),
        "imagecount": rec.get("imagecount"),
        "mediacount": rec.get("mediacount"),
        "colorcount": rec.get("colorcount"),
        "mediarank": rec.get("rank"),
        "datebegin": rec.get("datebegin"),
        "dateend": rec.get("dateend"),
    } for rec in unique])

    # Colors
    color_records = []
    for rec in unique:
        if isinstance(rec.get("colors"), list):
            for c in rec["colors"]:
                color_records.append({
                    "objectid": rec.get("id"),
                    "color": c.get("color"),
                    "spectrum": c.get("spectrum"),
                    "hue": c.get("hue"),
                    "percent": c.get("percent"),
                    "css3": c.get("css3"),
                })
    color_df = pd.DataFrame(color_records)

    return meta_df, media_df, color_df


def insert_into_db(meta_df, media_df, color_df):
    """Insert processed DataFrames into MySQL."""
    with st.spinner("Inserting data into database..."):
        try:
            with engine.connect() as conn:
                if not meta_df.empty:
                    meta_df.to_sql("artifact_metadata", con=conn, if_exists="append", index=False)
                if not media_df.empty:
                    media_df.to_sql("artifact_media", con=conn, if_exists="append", index=False)
                if not color_df.empty:
                    color_df.to_sql("artifact_colors", con=conn, if_exists="append", index=False)
                st.success("✅ Data inserted into database successfully.")
        except Exception as e:
            st.error(f"❌ Error inserting data: {e}")

# ----------------- Streamlit Tabs -----------------
tab1, tab2,tab3 = st.tabs(["Data Collection", "Data Insertion","SQL Queries"])

with tab1:
    st.header("Data Collection from Harvard Art Museums API")
    st.markdown("Select a classification and fetch related artifact data.")

    selected_option = st.selectbox(
        "📌 Choose classification",
        options=classification_df['name'].unique() if not classification_df.empty else [],
        placeholder="Select one classification..."
    )

    if st.button("🔍 Collect data",type = "primary"):
        API_KEY = "56bf439b-dbbc-4535-bfdf-39617a16d185"
        with st.spinner("Fetching data..."):
            records = fetch_harvard_data(selected_option, API_KEY)

        if records:
            meta_df, media_df, color_df = process_records(records)

            # Save to session state
            st.session_state["meta_df"] = meta_df
            st.session_state["media_df"] = media_df
            st.session_state["color_df"] = color_df

            st.success(f"✅ Collected {len(records)} records for classification: {selected_option}")

            st.subheader("Preview Data")
            with st.expander("Metadata Sample"):
                st.dataframe(meta_df.head())
            with st.expander("Media Sample"):
                st.dataframe(media_df.head())
            with st.expander("Colors Sample"):
                st.dataframe(color_df.head())
        else:
            st.warning("No records found.")

with tab2:
    st.header("Data Insertion into MySQL Database")
    st.markdown("This tab is for inserting previously collected data into the database.")
    st.info("Data insertion is now handled in the Data Collection tab after fetching and processing data.")
    
    if st.button("💾 Insert into Database", type="primary"):
        if "meta_df" in st.session_state:
            insert_into_db(st.session_state["meta_df"],
                        st.session_state["media_df"],
                        st.session_state["color_df"])
        else:
            st.error("No data available. Please collect data first in Tab 1.")
with tab3:
    st.header("Execute SQL Queries")
    st.markdown("Run custom SQL queries against the Harvard Artifacts database.")

    query = st.text_area("Enter your SQL query here:", height=150, placeholder="e.g., SELECT * FROM artifact_metadata LIMIT 10;")

    if st.button("Run Query", type="primary"):
        if query.strip():
            try:
                with engine.connect() as conn:
                    result_df = pd.read_sql(query, conn)
                    st.success("✅ Query executed successfully.")
                    st.dataframe(result_df)
            except Exception as e:
                st.error(f"❌ Query Error: {e}")
        else:
            st.warning("Please enter a valid SQL query.")            
