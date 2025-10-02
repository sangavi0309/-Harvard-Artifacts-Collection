import streamlit as st
import pandas as pd
import requests
import time
from sqlalchemy import create_engine
import mysql.connector

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
    # Tab 3: SQL Queries
    st.header("Run SQL Queries")
    with st.container():
            conn = mysql.connector.connect(
                host="localhost",
                user="root",
                password="2004",
                database="harvard_records"
            )
            cursor = conn.cursor(dictionary=True)
            # do something with cursor

    query_dict = {
        # --- artifact_metadata Queries ---
        "List all artifacts from the 11th century belonging to Byzantine culture.": """
            SELECT * FROM artifact_metadata 
            WHERE century='11th century' AND culture='Byzantine'
        """,
        "What are the unique cultures represented in the artifacts?": """
            SELECT DISTINCT culture FROM artifact_metadata
        """,
        "List all artifacts from the Archaic Period.": """
            SELECT * FROM artifact_metadata 
            WHERE period='Archaic Period'
        """,
        "List artifact titles ordered by accession year in descending order.": """
            SELECT title, accessionyear FROM artifact_metadata 
            ORDER BY accessionyear DESC
        """,
        "How many artifacts are there per department?": """
            SELECT department, COUNT(*) AS total 
            FROM artifact_metadata 
            GROUP BY department
        """,

        # --- artifact_media Queries ---
        "Which artifacts have more than 1 image?": """
            SELECT * FROM artifact_media 
            WHERE imagecount > 1
        """,
        "What is the average rank of all artifacts?": """
            SELECT AVG(rank) AS average_rank FROM artifact_media
        """,
        "Which artifacts have a higher colorcount than mediacount?": """
            SELECT * FROM artifact_media 
            WHERE colorcount > mediacount
        """,
        "List all artifacts created between 1500 and 1600.": """
            SELECT * FROM artifact_media 
            WHERE datebegin >= 1500 AND dateend <= 1600
        """,
        "How many artifacts have no media files?": """
            SELECT COUNT(*) AS no_media_count 
            FROM artifact_media 
            WHERE mediacount = 0
        """,

        # --- artifact_colors Queries ---
        "What are all the distinct hues used in the dataset?": """
            SELECT DISTINCT hue FROM artifact_colors
        """,
        "What are the top 5 most used colors by frequency?": """
            SELECT color, COUNT(*) AS frequency 
            FROM artifact_colors 
            GROUP BY color 
            ORDER BY frequency DESC 
            LIMIT 5
        """,
        "What is the average coverage percentage for each hue?": """
            SELECT hue, AVG(percent) AS avg_coverage 
            FROM artifact_colors 
            GROUP BY hue
        """,
        "List all colors used for a given artifact ID.": """
            SELECT * FROM artifact_colors 
            WHERE objectid = %s
        """,
        "What is the total number of color entries in the dataset?": """
            SELECT COUNT(*) AS total_colors FROM artifact_colors
        """,

        # --- Join-Based Queries ---
        "List artifact titles and hues for all artifacts belonging to the Byzantine culture.": """
            SELECT m.title, c.hue 
            FROM artifact_metadata m 
            JOIN artifact_colors c ON m.id = c.objectid 
            WHERE m.culture = 'Byzantine'
        """,
        "List each artifact title with its associated hues.": """
            SELECT m.title, c.hue 
            FROM artifact_metadata m 
            JOIN artifact_colors c ON m.id = c.objectid
        """,
        "Get artifact titles, cultures, and media ranks where the period is not null.": """
            SELECT m.title, m.culture, me.rank 
            FROM artifact_metadata m 
            JOIN artifact_media me ON m.id = me.objectid 
            WHERE m.period IS NOT NULL
        """,
        "Find artifact titles ranked in the top 10 that include the color hue 'Grey'.": """
            SELECT m.title, me.rank, c.hue 
            FROM artifact_metadata m 
            JOIN artifact_media me ON m.id = me.objectid 
            JOIN artifact_colors c ON m.id = c.objectid 
            WHERE c.hue = 'Grey' 
            ORDER BY me.rank DESC 
            LIMIT 10
        """,
        "How many artifacts exist per classification, and what is the average media count for each?": """
            SELECT m.classification, COUNT(*) AS total, AVG(me.mediacount) AS avg_media 
            FROM artifact_metadata m 
            JOIN artifact_media me ON m.id = me.objectid 
            GROUP BY m.classification
        """,

        # --- Own SQL Queries for Deeper Insights ---
        "Most Common Mediums Used Across Artifacts": """
            SELECT medium, COUNT(*) AS count
            FROM artifact_metadata
            GROUP BY medium
            ORDER BY count DESC
            LIMIT 10
        """,
        "Artifacts with Missing Descriptions": """
            SELECT id, title, culture, classification
            FROM artifact_metadata
            WHERE description IS NULL OR description = ''
        """,
        "Average Accession Year by Classification": """
            SELECT classification, AVG(accessionyear) AS avg_year
            FROM artifact_metadata
            GROUP BY classification
            ORDER BY avg_year DESC
        """,
        "Artifacts with Longest Time Span Between Creation Dates": """
            SELECT objectid, datebegin, dateend, (dateend - datebegin) AS duration
            FROM artifact_media
            ORDER BY duration DESC
            LIMIT 10
        """,
        "Top 5 Departments by Artifact Count": """
            SELECT department, COUNT(*) AS total
            FROM artifact_metadata
            GROUP BY department
            ORDER BY total DESC
            LIMIT 5
        """,
        "Most Frequently Used Hue Across All Artifacts": """
            SELECT hue, COUNT(*) AS frequency
            FROM artifact_colors
            GROUP BY hue
            ORDER BY frequency DESC
            LIMIT 1
        """
    }

    query_titles = list(query_dict.keys())
    selected_query = st.selectbox("Choose a query", query_titles)

    if "%s" in query_dict[selected_query]:
        artifact_id = st.number_input("Enter Artifact ID", min_value=1)
        cursor.execute(query_dict[selected_query], (artifact_id,))
    else:
        cursor.execute(query_dict[selected_query])

    df_result = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)

    # Show executed SQL
    st.subheader("SQL Query Executed")
    st.code(query_dict[selected_query], language="sql")

    # Show results
    st.subheader(f"📊 Results for: {selected_query}")
    st.dataframe(df_result)

cursor.close()
conn.close()
