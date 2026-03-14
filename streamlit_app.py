import streamlit as st
import subprocess
import os
import datetime
import pandas as pd
import sqlite3
import plotly.express as px

DB_FILE = "survivalrun.db"  # file with all information necessary for the app to run
UPDATE_SCRIPT = "Databaseupdater.py"  # your script that generates/updates the DB

# ============================
# Database update helpers
# ============================

def last_sunday_21():
    now = datetime.datetime.now()
    days_since_sunday = (now.weekday() + 1) % 7  # Monday=0, Sunday=6
    last_sunday = now - datetime.timedelta(days=days_since_sunday)
    return last_sunday.replace(hour=21, minute=0, second=0, microsecond=0)

def needs_update(db_file):
    if not os.path.exists(db_file):
        return True
    last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(db_file))
    return last_modified < last_sunday_21()

def update_db():
    """Run the database updater synchronously with a spinner."""
    try:
        with st.spinner("Updating database… This may take a few moments."):
            # Run the updater script synchronously
            subprocess.run(["python", UPDATE_SCRIPT], check=True)
    except subprocess.CalledProcessError as e:
        st.error(f"Database update failed: {e}")
    except Exception as e:
        st.error(f"Unexpected error during update: {e}")

# ============================
# Run update if needed
# ============================
if needs_update(DB_FILE):
    update_db()

# ============================
# Database helpers
# ============================

def get_connection():
    return sqlite3.connect(DB_FILE)

def get_available_years():
    conn = get_connection()
    query = """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name LIKE 'alle_uitslagen_%'
    """
    tables = pd.read_sql(query, conn)["name"].tolist()
    conn.close()
    return sorted(int(t.split("_")[-1]) for t in tables)

def build_union_query(years, where_clause="", params=None):
    queries = []
    all_params = []

    for year in years:
        q = f"SELECT * FROM alle_uitslagen_{year}"
        if where_clause:
            q += f" WHERE {where_clause}"
            if params:
                all_params.extend(params)
        queries.append(q)

    return " UNION ALL ".join(queries), all_params

# ============================
# Helpers
# ============================

def normalize_name(name: str) -> str:
    return " ".join(name.lower().strip().split())

def athlete_to_colname(name: str) -> str:
    return name.replace(" ", "_")

# ============================
# Data access
# ============================

def get_name_suggestions(input_text, years):
    if not input_text or not years:
        return []

    conn = get_connection()
    query, params = build_union_query(
        years,
        where_clause="voornaam LIKE ? OR achternaam LIKE ?",
        params=[f"{input_text}%", f"{input_text}%"]
    )

    sql = f"""
        SELECT DISTINCT voornaam, achternaam
        FROM ({query})
        ORDER BY achternaam, voornaam
        LIMIT 20
    """
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return [f"{r['voornaam']} {r['achternaam']}" for _, r in df.iterrows()]

def get_athlete_data(names, years):
    if not names or not years:
        return pd.DataFrame()

    conn = get_connection()
    placeholders = ",".join(["?"] * len(names))
    norm_names = [normalize_name(n) for n in names]

    query, params = build_union_query(
        years,
        where_clause="lower(voornaam || ' ' || achternaam) IN (" + placeholders + ")",
        params=norm_names
    )

    df = pd.read_sql(query, conn, params=params)
    conn.close()

    df["athlete"] = df["voornaam"].str.strip() + " " + df["achternaam"].str.strip()

    if "looptijd_min" in df.columns:
        df["looptijd_min"] = df["looptijd_min"].astype(float)
        df["looptijd_display"] = pd.to_timedelta(df["looptijd_min"], unit="m")
        df["looptijd_display"] = df["looptijd_display"].apply(
            lambda td: f"{int(td.total_seconds() // 3600):02d}:"
                       f"{int((td.total_seconds() % 3600) // 60):02d}:"
                       f"{int(td.total_seconds() % 60):02d}"
        )

    df["datum"] = pd.to_datetime(df["datum"], errors="coerce")
    df = df.sort_values(["datum", "run_type", "plaats", "voornaam", "achternaam"])
    df["run_id"] = df.groupby(["datum", "run_type", "plaats"]).cumcount()
    return df

# ============================
# Session state
# ============================

if "selected_athletes" not in st.session_state:
    st.session_state.selected_athletes = []

# ============================
# UI
# ============================
st.title("Survivalrundata Dashboard")

available_years = get_available_years()
if not available_years:
    st.warning("Database not yet available. It may be updating in the background.")
    st.stop()

min_year, max_year = min(available_years), max(available_years)

mode = st.radio(
    "Select mode:",
    ["Compare Athletes", "Individual Dashboard"],
    key="mode_selector"
)

# ============================
# Compare Athletes
# ============================

if mode == "Compare Athletes":
    st.subheader("Compare athletes")
    compare_year_range = st.slider(
        "Select year range (comparison)",
        min_value=min_year,
        max_value=max_year,
        value=(min_year, max_year),
        step=1,
        key="compare_years"
    )

    years_compare = [y for y in available_years if compare_year_range[0] <= y <= compare_year_range[1]]

    col1, col2 = st.columns([3, 1])
    with col1:
        typed_name = st.text_input(
            "Search athlete (type first or last name)",
            placeholder="Start typing a name...",
            key="compare_input"
        )
        suggestions = get_name_suggestions(typed_name, years_compare) if typed_name else []
        selected_to_add = st.selectbox(
            "Matching athletes",
            options=[""] + suggestions,
            label_visibility="collapsed",
            key="compare_select"
        )

    with col2:
        if st.button("➕ Add athlete", disabled=not selected_to_add, key="compare_add_button"):
            if selected_to_add not in st.session_state.selected_athletes:
                st.session_state.selected_athletes.append(selected_to_add)

    if st.session_state.selected_athletes:
        st.markdown("### Selected athletes")
        cols = st.columns(len(st.session_state.selected_athletes))
        for i, athlete in enumerate(st.session_state.selected_athletes):
            with cols[i]:
                st.button(
                    f"❌ {athlete}",
                    key=f"remove_{i}_{athlete}",
                    on_click=lambda a=athlete: st.session_state.selected_athletes.remove(a)
                )
        if st.button("🧹 Clear all athletes", key="compare_clear_all"):
            st.session_state.selected_athletes.clear()

    if len(st.session_state.selected_athletes) >= 2 and years_compare:
        df = get_athlete_data(st.session_state.selected_athletes, years_compare)
        if not df.empty:
            all_combinations = df[['datum', 'run_type', 'plaats']].drop_duplicates().sort_values(['datum', 'run_type', 'plaats'])
            comparison_data = {'datum': [], 'run_type': [], 'plaats': []}
            athletes = st.session_state.selected_athletes
            for athlete in athletes:
                comparison_data[f"{athlete} - punten"] = []
                comparison_data[f"{athlete} - positie"] = []
                comparison_data[f"{athlete} - looptijd_display"] = []
                comparison_data[f"{athlete} - hindernis gemist"] = []

            def safe_value(value, metric):
                if pd.isna(value):
                    return '-'
                if metric == 'looptijd_display' and isinstance(value, str) and ':' in value:
                    return value
                elif metric == 'looptijd_display' and pd.notna(value):
                    total_sec = value * 60
                    return f"{int(total_sec // 3600):02d}:{int((total_sec % 3600) // 60):02d}:{int(total_sec % 60):02d}"
                return str(value) if pd.notna(value) else '-'

            for idx, event in all_combinations.iterrows():
                comparison_data['datum'].append(event['datum'])
                comparison_data['run_type'].append(event['run_type'])
                comparison_data['plaats'].append(event['plaats'])
                for athlete in athletes:
                    mask = (
                        (df['datum'] == event['datum']) &
                        (df['run_type'] == event['run_type']) &
                        (df['plaats'] == event['plaats']) &
                        (df['athlete'] == athlete)
                    )
                    if mask.any():
                        row = df[mask].iloc[0]
                        comparison_data[f"{athlete} - punten"].append(safe_value(row.get('punten'), 'punten'))
                        comparison_data[f"{athlete} - positie"].append(safe_value(row.get('positie'), 'positie'))
                        comparison_data[f"{athlete} - looptijd_display"].append(safe_value(row.get('looptijd_display'), 'looptijd_display'))
                        comparison_data[f"{athlete} - hindernis gemist"].append(safe_value(row.get('hindernis_gemist'), 'hindernis_gemist'))
                    else:
                        comparison_data[f"{athlete} - punten"].append('-')
                        comparison_data[f"{athlete} - positie"].append('-')
                        comparison_data[f"{athlete} - looptijd_display"].append('-')
                        comparison_data[f"{athlete} - hindernis gemist"].append('-')

            comparison_df = pd.DataFrame(comparison_data)
            st.divider()
            only_complete = st.toggle("Show only runs where ALL athletes have data", value=False, key="compare_only_complete")
            if only_complete:
                athlete_cols = [f"{athlete} - punten" for athlete in athletes]
                complete_mask = True
                for col in athlete_cols:
                    complete_mask &= (comparison_df[col] != '-')
                filtered_df = comparison_df[complete_mask]
                if filtered_df.empty:
                    st.info("No events where all selected athletes participated together.")
                else:
                    st.dataframe(filtered_df, width="stretch", hide_index=True)
            else:
                st.dataframe(comparison_df, width="stretch", hide_index=True)
        else:
            st.info("No data found for selected athletes.")
    elif len(st.session_state.selected_athletes) < 2:
        st.info("Add at least two athletes.")
    else:
        st.warning("No data tables for selected years.")

# ============================
# Individual Dashboard
# ============================

else:
    st.subheader("Individual Athlete Dashboard")
    individual_year_range = st.slider(
        "Select year range (individual)",
        min_value=min_year,
        max_value=max_year,
        value=(min_year, max_year),
        step=1,
        key="individual_years"
    )
    years_individual = [y for y in available_years if individual_year_range[0] <= y <= individual_year_range[1]]

    typed_name = st.text_input("Type letters to search athlete", key="individual_input")
    suggestions = get_name_suggestions(typed_name, years_individual) if typed_name and years_individual else []
    athlete_name = st.selectbox("Click to select athlete", options=[""] + suggestions, key="individual_select")

    if athlete_name and years_individual:
        df = get_athlete_data([athlete_name], years_individual)
        table_cols = ['datum', 'run_type', 'run_omschrijving', 'subcategorie', 'positie', 'looptijd_display', 'punten', 'startnr', 'hindernis_gemist', 'plaats']
        st.dataframe(df[table_cols].sort_values(['datum', 'run_type']), width="stretch")

        fig1 = px.line(df, x='datum', y='punten', title=f"Punten over time: {athlete_name}")
        st.plotly_chart(fig1)

        run_types = df['run_type'].dropna().unique()
        selected_run_type = st.selectbox("Select Run Type", run_types, key="individual_run_type")
        df_filtered = df[df['run_type'] == selected_run_type].sort_values(['datum'])
        fig2 = px.line(df_filtered, x='datum', y='looptijd_min', title=f"Looptijd over time ({selected_run_type})", labels={"looptijd_min": "Looptijd (min)"})
        fig2.update_traces(hovertemplate="Datum=%{x}<br>Looptijd=%{customdata}", customdata=df_filtered["looptijd_display"])
        st.plotly_chart(fig2)
    elif athlete_name and not years_individual:
        st.warning("No data tables exist for the selected year range.")
