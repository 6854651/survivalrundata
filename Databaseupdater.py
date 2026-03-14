import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed


# -------------------- CONFIG --------------------

START_JAAR = 2024
EIND_JAAR = 2025

BASE_URL = "https://www.uvponline.nl/uvponlineU/index.php/uvproot/wedstrijdschema"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

session = requests.Session()
session.headers.update(HEADERS)


# -------------------- HELPERS --------------------

def get(url):
    """Request helper with error handling."""
    try:
        r = session.get(url)
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        print(f"❌ Request failed: {url} -> {e}")
        return None


def text(el):
    """Safely extract text."""
    return el.get_text(strip=True) if el else ""


def normalize_name(name):
    """Normalize names."""
    if not name:
        return ""
    return " ".join(name.strip().lower().split())


# -------------------- SCRAPER 1: WEDSTRIJDEN --------------------

def scrape_wedstrijdkalender(jaar):

    url = f"{BASE_URL}/{jaar}"
    html = get(url)

    if not html:
        return pd.DataFrame()

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="wedstrijdagenda")

    wedstrijden = []

    for row in table.find_all("tr")[1:]:

        datum = text(row.find("td", class_="wedstrijddatum"))
        plaats = text(row.find("td", class_="wedstrijdplaats"))

        organisator = ""
        organisator_link = ""

        org_td = row.find("td", id="wedstrijdlink")
        if org_td and org_td.find("a"):
            organisator = text(org_td)
            organisator_link = urljoin(url, org_td.find("a")["href"])

        uitslag_link = ""
        uitslag_td = row.find("td", class_="uitslaglink")
        if uitslag_td and uitslag_td.find("a"):
            uitslag_link = urljoin(url, uitslag_td.find("a")["href"])

        inschrijving_link = ""
        starttijden_link = ""

        tds = row.find_all("td")
        if len(tds) >= 2:

            link_td = tds[-2]

            ins_div = link_td.find("div", class_="inschrijflink")
            if ins_div and ins_div.find("a"):
                inschrijving_link = urljoin(url, ins_div.find("a")["href"])

            start_div = link_td.find("div", class_="eventinfo_link")
            if start_div and start_div.find("a"):
                starttijden_link = urljoin(url, start_div.find("a")["href"])

        wedstrijden.append({
            "datum": datum,
            "plaats": plaats,
            "organisator": organisator,
            "organisator_link": organisator_link,
            "inschrijving_link": inschrijving_link,
            "starttijden_link": starttijden_link,
            "uitslag_link": uitslag_link
        })

    return pd.DataFrame(wedstrijden)


# -------------------- SCRAPER 2: CATEGORIE LINKS --------------------

def scrape_links(df_wedstrijden):

    def process_row(row):

        url = row["uitslag_link"]

        if not url or pd.isna(url):
            return []

        html = get(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")

        results = []

        for hrow in soup.select("div.tbl-border.toon_uitslag > div.hrow"):

            run_type = text(hrow.select_one("div.wcatname"))
            run_omschrijving = text(hrow.select_one("div.wcatomschr"))

            for link in hrow.select("div.w8 div.center a"):

                results.append({
                    "datum": row["datum"],
                    "plaats": row["plaats"],
                    "organisator": row["organisator"],
                    "run_type": run_type,
                    "run_omschrijving": run_omschrijving,
                    "subcategorie": text(link),
                    "categorie_link": urljoin(url, link["href"])
                })

        return results


    alle_links = []

    with ThreadPoolExecutor(max_workers=10) as executor:

        futures = [
            executor.submit(process_row, row)
            for _, row in df_wedstrijden.iterrows()
        ]

        for future in as_completed(futures):
            alle_links.extend(future.result())

    return pd.DataFrame(alle_links)

# -------------------- SCRAPER 3: UITSLAGEN --------------------

def scrape_results(df_links):

    def process_row(row):

        url = row["categorie_link"]

        html = get(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")

        results = []

        for div in soup.select("div.tbl1.uitslagcatrow, div.tbl2.uitslagcatrow"):

            positie = text(div.select_one(".wpos"))
            voornaam = normalize_name(text(div.select_one(".wvnaam")))

            achternaam_tag = div.select_one(".wnaam a")
            achternaam = normalize_name(text(achternaam_tag))

            woonplaats = ""
            wp = div.select(".wnaam")
            if len(wp) >= 2:
                woonplaats = text(wp[1])

            vet = text(div.select_one(".wvet"))

            startnr_div = div.select_one(".wtijd div.center")
            startnr = text(startnr_div)

            looptijd = ""
            for t in div.select(".wtijd"):
                if not t.find("div"):
                    looptijd = text(t)
                    break

            hindernis = text(div.select_one(".wtijd.center"))
            punten = text(div.select_one(".wtijd.right"))

            lid_link = achternaam_tag["href"] if achternaam_tag else ""

            results.append({
                "datum": row["datum"],
                "plaats": row["plaats"],
                "organisator": row["organisator"],
                "run_type": row["run_type"],
                "run_omschrijving": row["run_omschrijving"],
                "subcategorie": row["subcategorie"],
                "positie": positie,
                "voornaam": voornaam,
                "achternaam": achternaam,
                "woonplaats": woonplaats,
                "vet": vet,
                "startnr": startnr,
                "looptijd": looptijd,
                "hindernis_gemist": hindernis,
                "punten": punten,
                "lid_link": lid_link
            })

        return results


    alle_uitslagen = []

    with ThreadPoolExecutor(max_workers=15) as executor:

        futures = [
            executor.submit(process_row, row)
            for _, row in df_links.iterrows()
        ]

        for future in as_completed(futures):
            alle_uitslagen.extend(future.result())


    df = pd.DataFrame(alle_uitslagen)

    cols = [
        "voornaam", "achternaam", "woonplaats",
        "startnr", "looptijd", "hindernis_gemist",
        "punten", "lid_link"
    ]

    df[cols] = df[cols].replace("", pd.NA)

    df = df.drop_duplicates(
        subset=["voornaam", "achternaam", "startnr", "subcategorie"]
    )

    df = df.dropna(subset=["voornaam", "achternaam", "woonplaats"])

    return df

# -------------------- PIPELINE --------------------

def run_for_year(jaar):

    print(f"\n===== START JAAR {jaar} =====")

    print("▶ Kalender scrapen")
    df_wed = scrape_wedstrijdkalender(jaar)
    df_wed.to_csv(f"wedstrijden_{jaar}.csv", index=False)

    print("▶ Categorie-links scrapen")
    df_links = scrape_links(df_wed)
    df_links.to_csv(f"uitslagen_links_{jaar}.csv", index=False)

    print("▶ Resultaten scrapen")
    df_res = scrape_results(df_links)
    df_res.to_csv(f"uitslagen_{jaar}.csv", index=False)

    print(f"✅ {len(df_res)} resultaten opgeslagen")


# -------------------- MAIN --------------------

if __name__ == "__main__":

    for jaar in range(START_JAAR, EIND_JAAR + 1):

        try:
            run_for_year(jaar)
        except Exception as e:
            print(f"❌ Fout in jaar {jaar}: {e}")

import pandas as pd
import os
from sqlalchemy import create_engine

# ============================
# SETTINGS for database connection and file paths
# ============================

USE_POSTGRES = False   # False = local SQLite | True = Supabase

SQLITE_DB = "survivalrun.db"

DATABASE_URL = "postgresql://postgres:Beltrumselus!@db.gcclzjppvyrdcjqsmrbi.supabase.co:5432/postgres"


# ============================
# DATABASE CONNECTION
# ============================

if USE_POSTGRES:
    engine = create_engine(DATABASE_URL)
    print("Connected to Supabase PostgreSQL")
else:
    engine = create_engine(f"sqlite:///{SQLITE_DB}")
    print("Connected to local SQLite database")


# ============================
# HELPER FUNCTIONS
# ============================

def parse_datum(d):
    """Convert DD-MM-YYYY → YYYY-MM-DD"""
    return pd.to_datetime(d, format="%d-%m-%Y", errors="coerce").dt.date


def looptijd_to_minutes(t):
    """Convert HH:MM:SS → total minutes"""
    if pd.isna(t):
        return None
    try:
        h, m, s = map(int, t.split(":"))
        return h * 60 + m + s / 60
    except:
        return None


# ============================
# IMPORT LOOP
# ============================

for jaar in range(START_JAAR, EIND_JAAR + 1):

    print(f"\n▶ Importing year {jaar}")

    uitslagen_csv = f"uitslagen_{jaar}.csv"
    wedstrijden_csv = f"wedstrijden_{jaar}.csv"
    links_csv = f"uitslagen_links_{jaar}.csv"

    if not os.path.exists(uitslagen_csv):
        print(f"⚠️ Skipping {jaar}: CSV files missing")
        continue

    try:

        # ============================
        # READ CSV
        # ============================

        df = pd.read_csv(uitslagen_csv)
        df2 = pd.read_csv(wedstrijden_csv)
        df3 = pd.read_csv(links_csv)

        # ============================
        # TRANSFORM DATA
        # ============================

        df["datum"] = parse_datum(df["datum"])
        df["positie"] = pd.to_numeric(df["positie"], errors="coerce").astype("Int64")
        df["punten"] = pd.to_numeric(df["punten"], errors="coerce")
        df["hindernis_gemist"] = pd.to_numeric(df["hindernis_gemist"], errors="coerce").astype("Int64")
        df["startnr"] = pd.to_numeric(df["startnr"], errors="coerce").astype("Int64")

        # Convert time
        df["looptijd_min"] = df["looptijd"].apply(looptijd_to_minutes)
        df["looptijd"] = df["looptijd_min"].apply(lambda m: pd.to_timedelta(m, unit="m"))

        # Add year
        df["jaar"] = jaar
        df2["jaar"] = jaar
        df3["jaar"] = jaar

        # ============================
        # WRITE TO DATABASE
        # ============================

        df.to_sql(f"alle_uitslagen_{jaar}", engine, if_exists="replace", index=False)
        df2.to_sql(f"wedstrijden_{jaar}", engine, if_exists="replace", index=False)
        df3.to_sql(f"uitslagen_links_{jaar}", engine, if_exists="replace", index=False)

        print(f"✅ Year {jaar} imported")

    except Exception as e:
        print(f"❌ Error importing {jaar}: {e}")


print("\n🎉 All available years imported successfully")

