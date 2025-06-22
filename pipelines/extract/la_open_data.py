import pandas as pd
from sodapy import Socrata


client = Socrata("data.lacity.org", None)

def fetch_crime_data():
    """
    Fetch crime data from LA Open Data.
    """
    results = client.get("2nrs-mtv8", limit=5000)
    return pd.DataFrame.from_records(results)