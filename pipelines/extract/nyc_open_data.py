import pandas as pd
from sodapy import Socrata


client = Socrata("data.cityofnewyork.us", None)

def fetch_vehicle_collisions_crashes(offset=0, limit=5000):
    """
    Fetch vehicle collisions data from NYC Open Data.
    """
    results = client.get("h9gi-nx95", limit=limit, offset=offset)
    return pd.DataFrame.from_records(results)


def fetch_emergency_responses(offset=0, limit=5000):
    """
    Fetch emergency response data from NYC Open Data.
    """
    results = client.get("pasr-j7fb", limit=limit, offset=offset)
    return pd.DataFrame.from_records(results)


def fetch_arrest_data(offset=0, limit=5000):
    """
    Fetch crime data from NYC Open Data.
    """
    results = client.get("uip8-fykc", limit=limit, offset=offset)
    return pd.DataFrame.from_records(results)


if __name__ == "__main__":
    # Fetch and print the first few rows of the vehicle collisions data
    df = fetch_emergency_responses(offset=0, limit=500)
    print(df.head())
    