import pandas as pd
from sodapy import Socrata


client = Socrata("data.cityofnewyork.us", None)

def fetch_vehicle_collisions_crashes():
    """
    Fetch vehicle collisions data from NYC Open Data.
    """
    results = client.get("h9gi-nx95", limit=5000)
    return pd.DataFrame.from_records(results)


def fetch_emergency_responses():
    """
    Fetch emergency response data from NYC Open Data.
    """
    results = client.get("pasr-j7fb", limit=5000)
    return pd.DataFrame.from_records(results)


def fetch_arrest_data():
    """
    Fetch crime data from NYC Open Data.
    """
    results = client.get("uip8-fykc", limit=5000)
    return pd.DataFrame.from_records(results)


if __name__ == "__main__":
    # Fetch and print the first few rows of the vehicle collisions data
    df = fetch_emergency_responses()
    print(df.head())
    