import pandas as pd
from sodapy import Socrata


client = Socrata("data.lacity.org", None)

def fetch_crime_data(offset=0, limit=5000):
    """
    Fetch crime data from LA Open Data.
    """
    results = client.get("2nrs-mtv8", limit=limit, offset=offset)
    return pd.DataFrame.from_records(results)

def fetch_traffic_collisions(offset=0, limit=5000):
    """
    Fetch traffic collisions data from LA Open Data.
    """
    results = client.get("d5tf-ez2w", limit=limit, offset=offset)
    return pd.DataFrame.from_records(results)


if __name__ == "__main__":
    # Fetch and print the first few rows of the crime data
    df = fetch_crime_data()
    print(df.head())
    
    # Fetch and print the first few rows of the traffic collisions data
    df_traffic = fetch_traffic_collisions()
    print(df_traffic.head())