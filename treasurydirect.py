import io

import pandas as pd
from requests import get

COLUMNS = ["cusip", "announcedCusip", "corpusCusip", "originalCusip", "type"]


def read_auctions(text):
    return pd.read_json(io.StringIO(text), orient="records")


def unknown_securities(df, known_cusip_df, cols=COLUMNS):
    """Auctioned securities whose CUSIP isn't stored yet."""
    known_cusip_lbl = df["cusip"].isin(known_cusip_df["cusip"])
    return df.loc[~known_cusip_lbl, cols]


if __name__ == "__main__":
    base_url = "http://www.treasurydirect.gov/TA_WS/"

    auctioned_endpt = "securities/auctioned"

    qs: dict[str, str | int] = {
        "format": "json",
        "pagesize": 250,  # Only a few auction per month but shouldn't hurt
        "type": "Note",  # TODO: Either iterate over ['Note', 'Bond'] or remove
        "days": 365,  # On-the-run likely issued in last week but will filter
        "reopening": "Yes",  # Schema should handle this
    }
    endpt = base_url + auctioned_endpt
    result = get(endpt, qs)
    df = read_auctions(result.text)

    # TODO: Create connection to database
    engine = None

    # TODO: Query database for known CUSIPS in some security table
    sec_table_name = "foo"
    schema = "faz"
    known_q = f"SELECT * FROM {sec_table_name} WHERE whatever == 'baz'"
    known_cusip_df = pd.read_sql_query(known_q, engine, schema=schema)

    # TODO: Filter out undesired columns (COLUMNS) though I'd keep everything
    unknown_df = unknown_securities(df, known_cusip_df)

    # TODO: Append unknown CUSIPS to security table
    unknown_df.to_sql(sec_table_name, engine, schema=schema, if_exists="append")
