from requests import get
import pandas as pd

if __name__ == '__main__':
    base_url = 'http://www.treasurydirect.gov/TA_WS/'

    auctioned_endpt = 'securities/auctioned'

    qs = {
        'format': 'json',
        'pagesize': 250,  # Only a few auction per month but shouldn't hurt
        'type': 'Note',  # TODO: Either iterate over ['Note', 'Bond'] or remove
        'days': 365,  # On-the-run likely issued in last week but will filter
        'reopening': 'Yes',  # Schema should handle this
    }
    endpt = base_url + auctioned_endpt
    result = get(endpt, qs)
    df = pd.read_json(result.text, orient='records')

    # TODO: Create connection to database
    engine = None

    # TODO: Query database for known CUSIPS in some security table
    sec_table_name = 'foo'
    schema = 'faz'
    known_q = "SELECT * FROM {} WHERE whatever == 'baz'".format(sec_table_name)
    known_cusip_df = pd.read_sql_query(known_q, engine, schema=schema)

    # TODO: Filter out undesired columns though I'd keep everything
    cols = ['cusip', 'announcedCusip', 'corpusCusip', 'originalCusip', 'type']

    # TODO: Filter out known CUSIPS from dataframe
    known_cusip_lbl = df['cusip'].isin(known_cusip_df['cusip'])
    unknown_df = df.loc[~known_cusip_lbl, cols]

    # TODO: Append unknown CUSIPS to security table
    unknown_df.to_sql(sec_table_name, engine, schema=schema, if_exists='append')
