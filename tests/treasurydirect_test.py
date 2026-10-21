import json

import pandas as pd

import treasurydirect

AUCTIONS = [
    {
        "cusip": "91282CAA9",
        "announcedCusip": "91282CAA9",
        "corpusCusip": "912821AA1",
        "originalCusip": "",
        "type": "Note",
        "securityTerm": "2-Year",
    },
    {
        "cusip": "91282CBB7",
        "announcedCusip": "91282CBB7",
        "corpusCusip": "912821BB8",
        "originalCusip": "",
        "type": "Note",
        "securityTerm": "5-Year",
    },
]


def test_read_auctions_parses_records():
    df = treasurydirect.read_auctions(json.dumps(AUCTIONS))
    assert list(df["cusip"]) == ["91282CAA9", "91282CBB7"]
    assert "securityTerm" in df.columns


def test_unknown_securities_drops_stored_cusips():
    df = treasurydirect.read_auctions(json.dumps(AUCTIONS))
    known = pd.DataFrame({"cusip": ["91282CAA9"]})
    unknown = treasurydirect.unknown_securities(df, known)
    assert list(unknown["cusip"]) == ["91282CBB7"]
    assert list(unknown.columns) == treasurydirect.COLUMNS


def test_unknown_securities_with_empty_table():
    df = treasurydirect.read_auctions(json.dumps(AUCTIONS))
    known = pd.DataFrame({"cusip": []})
    assert len(treasurydirect.unknown_securities(df, known)) == 2
