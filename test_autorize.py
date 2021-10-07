import io
import json
import sys

import autorize


def test_input_operations(monkeypatch):
    input = """{"account": {"active-card": true, "available-limit": 100}}
{"transaction": {"merchant": "Burger King", "amount": 10, "time": "2019-02-13T10:00:00.000Z"}}
{"transaction": {"merchant": "Habbib's", "amount": 20, "time": "2019-02-13T11:00:00.000Z"}}
{"transaction": {"merchant": "McDonald's", "amount": 30, "time": "2019-02-13T12:00:00.000Z"}}"""
    monkeypatch.setattr("sys.stdin", io.StringIO(input))
    expected_output = [
        {"account": {"active-card": True, "available-limit": 100}},
        {
            "transaction": {
                "merchant": "Burger King",
                "amount": 10,
                "time": "2019-02-13T10:00:00.000Z",
            }
        },
        {
            "transaction": {
                "merchant": "Habbib's",
                "amount": 20,
                "time": "2019-02-13T11:00:00.000Z",
            }
        },
        {
            "transaction": {
                "merchant": "McDonald's",
                "amount": 30,
                "time": "2019-02-13T12:00:00.000Z",
            }
        },
    ]
    output = [autorize.input_operation(line) for line in sys.stdin]

    assert output == expected_output
