import io
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


def test_account_already_initialized_rule():
    input = [
        {"account": {"active-card": True, "available-limit": 175}},
        {"account": {"active-card": True, "available-limit": 350}},
    ]
    expected_output = [
        {"account": {"active-card": True, "available-limit": 175}, "violations": []},
        {
            "account": {"active-card": True, "available-limit": 350},
            "violations": ["account-already-initialized"],
        },
    ]

    assert autorize.autorize(input) == expected_output


def test_insufficient_limit_rule():
    input = [
        {"account": {"active-card": True, "available-limit": 20}},
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
                "merchant": "Uber",
                "amount": 5,
                "time": "2019-02-13T12:00:00.000Z",
            }
        },
    ]
    expected_output = [
        {"account": {"active-card": True, "available-limit": 20}, "violations": []},
        {"account": {"active-card": True, "available-limit": 10}, "violations": []},
        {
            "account": {"active-card": True, "available-limit": 10},
            "violations": ["insufficient-limit"],
        },
        {"account": {"active-card": True, "available-limit": 5}, "violations": []},
    ]

    assert autorize.autorize(input) == expected_output


def test_account_not_initialized_rule():
    input = [
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
        {"account": {"active-card": True, "available-limit": 100}},
        {
            "transaction": {
                "merchant": "Uber",
                "amount": 5,
                "time": "2019-02-13T12:00:00.000Z",
            }
        },
    ]
    expected_output = [
        {"account": {}, "violations": ["account-not-initialized"]},
        {"account": {}, "violations": ["account-not-initialized"]},
        {"account": {"active-card": True, "available-limit": 100}, "violations": []},
        {"account": {"active-card": True, "available-limit": 95}, "violations": []},
    ]

    assert autorize.autorize(input) == expected_output
