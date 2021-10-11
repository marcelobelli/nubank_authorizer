import io
import sys

import authorize


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
    output = [authorize.input_operation(line) for line in sys.stdin]

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

    assert authorize.authorize(input) == expected_output


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

    assert authorize.authorize(input) == expected_output


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

    assert authorize.authorize(input) == expected_output


def test_card_not_active_rule():
    input = [
        {"account": {"active-card": False, "available-limit": 20}},
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
        {"account": {"active-card": False, "available-limit": 20}, "violations": []},
        {
            "account": {"active-card": False, "available-limit": 20},
            "violations": ["card-not-active"],
        },
        {
            "account": {"active-card": False, "available-limit": 20},
            "violations": ["card-not-active"],
        },
        {
            "account": {"active-card": False, "available-limit": 20},
            "violations": ["card-not-active"],
        },
    ]

    assert authorize.authorize(input) == expected_output


def test_high_frequency_small_interval():
    input = [
        {"account": {"active-card": True, "available-limit": 100}},
        {
            "transaction": {
                "merchant": "Burger King",
                "amount": 20,
                "time": "2019-02-13T11:00:00.000Z",
            }
        },
        {
            "transaction": {
                "merchant": "Habbib's",
                "amount": 20,
                "time": "2019-02-13T11:00:01.000Z",
            }
        },
        {
            "transaction": {
                "merchant": "McDonald's",
                "amount": 20,
                "time": "2019-02-13T11:01:01.000Z",
            }
        },
        {
            "transaction": {
                "merchant": "Subway",
                "amount": 20,
                "time": "2019-02-13T11:01:31.000Z",
            }
        },
        {
            "transaction": {
                "merchant": "Burger King",
                "amount": 10,
                "time": "2019-02-13T12:00:00.000Z",
            }
        },
    ]
    expected_output = [
        {"account": {"active-card": True, "available-limit": 100}, "violations": []},
        {"account": {"active-card": True, "available-limit": 80}, "violations": []},
        {"account": {"active-card": True, "available-limit": 60}, "violations": []},
        {"account": {"active-card": True, "available-limit": 40}, "violations": []},
        {
            "account": {"active-card": True, "available-limit": 40},
            "violations": ["high-frequency-small-interval"],
        },
        {"account": {"active-card": True, "available-limit": 30}, "violations": []},
    ]

    assert authorize.authorize(input) == expected_output


def test_doubled_transaction_rule():
    input = [
        {"account": {"active-card": True, "available-limit": 100}},
        {"transaction": {"merchant": "Burger King", "amount": 20, "time": "2019-02-13T11:00:00.000Z"}},
        {"transaction": {"merchant": "McDonald's", "amount": 10, "time": "2019-02-13T11:00:01.000Z"}},
        {"transaction": {"merchant": "Burger King", "amount": 20, "time": "2019-02-13T11:00:02.000Z"}},
        {"transaction": {"merchant": "Burger King", "amount": 15, "time": "2019-02-13T11:00:03.000Z"}},
    ]
    expected_output = [
        {"account": {"active-card": True, "available-limit": 100}, "violations": []},
        {"account": {"active-card": True, "available-limit": 80}, "violations": []},
        {"account": {"active-card": True, "available-limit": 70}, "violations": []},
        {"account": {"active-card": True, "available-limit": 70}, "violations": ["doubled-transaction"]},
        {"account": {"active-card": True, "available-limit": 55}, "violations": []}
    ]

    assert authorize.authorize(input) == expected_output

