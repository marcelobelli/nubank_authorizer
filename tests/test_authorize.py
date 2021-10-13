import io
import sys

from authorizer import authorizer

from authorizer.state import AccountState


def test_input_data_operations(monkeypatch):
    input_data = """{"account": {"active-card": true, "available-limit": 100}}
{"transaction": {"merchant": "Burger King", "amount": 10, "time": "2019-02-13T10:00:00.000Z"}}
{"transaction": {"merchant": "Habbib's", "amount": 20, "time": "2019-02-13T11:00:00.000Z"}}
{"transaction": {"merchant": "McDonald's", "amount": 30, "time": "2019-02-13T12:00:00.000Z"}}"""
    monkeypatch.setattr("sys.stdin", io.StringIO(input_data))
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
    output = [authorizer.input_operation(line) for line in sys.stdin]

    assert output == expected_output


def test_account_already_initialized_rule():
    input_data = [
        {"account": {"active-card": True, "available-limit": 175}},
        {"account": {"active-card": True, "available-limit": 350}},
    ]
    expected_output = [
        {"account": {"active-card": True, "available-limit": 175}, "violations": []},
        {
            "account": {"active-card": True, "available-limit": 175},
            "violations": ["account-already-initialized"],
        },
    ]
    account_state = AccountState()
    output = []
    for transaction in input_data:
        account_state, result = authorizer.authorize_transaction(account_state, transaction)
        output.append(result)

    assert output == expected_output


def test_insufficient_limit_rule():
    input_data = [
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

    account_state = AccountState()
    output = []
    for transaction in input_data:
        account_state, result = authorizer.authorize_transaction(account_state, transaction)
        output.append(result)

    assert output == expected_output


def test_account_not_initialized_rule():
    input_data = [
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

    account_state = AccountState()
    output = []
    for transaction in input_data:
        account_state, result = authorizer.authorize_transaction(account_state, transaction)
        output.append(result)

    assert output == expected_output


def test_card_not_active_rule():
    input_data = [
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

    account_state = AccountState()
    output = []
    for transaction in input_data:
        account_state, result = authorizer.authorize_transaction(account_state, transaction)
        output.append(result)

    assert output == expected_output


def test_high_frequency_small_interval():
    input_data = [
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

    account_state = AccountState()
    output = []
    for transaction in input_data:
        account_state, result = authorizer.authorize_transaction(account_state, transaction)
        output.append(result)

    assert output == expected_output


def test_doubled_transaction_rule():
    input_data = [
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

    account_state = AccountState()
    output = []
    for transaction in input_data:
        account_state, result = authorizer.authorize_transaction(account_state, transaction)
        output.append(result)

    assert output == expected_output


def test_multiple_violations():
    input_data = [
        {"account": {"active-card": True, "available-limit": 100}},
        {"transaction": {"merchant": "McDonald's", "amount": 10, "time": "2019-02-13T11:00:01.000Z"}},
        {"transaction": {"merchant": "Burger King", "amount": 20, "time": "2019-02-13T11:00:02.000Z"}},
        {"transaction": {"merchant": "Burger King", "amount": 5, "time": "2019-02-13T11:00:07.000Z"}},
        {"transaction": {"merchant": "Burger King", "amount": 5, "time": "2019-02-13T11:00:08.000Z"}},
        {"transaction": {"merchant": "Burger King", "amount": 150, "time": "2019-02-13T11:00:18.000Z"}},
        {"transaction": {"merchant": "Burger King", "amount": 190, "time": "2019-02-13T11:00:22.000Z"}},
        {"transaction": {"merchant": "Burger King", "amount": 15, "time": "2019-02-13T12:00:27.000Z"}},
    ]
    expected_output = [
        {"account": {"active-card": True, "available-limit": 100}, "violations": []},
        {"account": {"active-card": True, "available-limit": 90}, "violations": []},
        {"account": {"active-card": True, "available-limit": 70}, "violations": []},
        {"account": {"active-card": True, "available-limit": 65}, "violations": []},
        {
            "account": {"active-card": True, "available-limit": 65},
            "violations": ["high-frequency-small-interval", "doubled-transaction"],
        },
        {
            "account": {"active-card": True, "available-limit": 65},
            "violations": ["insufficient-limit", "high-frequency-small-interval"],
        },
        {
            "account": {"active-card": True, "available-limit": 65},
            "violations": ["insufficient-limit", "high-frequency-small-interval"],
        },
        {"account": {"active-card": True, "available-limit": 50}, "violations": []},
    ]

    account_state = AccountState()
    output = []
    for transaction in input_data:
        account_state, result = authorizer.authorize_transaction(account_state, transaction)
        output.append(result)

    assert output == expected_output
