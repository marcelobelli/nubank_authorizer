from authorizer.processors import process_repeated_transaction
from authorizer.state import AccountState


def test_one_successful_transaction_then_add_to_processor():
    account_state = AccountState()
    transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 20,
            "time": "2019-02-13T11:00:00.000Z",
        }
    }

    account_state, response = process_repeated_transaction(account_state, transaction["transaction"])

    assert response is True

    state = account_state.processors_state["repeated_transaction"]
    state.add_transaction(transaction["transaction"])

    assert len(state.transactions) == 1
    assert state.transactions[0] == transaction["transaction"]
    transaction_key = f"{transaction['transaction']['merchant']}-{transaction['transaction']['amount']}"
    assert state.transactions_counter.get(transaction_key) == 1


def test_two_equals_transactions_inside_time_window():
    account_state = AccountState()
    first_transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 20,
            "time": "2019-02-13T11:00:00.000Z",
        }
    }
    second_transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 20,
            "time": "2019-02-13T11:00:30.000Z",
        }
    }

    account_state, first_response = process_repeated_transaction(account_state, first_transaction["transaction"])
    assert first_response is True
    state = account_state.processors_state["repeated_transaction"]
    state.add_transaction(first_transaction["transaction"])

    account_state, second_response = process_repeated_transaction(account_state, second_transaction["transaction"])
    assert second_response is False

    assert len(state.transactions) == 1
    assert state.transactions[0] == first_transaction["transaction"]
    transaction_key = f"{first_transaction['transaction']['merchant']}-{first_transaction['transaction']['amount']}"
    assert state.transactions_counter.get(transaction_key) == 1


def test_two_different_transactions_inside_time_window():
    account_state = AccountState()
    first_transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 20,
            "time": "2019-02-13T11:00:00.000Z",
        }
    }
    second_transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 30,
            "time": "2019-02-13T11:00:01.000Z",
        }
    }

    account_state, first_response = process_repeated_transaction(account_state, first_transaction["transaction"])
    assert first_response is True
    state = account_state.processors_state["repeated_transaction"]
    state.add_transaction(first_transaction["transaction"])

    account_state, second_response = process_repeated_transaction(account_state, second_transaction["transaction"])
    assert second_response is True
    state.add_transaction(second_transaction["transaction"])

    assert len(state.transactions) == 2
    assert state.transactions[0] == first_transaction["transaction"]
    assert state.transactions[1] == second_transaction["transaction"]
    first_transaction_key = (
        f"{first_transaction['transaction']['merchant']}-{first_transaction['transaction']['amount']}"
    )
    assert state.transactions_counter.get(first_transaction_key) == 1
    second_transaction_key = (
        f"{second_transaction['transaction']['merchant']}-{second_transaction['transaction']['amount']}"
    )
    assert state.transactions_counter.get(second_transaction_key) == 1


def test_two_equals_transactions_outside_time_window():
    account_state = AccountState()
    first_transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 20,
            "time": "2019-02-13T11:00:00.000Z",
        }
    }
    second_transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 20,
            "time": "2019-02-13T11:02:01.000Z",
        }
    }

    account_state, first_response = process_repeated_transaction(account_state, first_transaction["transaction"])
    assert first_response is True
    state = account_state.processors_state["repeated_transaction"]
    state.add_transaction(first_transaction["transaction"])

    account_state, second_response = process_repeated_transaction(account_state, second_transaction["transaction"])
    assert second_response is True
    state.add_transaction(second_transaction["transaction"])

    assert len(state.transactions) == 1
    assert state.transactions[0] == second_transaction["transaction"]
    transaction_key = f"{second_transaction['transaction']['merchant']}-{second_transaction['transaction']['amount']}"
    assert state.transactions_counter.get(transaction_key) == 1


def test_two_equals_and_one_different_inside_time_window_first_case():
    """
    This case is equal -> different -> equal inside time window
    """
    account_state = AccountState()
    first_transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 20,
            "time": "2019-02-13T11:00:00.000Z",
        }
    }
    second_transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 30,
            "time": "2019-02-13T11:00:01.000Z",
        }
    }
    third_transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 20,
            "time": "2019-02-13T11:00:30.000Z",
        }
    }

    account_state, first_response = process_repeated_transaction(account_state, first_transaction["transaction"])
    assert first_response is True
    state = account_state.processors_state["repeated_transaction"]
    state.add_transaction(first_transaction["transaction"])

    account_state, second_response = process_repeated_transaction(account_state, second_transaction["transaction"])
    assert second_response is True
    state.add_transaction(second_transaction["transaction"])

    account_state, third_response = process_repeated_transaction(account_state, third_transaction["transaction"])
    assert third_response is False

    assert len(state.transactions) == 2
    assert state.transactions[0] == first_transaction["transaction"]
    assert state.transactions[1] == second_transaction["transaction"]
    first_transaction_key = (
        f"{first_transaction['transaction']['merchant']}-{first_transaction['transaction']['amount']}"
    )
    assert state.transactions_counter.get(first_transaction_key) == 1
    second_transaction_key = (
        f"{second_transaction['transaction']['merchant']}-{second_transaction['transaction']['amount']}"
    )
    assert state.transactions_counter.get(second_transaction_key) == 1


def test_two_equals_and_one_different_inside_time_window_second_case():
    """
    This case is equal -> equal -> different inside time window
    """
    account_state = AccountState()
    first_transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 20,
            "time": "2019-02-13T11:00:00.000Z",
        }
    }
    second_transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 20,
            "time": "2019-02-13T11:00:01.000Z",
        }
    }
    third_transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 30,
            "time": "2019-02-13T11:00:30.000Z",
        }
    }

    account_state, first_response = process_repeated_transaction(account_state, first_transaction["transaction"])
    assert first_response is True
    state = account_state.processors_state["repeated_transaction"]
    state.add_transaction(first_transaction["transaction"])

    account_state, second_response = process_repeated_transaction(account_state, second_transaction["transaction"])
    assert second_response is False

    account_state, third_response = process_repeated_transaction(account_state, third_transaction["transaction"])
    assert third_response is True
    state.add_transaction(third_transaction["transaction"])

    assert len(state.transactions) == 2
    assert state.transactions[0] == first_transaction["transaction"]
    assert state.transactions[1] == third_transaction["transaction"]
    first_transaction_key = (
        f"{first_transaction['transaction']['merchant']}-{first_transaction['transaction']['amount']}"
    )
    assert state.transactions_counter.get(first_transaction_key) == 1
    third_transaction_key = (
        f"{third_transaction['transaction']['merchant']}-{third_transaction['transaction']['amount']}"
    )
    assert state.transactions_counter.get(third_transaction_key) == 1
