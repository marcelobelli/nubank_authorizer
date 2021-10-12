from processors import process_frequency_transaction
from state import AccountState


def test_one_successful_transaction_then_add_to_processor():
    account_state = AccountState()
    transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 20,
            "time": "2019-02-13T11:00:00.000Z",
        }
    }

    account_state, response = process_frequency_transaction(account_state, transaction["transaction"])

    assert response is True

    state = account_state.processors_state["frequency_transaction"]
    state.add_transaction(transaction["transaction"])

    assert state.transactions_qty == 1
    assert state.transactions[0] == transaction["transaction"]


def test_two_successful_transactions_inside_time_window():
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
            "merchant": "Habbib's",
            "amount": 20,
            "time": "2019-02-13T11:00:01.000Z",
        }
    }

    account_state, first_response = process_frequency_transaction(account_state, first_transaction["transaction"])
    assert first_response is True
    state = account_state.processors_state["frequency_transaction"]
    state.add_transaction(first_transaction["transaction"])

    account_state, second_response = process_frequency_transaction(account_state, second_transaction["transaction"])
    assert second_response is True
    state.add_transaction(second_transaction["transaction"])

    assert state.transactions_qty == 2
    assert state.transactions[0] == first_transaction["transaction"]
    assert state.transactions[1] == second_transaction["transaction"]


def test_two_successful_transactions_outside_time_window():
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
            "merchant": "Habbib's",
            "amount": 20,
            "time": "2019-02-13T11:02:01.000Z",
        }
    }

    account_state, first_response = process_frequency_transaction(account_state, first_transaction["transaction"])
    assert first_response is True
    state = account_state.processors_state["frequency_transaction"]
    state.add_transaction(first_transaction["transaction"])

    account_state, second_response = process_frequency_transaction(account_state, second_transaction["transaction"])
    assert second_response is True
    state.add_transaction(second_transaction["transaction"])

    assert state.transactions_qty == 1
    assert state.transactions[0] == second_transaction["transaction"]


def test_three_successful_transactions_inside_time_window():
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
            "merchant": "Habbib's",
            "amount": 20,
            "time": "2019-02-13T11:00:01.000Z",
        }
    }
    third_transaction = {
        "transaction": {
            "merchant": "McDonald's",
            "amount": 20,
            "time": "2019-02-13T11:01:01.000Z",
        }
    }

    account_state, first_response = process_frequency_transaction(account_state, first_transaction["transaction"])
    assert first_response is True
    state = account_state.processors_state["frequency_transaction"]
    state.add_transaction(first_transaction["transaction"])

    account_state, second_response = process_frequency_transaction(account_state, second_transaction["transaction"])
    assert second_response is True
    state.add_transaction(second_transaction["transaction"])

    account_state, third_response = process_frequency_transaction(account_state, third_transaction["transaction"])
    assert third_response is True
    state.add_transaction(third_transaction["transaction"])

    assert state.transactions_qty == 3
    assert state.transactions[0] == first_transaction["transaction"]
    assert state.transactions[1] == second_transaction["transaction"]
    assert state.transactions[2] == third_transaction["transaction"]


def test_three_successful_transactions_where_the_first_one_is_outside_time_window():
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
            "merchant": "Habbib's",
            "amount": 20,
            "time": "2019-02-13T11:02:01.000Z",
        }
    }
    third_transaction = {
        "transaction": {
            "merchant": "McDonald's",
            "amount": 20,
            "time": "2019-02-13T11:02:30.000Z",
        }
    }

    account_state, first_response = process_frequency_transaction(account_state, first_transaction["transaction"])
    assert first_response is True
    state = account_state.processors_state["frequency_transaction"]
    state.add_transaction(first_transaction["transaction"])

    account_state, second_response = process_frequency_transaction(account_state, second_transaction["transaction"])
    assert second_response is True
    state.add_transaction(second_transaction["transaction"])

    account_state, third_response = process_frequency_transaction(account_state, third_transaction["transaction"])
    assert third_response is True
    state.add_transaction(third_transaction["transaction"])

    assert state.transactions_qty == 2
    assert state.transactions[0] == second_transaction["transaction"]
    assert state.transactions[1] == third_transaction["transaction"]


def test_three_successful_transactions_where_the_three_are_outside_time_window():
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
            "merchant": "Habbib's",
            "amount": 20,
            "time": "2019-02-13T11:05:00.000Z",
        }
    }
    third_transaction = {
        "transaction": {
            "merchant": "McDonald's",
            "amount": 20,
            "time": "2019-02-13T11:10:00.000Z",
        }
    }

    account_state, first_response = process_frequency_transaction(account_state, first_transaction["transaction"])
    assert first_response is True
    state = account_state.processors_state["frequency_transaction"]
    state.add_transaction(first_transaction["transaction"])

    account_state, second_response = process_frequency_transaction(account_state, second_transaction["transaction"])
    assert second_response is True
    state.add_transaction(second_transaction["transaction"])

    account_state, third_response = process_frequency_transaction(account_state, third_transaction["transaction"])
    assert third_response is True
    state.add_transaction(third_transaction["transaction"])

    assert state.transactions_qty == 1
    assert state.transactions[0] == third_transaction["transaction"]


def test_three_successful_transactions_where_the_third_are_outside_time_window():
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
            "merchant": "Habbib's",
            "amount": 20,
            "time": "2019-02-13T11:00:01.000Z",
        }
    }
    third_transaction = {
        "transaction": {
            "merchant": "McDonald's",
            "amount": 20,
            "time": "2019-02-13T11:10:00.000Z",
        }
    }

    account_state, first_response = process_frequency_transaction(account_state, first_transaction["transaction"])
    assert first_response is True
    state = account_state.processors_state["frequency_transaction"]
    state.add_transaction(first_transaction["transaction"])

    account_state, second_response = process_frequency_transaction(account_state, second_transaction["transaction"])
    assert second_response is True
    state.add_transaction(second_transaction["transaction"])

    account_state, third_response = process_frequency_transaction(account_state, third_transaction["transaction"])
    assert third_response is True
    state.add_transaction(third_transaction["transaction"])

    assert state.transactions_qty == 1
    assert state.transactions[0] == third_transaction["transaction"]


def test_three_successful_and_one_denied_transactions_inside_time_window():
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
            "merchant": "Habbib's",
            "amount": 20,
            "time": "2019-02-13T11:00:01.000Z",
        }
    }
    third_transaction = {
        "transaction": {
            "merchant": "McDonald's",
            "amount": 20,
            "time": "2019-02-13T11:01:01.000Z",
        }
    }
    fourth_transaction = {
        "transaction": {
            "merchant": "Subway",
            "amount": 20,
            "time": "2019-02-13T11:01:31.000Z",
        }
    }

    account_state, first_response = process_frequency_transaction(account_state, first_transaction["transaction"])
    assert first_response is True
    state = account_state.processors_state["frequency_transaction"]
    state.add_transaction(first_transaction["transaction"])

    account_state, second_response = process_frequency_transaction(account_state, second_transaction["transaction"])
    assert second_response is True
    state.add_transaction(second_transaction["transaction"])

    account_state, third_response = process_frequency_transaction(account_state, third_transaction["transaction"])
    assert third_response is True
    state.add_transaction(third_transaction["transaction"])

    account_state, fourth_response = process_frequency_transaction(account_state, fourth_transaction["transaction"])
    assert fourth_response is False

    assert state.transactions_qty == 3
    assert state.transactions[0] == first_transaction["transaction"]
    assert state.transactions[1] == second_transaction["transaction"]
    assert state.transactions[2] == third_transaction["transaction"]


def test_three_successful_and_two_denied_transactions_inside_time_window():
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
            "merchant": "Habbib's",
            "amount": 20,
            "time": "2019-02-13T11:00:01.000Z",
        }
    }
    third_transaction = {
        "transaction": {
            "merchant": "McDonald's",
            "amount": 20,
            "time": "2019-02-13T11:01:01.000Z",
        }
    }
    fourth_transaction = {
        "transaction": {
            "merchant": "Subway",
            "amount": 20,
            "time": "2019-02-13T11:01:31.000Z",
        }
    }
    fifth_transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 30,
            "time": "2019-02-13T11:01:40.000Z",
        }
    }

    account_state, first_response = process_frequency_transaction(account_state, first_transaction["transaction"])
    assert first_response is True
    state = account_state.processors_state["frequency_transaction"]
    state.add_transaction(first_transaction["transaction"])

    account_state, second_response = process_frequency_transaction(account_state, second_transaction["transaction"])
    assert second_response is True
    state.add_transaction(second_transaction["transaction"])

    account_state, third_response = process_frequency_transaction(account_state, third_transaction["transaction"])
    assert third_response is True
    state.add_transaction(third_transaction["transaction"])

    account_state, fourth_response = process_frequency_transaction(account_state, fourth_transaction["transaction"])
    assert fourth_response is False

    account_state, fifth_response = process_frequency_transaction(account_state, fifth_transaction["transaction"])
    assert fifth_response is False

    assert state.transactions_qty == 3
    assert state.transactions[0] == first_transaction["transaction"]
    assert state.transactions[1] == second_transaction["transaction"]
    assert state.transactions[2] == third_transaction["transaction"]


def test_three_successful_and_one_denied_inside_time_window_then_one_successful_after():
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
            "merchant": "Habbib's",
            "amount": 20,
            "time": "2019-02-13T11:00:40.000Z",
        }
    }
    third_transaction = {
        "transaction": {
            "merchant": "McDonald's",
            "amount": 20,
            "time": "2019-02-13T11:01:01.000Z",
        }
    }
    fourth_transaction = {
        "transaction": {
            "merchant": "Subway",
            "amount": 20,
            "time": "2019-02-13T11:01:31.000Z",
        }
    }
    fifth_transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 30,
            "time": "2019-02-13T11:02:30.000Z",
        }
    }

    account_state, first_response = process_frequency_transaction(account_state, first_transaction["transaction"])
    assert first_response is True
    state = account_state.processors_state["frequency_transaction"]
    state.add_transaction(first_transaction["transaction"])

    account_state, second_response = process_frequency_transaction(account_state, second_transaction["transaction"])
    assert second_response is True
    state.add_transaction(second_transaction["transaction"])

    account_state, third_response = process_frequency_transaction(account_state, third_transaction["transaction"])
    assert third_response is True
    state.add_transaction(third_transaction["transaction"])

    account_state, fourth_response = process_frequency_transaction(account_state, fourth_transaction["transaction"])
    assert fourth_response is False

    account_state, fifth_response = process_frequency_transaction(account_state, fifth_transaction["transaction"])
    assert fifth_response is True
    state.add_transaction(fifth_transaction["transaction"])

    assert state.transactions_qty == 3
    assert state.transactions[0] == second_transaction["transaction"]
    assert state.transactions[1] == third_transaction["transaction"]
    assert state.transactions[2] == fifth_transaction["transaction"]


def test_three_successful_and_one_denied_inside_time_window_then_one_successful_and_one_denied_after():
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
            "merchant": "Habbib's",
            "amount": 20,
            "time": "2019-02-13T11:00:40.000Z",
        }
    }
    third_transaction = {
        "transaction": {
            "merchant": "McDonald's",
            "amount": 20,
            "time": "2019-02-13T11:01:01.000Z",
        }
    }
    fourth_transaction = {
        "transaction": {
            "merchant": "Subway",
            "amount": 20,
            "time": "2019-02-13T11:01:31.000Z",
        }
    }
    fifth_transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 30,
            "time": "2019-02-13T11:02:30.000Z",
        }
    }
    sixth_transaction = {
        "transaction": {
            "merchant": "Habbib's",
            "amount": 20,
            "time": "2019-02-13T11:02:35.000Z",
        }
    }

    account_state, first_response = process_frequency_transaction(account_state, first_transaction["transaction"])
    assert first_response is True
    state = account_state.processors_state["frequency_transaction"]
    state.add_transaction(first_transaction["transaction"])

    account_state, second_response = process_frequency_transaction(account_state, second_transaction["transaction"])
    assert second_response is True
    state.add_transaction(second_transaction["transaction"])

    account_state, third_response = process_frequency_transaction(account_state, third_transaction["transaction"])
    assert third_response is True
    state.add_transaction(third_transaction["transaction"])

    account_state, fourth_response = process_frequency_transaction(account_state, fourth_transaction["transaction"])
    assert fourth_response is False

    account_state, fifth_response = process_frequency_transaction(account_state, fifth_transaction["transaction"])
    assert fifth_response is True
    state.add_transaction(fifth_transaction["transaction"])

    account_state, sixth_response = process_frequency_transaction(account_state, sixth_transaction["transaction"])
    assert sixth_response is False

    assert state.transactions_qty == 3
    assert state.transactions[0] == second_transaction["transaction"]
    assert state.transactions[1] == third_transaction["transaction"]
    assert state.transactions[2] == fifth_transaction["transaction"]


def test_three_successful_and_one_denied_inside_time_window_then_three_successful_after():
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
            "merchant": "Habbib's",
            "amount": 20,
            "time": "2019-02-13T11:00:40.000Z",
        }
    }
    third_transaction = {
        "transaction": {
            "merchant": "McDonald's",
            "amount": 20,
            "time": "2019-02-13T11:01:01.000Z",
        }
    }
    fourth_transaction = {
        "transaction": {
            "merchant": "Subway",
            "amount": 20,
            "time": "2019-02-13T11:01:31.000Z",
        }
    }
    fifth_transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 30,
            "time": "2019-02-13T11:03:02.000Z",
        }
    }
    sixth_transaction = {
        "transaction": {
            "merchant": "Habbib's",
            "amount": 20,
            "time": "2019-02-13T11:03:03.000Z",
        }
    }
    seventh_transaction = {
        "transaction": {
            "merchant": "McDonald's",
            "amount": 20,
            "time": "2019-02-13T11:03:04.000Z",
        }
    }

    account_state, first_response = process_frequency_transaction(account_state, first_transaction["transaction"])
    assert first_response is True
    state = account_state.processors_state["frequency_transaction"]
    state.add_transaction(first_transaction["transaction"])

    account_state, second_response = process_frequency_transaction(account_state, second_transaction["transaction"])
    assert second_response is True
    state.add_transaction(second_transaction["transaction"])

    account_state, third_response = process_frequency_transaction(account_state, third_transaction["transaction"])
    assert third_response is True
    state.add_transaction(third_transaction["transaction"])

    account_state, fourth_response = process_frequency_transaction(account_state, fourth_transaction["transaction"])
    assert fourth_response is False

    account_state, fifth_response = process_frequency_transaction(account_state, fifth_transaction["transaction"])
    assert fifth_response is True
    state.add_transaction(fifth_transaction["transaction"])

    account_state, sixth_response = process_frequency_transaction(account_state, sixth_transaction["transaction"])
    assert sixth_response is True
    state.add_transaction(sixth_transaction["transaction"])

    account_state, seventh_response = process_frequency_transaction(account_state, seventh_transaction["transaction"])
    assert seventh_response is True
    state.add_transaction(seventh_transaction["transaction"])

    assert state.transactions_qty == 3
    assert state.transactions[0] == fifth_transaction["transaction"]
    assert state.transactions[1] == sixth_transaction["transaction"]
    assert state.transactions[2] == seventh_transaction["transaction"]
