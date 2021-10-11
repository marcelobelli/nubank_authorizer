import pendulum

from sliding_windows import RepeatedTransactionSlidingWindow


def test_one_successful_transaction():
    sliding_window = RepeatedTransactionSlidingWindow()
    transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 20,
            "time": "2019-02-13T11:00:00.000Z",
        }
    }

    response = sliding_window.process_transaction(transaction["transaction"])

    assert response is True
    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert len(sliding_window.transactions) == 1
    assert sliding_window.transactions[0] == transaction["transaction"]
    transaction_key = f"{transaction['transaction']['merchant']}-{transaction['transaction']['amount']}"
    assert sliding_window.transactions_counter.get(transaction_key) == 1


def test_two_equals_transactions_inside_time_window():
    sliding_window = RepeatedTransactionSlidingWindow()
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

    first_response = sliding_window.process_transaction(first_transaction["transaction"])
    second_response = sliding_window.process_transaction(second_transaction["transaction"])

    assert first_response is True
    assert second_response is False

    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert len(sliding_window.transactions) == 1
    assert sliding_window.transactions[0] == first_transaction["transaction"]
    transaction_key = f"{first_transaction['transaction']['merchant']}-{first_transaction['transaction']['amount']}"
    assert sliding_window.transactions_counter.get(transaction_key) == 1


def test_two_different_transactions_inside_time_window():
    sliding_window = RepeatedTransactionSlidingWindow()
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

    first_response = sliding_window.process_transaction(first_transaction["transaction"])
    second_response = sliding_window.process_transaction(second_transaction["transaction"])

    assert first_response is True
    assert second_response is True

    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert len(sliding_window.transactions) == 2
    assert sliding_window.transactions[0] == first_transaction["transaction"]
    assert sliding_window.transactions[1] == second_transaction["transaction"]
    first_transaction_key = (
        f"{first_transaction['transaction']['merchant']}-{first_transaction['transaction']['amount']}"
    )
    assert sliding_window.transactions_counter.get(first_transaction_key) == 1
    second_transaction_key = (
        f"{second_transaction['transaction']['merchant']}-{second_transaction['transaction']['amount']}"
    )
    assert sliding_window.transactions_counter.get(second_transaction_key) == 1


def test_two_equals_transactions_outside_time_window():
    sliding_window = RepeatedTransactionSlidingWindow()
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

    first_response = sliding_window.process_transaction(first_transaction["transaction"])
    second_response = sliding_window.process_transaction(second_transaction["transaction"])

    assert first_response is True
    assert second_response is True

    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:02:01.000Z")
    assert len(sliding_window.transactions) == 1
    assert sliding_window.transactions[0] == second_transaction["transaction"]
    transaction_key = f"{second_transaction['transaction']['merchant']}-{second_transaction['transaction']['amount']}"
    assert sliding_window.transactions_counter.get(transaction_key) == 1


def test_two_equals_and_one_different_inside_time_window_first_case():
    """
    This case is equal -> different -> equal inside time window
    """
    sliding_window = RepeatedTransactionSlidingWindow()
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

    first_response = sliding_window.process_transaction(first_transaction["transaction"])
    second_response = sliding_window.process_transaction(second_transaction["transaction"])
    third_response = sliding_window.process_transaction(third_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is False

    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert len(sliding_window.transactions) == 2
    assert sliding_window.transactions[0] == first_transaction["transaction"]
    assert sliding_window.transactions[1] == second_transaction["transaction"]
    first_transaction_key = (
        f"{first_transaction['transaction']['merchant']}-{first_transaction['transaction']['amount']}"
    )
    assert sliding_window.transactions_counter.get(first_transaction_key) == 1
    second_transaction_key = (
        f"{second_transaction['transaction']['merchant']}-{second_transaction['transaction']['amount']}"
    )
    assert sliding_window.transactions_counter.get(second_transaction_key) == 1


def test_two_equals_and_one_different_inside_time_window_second_case():
    """
    This case is equal -> equal -> different inside time window
    """
    sliding_window = RepeatedTransactionSlidingWindow()
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

    first_response = sliding_window.process_transaction(first_transaction["transaction"])
    second_response = sliding_window.process_transaction(second_transaction["transaction"])
    third_response = sliding_window.process_transaction(third_transaction["transaction"])

    assert first_response is True
    assert second_response is False
    assert third_response is True

    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert len(sliding_window.transactions) == 2
    assert sliding_window.transactions[0] == first_transaction["transaction"]
    assert sliding_window.transactions[1] == third_transaction["transaction"]
    first_transaction_key = (
        f"{first_transaction['transaction']['merchant']}-{first_transaction['transaction']['amount']}"
    )
    assert sliding_window.transactions_counter.get(first_transaction_key) == 1
    third_transaction_key = (
        f"{third_transaction['transaction']['merchant']}-{third_transaction['transaction']['amount']}"
    )
    assert sliding_window.transactions_counter.get(third_transaction_key) == 1
