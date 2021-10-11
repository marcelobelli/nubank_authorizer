import pendulum
import pytest

from processors import RepeatedTransactionProcessor


def test_one_successful_transaction_then_add_to_processor():
    processor = RepeatedTransactionProcessor()
    transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 20,
            "time": "2019-02-13T11:00:00.000Z",
        }
    }

    response = processor.process_transaction(transaction["transaction"])

    assert response is True

    with pytest.raises(IndexError):
        processor.first_transaction_dt

    assert len(processor.transactions) == 0
    assert len(processor.transactions_counter) == 0

    processor.add_transaction(transaction["transaction"])

    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert len(processor.transactions) == 1
    assert processor.transactions[0] == transaction["transaction"]
    transaction_key = f"{transaction['transaction']['merchant']}-{transaction['transaction']['amount']}"
    assert processor.transactions_counter.get(transaction_key) == 1


def test_two_equals_transactions_inside_time_window():
    processor = RepeatedTransactionProcessor()
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

    first_response = processor.process_transaction(first_transaction["transaction"])
    assert first_response is True
    processor.add_transaction(first_transaction["transaction"])

    second_response = processor.process_transaction(second_transaction["transaction"])
    assert second_response is False

    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert len(processor.transactions) == 1
    assert processor.transactions[0] == first_transaction["transaction"]
    transaction_key = f"{first_transaction['transaction']['merchant']}-{first_transaction['transaction']['amount']}"
    assert processor.transactions_counter.get(transaction_key) == 1


def test_two_different_transactions_inside_time_window():
    processor = RepeatedTransactionProcessor()
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

    first_response = processor.process_transaction(first_transaction["transaction"])
    assert first_response is True
    processor.add_transaction(first_transaction["transaction"])

    second_response = processor.process_transaction(second_transaction["transaction"])
    assert second_response is True
    processor.add_transaction(second_transaction["transaction"])

    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert len(processor.transactions) == 2
    assert processor.transactions[0] == first_transaction["transaction"]
    assert processor.transactions[1] == second_transaction["transaction"]
    first_transaction_key = (
        f"{first_transaction['transaction']['merchant']}-{first_transaction['transaction']['amount']}"
    )
    assert processor.transactions_counter.get(first_transaction_key) == 1
    second_transaction_key = (
        f"{second_transaction['transaction']['merchant']}-{second_transaction['transaction']['amount']}"
    )
    assert processor.transactions_counter.get(second_transaction_key) == 1


def test_two_equals_transactions_outside_time_window():
    processor = RepeatedTransactionProcessor()
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

    first_response = processor.process_transaction(first_transaction["transaction"])
    assert first_response is True
    processor.add_transaction(first_transaction["transaction"])

    second_response = processor.process_transaction(second_transaction["transaction"])
    assert second_response is True
    processor.add_transaction(second_transaction["transaction"])

    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:02:01.000Z")
    assert len(processor.transactions) == 1
    assert processor.transactions[0] == second_transaction["transaction"]
    transaction_key = f"{second_transaction['transaction']['merchant']}-{second_transaction['transaction']['amount']}"
    assert processor.transactions_counter.get(transaction_key) == 1


def test_two_equals_and_one_different_inside_time_window_first_case():
    """
    This case is equal -> different -> equal inside time window
    """
    processor = RepeatedTransactionProcessor()
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

    first_response = processor.process_transaction(first_transaction["transaction"])
    assert first_response is True
    processor.add_transaction(first_transaction["transaction"])

    second_response = processor.process_transaction(second_transaction["transaction"])
    assert second_response is True
    processor.add_transaction(second_transaction["transaction"])

    third_response = processor.process_transaction(third_transaction["transaction"])
    assert third_response is False

    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert len(processor.transactions) == 2
    assert processor.transactions[0] == first_transaction["transaction"]
    assert processor.transactions[1] == second_transaction["transaction"]
    first_transaction_key = (
        f"{first_transaction['transaction']['merchant']}-{first_transaction['transaction']['amount']}"
    )
    assert processor.transactions_counter.get(first_transaction_key) == 1
    second_transaction_key = (
        f"{second_transaction['transaction']['merchant']}-{second_transaction['transaction']['amount']}"
    )
    assert processor.transactions_counter.get(second_transaction_key) == 1


def test_two_equals_and_one_different_inside_time_window_second_case():
    """
    This case is equal -> equal -> different inside time window
    """
    processor = RepeatedTransactionProcessor()
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

    first_response = processor.process_transaction(first_transaction["transaction"])
    assert first_response is True
    processor.add_transaction(first_transaction["transaction"])

    second_response = processor.process_transaction(second_transaction["transaction"])
    assert second_response is False

    third_response = processor.process_transaction(third_transaction["transaction"])
    assert third_response is True
    processor.add_transaction(third_transaction["transaction"])

    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert len(processor.transactions) == 2
    assert processor.transactions[0] == first_transaction["transaction"]
    assert processor.transactions[1] == third_transaction["transaction"]
    first_transaction_key = (
        f"{first_transaction['transaction']['merchant']}-{first_transaction['transaction']['amount']}"
    )
    assert processor.transactions_counter.get(first_transaction_key) == 1
    third_transaction_key = (
        f"{third_transaction['transaction']['merchant']}-{third_transaction['transaction']['amount']}"
    )
    assert processor.transactions_counter.get(third_transaction_key) == 1
