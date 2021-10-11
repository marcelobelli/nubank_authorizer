import pendulum

from processors import HighFrequencyTransactionProcessor


def test_one_successful_transaction():
    processor = HighFrequencyTransactionProcessor()
    transaction = {
        "transaction": {
            "merchant": "Burger King",
            "amount": 20,
            "time": "2019-02-13T11:00:00.000Z",
        }
    }

    response = processor.process_transaction(transaction["transaction"])

    assert response is True
    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert processor.successful_transactions == 1
    assert len(processor.transactions) == 1
    assert processor.transactions[0] == transaction["transaction"]


def test_two_successful_transactions_in_less_than_time_window():
    processor = HighFrequencyTransactionProcessor()
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

    first_response = processor.process_transaction(first_transaction["transaction"])
    second_response = processor.process_transaction(second_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert processor.successful_transactions == 2
    assert len(processor.transactions) == 2
    assert processor.transactions[0] == first_transaction["transaction"]
    assert processor.transactions[1] == second_transaction["transaction"]


def test_two_successful_transactions_in_more_than_time_window():
    processor = HighFrequencyTransactionProcessor()
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

    first_response = processor.process_transaction(first_transaction["transaction"])
    second_response = processor.process_transaction(second_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:02:01.000Z")
    assert processor.successful_transactions == 1
    assert len(processor.transactions) == 1
    assert processor.transactions[0] == second_transaction["transaction"]


def test_three_successful_transactions_in_less_than_time_window():
    processor = HighFrequencyTransactionProcessor()
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

    first_response = processor.process_transaction(first_transaction["transaction"])
    second_response = processor.process_transaction(second_transaction["transaction"])
    third_response = processor.process_transaction(third_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert processor.successful_transactions == 3
    assert len(processor.transactions) == 3
    assert processor.transactions[0] == first_transaction["transaction"]
    assert processor.transactions[1] == second_transaction["transaction"]
    assert processor.transactions[2] == third_transaction["transaction"]


def test_three_successful_transactions_where_the_first_one_is_outside_time_window():
    processor = HighFrequencyTransactionProcessor()
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

    first_response = processor.process_transaction(first_transaction["transaction"])
    second_response = processor.process_transaction(second_transaction["transaction"])
    third_response = processor.process_transaction(third_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:02:01.000Z")
    assert processor.successful_transactions == 2
    assert len(processor.transactions) == 2
    assert processor.transactions[0] == second_transaction["transaction"]
    assert processor.transactions[1] == third_transaction["transaction"]


def test_three_successful_transactions_where_the_three_are_outside_time_window():
    processor = HighFrequencyTransactionProcessor()
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

    first_response = processor.process_transaction(first_transaction["transaction"])
    second_response = processor.process_transaction(second_transaction["transaction"])
    third_response = processor.process_transaction(third_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:10:00.000Z")
    assert processor.successful_transactions == 1
    assert len(processor.transactions) == 1
    assert processor.transactions[0] == third_transaction["transaction"]


def test_three_successful_transactions_where_the_third_are_outside_time_window():
    processor = HighFrequencyTransactionProcessor()
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

    first_response = processor.process_transaction(first_transaction["transaction"])
    second_response = processor.process_transaction(second_transaction["transaction"])
    third_response = processor.process_transaction(third_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:10:00.000Z")
    assert processor.successful_transactions == 1
    assert len(processor.transactions) == 1
    assert processor.transactions[0] == third_transaction["transaction"]


def test_three_successful_and_one_denied_transactions_in_less_than_time_window():
    processor = HighFrequencyTransactionProcessor()
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

    first_response = processor.process_transaction(first_transaction["transaction"])
    second_response = processor.process_transaction(second_transaction["transaction"])
    third_response = processor.process_transaction(third_transaction["transaction"])
    fourth_response = processor.process_transaction(fourth_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert fourth_response is False
    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert processor.successful_transactions == 3
    assert len(processor.transactions) == 3
    assert processor.transactions[0] == first_transaction["transaction"]
    assert processor.transactions[1] == second_transaction["transaction"]
    assert processor.transactions[2] == third_transaction["transaction"]


def test_three_successful_and_two_denied_transactions_in_less_than_time_window():
    processor = HighFrequencyTransactionProcessor()
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

    first_response = processor.process_transaction(first_transaction["transaction"])
    second_response = processor.process_transaction(second_transaction["transaction"])
    third_response = processor.process_transaction(third_transaction["transaction"])
    fourth_response = processor.process_transaction(fourth_transaction["transaction"])
    fifth_response = processor.process_transaction(fifth_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert fourth_response is False
    assert fifth_response is False
    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert processor.successful_transactions == 3
    assert len(processor.transactions) == 3
    assert processor.transactions[0] == first_transaction["transaction"]
    assert processor.transactions[1] == second_transaction["transaction"]
    assert processor.transactions[2] == third_transaction["transaction"]


def test_three_successful_and_one_denied_in_less_than_time_window_then_one_successful_after():
    processor = HighFrequencyTransactionProcessor()
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

    first_response = processor.process_transaction(first_transaction["transaction"])
    second_response = processor.process_transaction(second_transaction["transaction"])
    third_response = processor.process_transaction(third_transaction["transaction"])
    fourth_response = processor.process_transaction(fourth_transaction["transaction"])
    fifth_response = processor.process_transaction(fifth_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert fourth_response is False
    assert fifth_response is True

    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:00:40.000Z")
    assert processor.successful_transactions == 3
    assert len(processor.transactions) == 3
    assert processor.transactions[0] == second_transaction["transaction"]
    assert processor.transactions[1] == third_transaction["transaction"]
    assert processor.transactions[2] == fifth_transaction["transaction"]


def test_three_successful_and_one_denied_in_less_than_time_window_then_one_successful_and_one_denied_after():
    processor = HighFrequencyTransactionProcessor()
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

    first_response = processor.process_transaction(first_transaction["transaction"])
    second_response = processor.process_transaction(second_transaction["transaction"])
    third_response = processor.process_transaction(third_transaction["transaction"])
    fourth_response = processor.process_transaction(fourth_transaction["transaction"])
    fifth_response = processor.process_transaction(fifth_transaction["transaction"])
    sixth_response = processor.process_transaction(sixth_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert fourth_response is False
    assert fifth_response is True
    assert sixth_response is False

    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:00:40.000Z")
    assert processor.successful_transactions == 3
    assert len(processor.transactions) == 3
    assert processor.transactions[0] == second_transaction["transaction"]
    assert processor.transactions[1] == third_transaction["transaction"]
    assert processor.transactions[2] == fifth_transaction["transaction"]


def test_three_successful_and_one_denied_in_less_than_time_window_then_three_successful_after():
    processor = HighFrequencyTransactionProcessor()
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

    first_response = processor.process_transaction(first_transaction["transaction"])
    second_response = processor.process_transaction(second_transaction["transaction"])
    third_response = processor.process_transaction(third_transaction["transaction"])
    fourth_response = processor.process_transaction(fourth_transaction["transaction"])
    fifth_response = processor.process_transaction(fifth_transaction["transaction"])
    sixth_response = processor.process_transaction(sixth_transaction["transaction"])
    seventh_response = processor.process_transaction(seventh_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert fourth_response is False
    assert fifth_response is True
    assert sixth_response is True
    assert seventh_response is True

    assert processor.first_transaction_dt == pendulum.parse("2019-02-13T11:03:02.000Z")
    assert processor.successful_transactions == 3
    assert len(processor.transactions) == 3
    assert processor.transactions[0] == fifth_transaction["transaction"]
    assert processor.transactions[1] == sixth_transaction["transaction"]
    assert processor.transactions[2] == seventh_transaction["transaction"]
