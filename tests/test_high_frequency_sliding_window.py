import pendulum

from sliding_windows import HighFrequencySlidingWindow


def test_one_successful_transaction():
    sliding_window = HighFrequencySlidingWindow()
    transaction = {"transaction": {"merchant": "Burger King", "amount": 20, "time": "2019-02-13T11:00:00.000Z"}}

    response = sliding_window.process_high_frequency_small_interval_rule(transaction["transaction"])

    assert response is True
    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert sliding_window.successful_transactions == 1
    assert len(sliding_window.transactions) == 1
    assert sliding_window.transactions[0] == transaction["transaction"]


def test_two_successful_transactions_in_less_than_time_window():
    sliding_window = HighFrequencySlidingWindow()
    first_transaction = {"transaction": {"merchant": "Burger King", "amount": 20, "time": "2019-02-13T11:00:00.000Z"}}
    second_transaction = {"transaction": {"merchant": "Habbib's", "amount": 20, "time": "2019-02-13T11:00:01.000Z"}}

    first_response = sliding_window.process_high_frequency_small_interval_rule(first_transaction["transaction"])
    second_response = sliding_window.process_high_frequency_small_interval_rule(second_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert sliding_window.successful_transactions == 2
    assert len(sliding_window.transactions) == 2
    assert sliding_window.transactions[0] == first_transaction["transaction"]
    assert sliding_window.transactions[1] == second_transaction["transaction"]


def test_two_successful_transactions_in_more_than_time_window():
    sliding_window = HighFrequencySlidingWindow()
    first_transaction = {"transaction": {"merchant": "Burger King", "amount": 20, "time": "2019-02-13T11:00:00.000Z"}}
    second_transaction = {"transaction": {"merchant": "Habbib's", "amount": 20, "time": "2019-02-13T11:02:01.000Z"}}

    first_response = sliding_window.process_high_frequency_small_interval_rule(first_transaction["transaction"])
    second_response = sliding_window.process_high_frequency_small_interval_rule(second_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:02:01.000Z")
    assert sliding_window.successful_transactions == 1
    assert len(sliding_window.transactions) == 1
    assert sliding_window.transactions[0] == second_transaction["transaction"]


def test_three_successful_transactions_in_less_than_time_window():
    sliding_window = HighFrequencySlidingWindow()
    first_transaction = {"transaction": {"merchant": "Burger King", "amount": 20, "time": "2019-02-13T11:00:00.000Z"}}
    second_transaction = {"transaction": {"merchant": "Habbib's", "amount": 20, "time": "2019-02-13T11:00:01.000Z"}}
    third_transaction = {"transaction": {"merchant": "McDonald's", "amount": 20, "time": "2019-02-13T11:01:01.000Z"}}

    first_response = sliding_window.process_high_frequency_small_interval_rule(first_transaction["transaction"])
    second_response = sliding_window.process_high_frequency_small_interval_rule(second_transaction["transaction"])
    third_response = sliding_window.process_high_frequency_small_interval_rule(third_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert sliding_window.successful_transactions == 3
    assert len(sliding_window.transactions) == 3
    assert sliding_window.transactions[0] == first_transaction["transaction"]
    assert sliding_window.transactions[1] == second_transaction["transaction"]
    assert sliding_window.transactions[2] == third_transaction["transaction"]


def test_three_successful_transactions_where_the_first_one_is_outside_time_window():
    sliding_window = HighFrequencySlidingWindow()
    first_transaction = {"transaction": {"merchant": "Burger King", "amount": 20, "time": "2019-02-13T11:00:00.000Z"}}
    second_transaction = {"transaction": {"merchant": "Habbib's", "amount": 20, "time": "2019-02-13T11:02:01.000Z"}}
    third_transaction = {"transaction": {"merchant": "McDonald's", "amount": 20, "time": "2019-02-13T11:02:30.000Z"}}

    first_response = sliding_window.process_high_frequency_small_interval_rule(first_transaction["transaction"])
    second_response = sliding_window.process_high_frequency_small_interval_rule(second_transaction["transaction"])
    third_response = sliding_window.process_high_frequency_small_interval_rule(third_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:02:01.000Z")
    assert sliding_window.successful_transactions == 2
    assert len(sliding_window.transactions) == 2
    assert sliding_window.transactions[0] == second_transaction["transaction"]
    assert sliding_window.transactions[1] == third_transaction["transaction"]


def test_three_successful_transactions_where_the_three_are_outside_time_window():
    sliding_window = HighFrequencySlidingWindow()
    first_transaction = {"transaction": {"merchant": "Burger King", "amount": 20, "time": "2019-02-13T11:00:00.000Z"}}
    second_transaction = {"transaction": {"merchant": "Habbib's", "amount": 20, "time": "2019-02-13T11:05:00.000Z"}}
    third_transaction = {"transaction": {"merchant": "McDonald's", "amount": 20, "time": "2019-02-13T11:10:00.000Z"}}

    first_response = sliding_window.process_high_frequency_small_interval_rule(first_transaction["transaction"])
    second_response = sliding_window.process_high_frequency_small_interval_rule(second_transaction["transaction"])
    third_response = sliding_window.process_high_frequency_small_interval_rule(third_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:10:00.000Z")
    assert sliding_window.successful_transactions == 1
    assert len(sliding_window.transactions) == 1
    assert sliding_window.transactions[0] == third_transaction["transaction"]


def test_three_successful_transactions_where_the_third_are_outside_time_window():
    sliding_window = HighFrequencySlidingWindow()
    first_transaction = {"transaction": {"merchant": "Burger King", "amount": 20, "time": "2019-02-13T11:00:00.000Z"}}
    second_transaction = {"transaction": {"merchant": "Habbib's", "amount": 20, "time": "2019-02-13T11:00:01.000Z"}}
    third_transaction = {"transaction": {"merchant": "McDonald's", "amount": 20, "time": "2019-02-13T11:10:00.000Z"}}

    first_response = sliding_window.process_high_frequency_small_interval_rule(first_transaction["transaction"])
    second_response = sliding_window.process_high_frequency_small_interval_rule(second_transaction["transaction"])
    third_response = sliding_window.process_high_frequency_small_interval_rule(third_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:10:00.000Z")
    assert sliding_window.successful_transactions == 1
    assert len(sliding_window.transactions) == 1
    assert sliding_window.transactions[0] == third_transaction["transaction"]


def test_three_successful_and_one_denied_transactions_in_less_than_time_window():
    sliding_window = HighFrequencySlidingWindow()
    first_transaction = {"transaction": {"merchant": "Burger King", "amount": 20, "time": "2019-02-13T11:00:00.000Z"}}
    second_transaction = {"transaction": {"merchant": "Habbib's", "amount": 20, "time": "2019-02-13T11:00:01.000Z"}}
    third_transaction = {"transaction": {"merchant": "McDonald's", "amount": 20, "time": "2019-02-13T11:01:01.000Z"}}
    fourth_transaction = {"transaction": {"merchant": "Subway", "amount": 20, "time": "2019-02-13T11:01:31.000Z"}}

    first_response = sliding_window.process_high_frequency_small_interval_rule(first_transaction["transaction"])
    second_response = sliding_window.process_high_frequency_small_interval_rule(second_transaction["transaction"])
    third_response = sliding_window.process_high_frequency_small_interval_rule(third_transaction["transaction"])
    fourth_response = sliding_window.process_high_frequency_small_interval_rule(fourth_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert fourth_response is False
    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert sliding_window.successful_transactions == 3
    assert len(sliding_window.transactions) == 3
    assert sliding_window.transactions[0] == first_transaction["transaction"]
    assert sliding_window.transactions[1] == second_transaction["transaction"]
    assert sliding_window.transactions[2] == third_transaction["transaction"]


def test_three_successful_and_two_denied_transactions_in_less_than_time_window():
    sliding_window = HighFrequencySlidingWindow()
    first_transaction = {"transaction": {"merchant": "Burger King", "amount": 20, "time": "2019-02-13T11:00:00.000Z"}}
    second_transaction = {"transaction": {"merchant": "Habbib's", "amount": 20, "time": "2019-02-13T11:00:01.000Z"}}
    third_transaction = {"transaction": {"merchant": "McDonald's", "amount": 20, "time": "2019-02-13T11:01:01.000Z"}}
    fourth_transaction = {"transaction": {"merchant": "Subway", "amount": 20, "time": "2019-02-13T11:01:31.000Z"}}
    fifth_transaction = {"transaction": {"merchant": "Burger King", "amount": 30, "time": "2019-02-13T11:01:40.000Z"}}

    first_response = sliding_window.process_high_frequency_small_interval_rule(first_transaction["transaction"])
    second_response = sliding_window.process_high_frequency_small_interval_rule(second_transaction["transaction"])
    third_response = sliding_window.process_high_frequency_small_interval_rule(third_transaction["transaction"])
    fourth_response = sliding_window.process_high_frequency_small_interval_rule(fourth_transaction["transaction"])
    fifth_response = sliding_window.process_high_frequency_small_interval_rule(fifth_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert fourth_response is False
    assert fifth_response is False
    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:00:00.000Z")
    assert sliding_window.successful_transactions == 3
    assert len(sliding_window.transactions) == 3
    assert sliding_window.transactions[0] == first_transaction["transaction"]
    assert sliding_window.transactions[1] == second_transaction["transaction"]
    assert sliding_window.transactions[2] == third_transaction["transaction"]


def test_three_successful_and_one_denied_in_less_than_time_window_then_one_successful_after():
    sliding_window = HighFrequencySlidingWindow()
    first_transaction = {"transaction": {"merchant": "Burger King", "amount": 20, "time": "2019-02-13T11:00:00.000Z"}}
    second_transaction = {"transaction": {"merchant": "Habbib's", "amount": 20, "time": "2019-02-13T11:00:40.000Z"}}
    third_transaction = {"transaction": {"merchant": "McDonald's", "amount": 20, "time": "2019-02-13T11:01:01.000Z"}}
    fourth_transaction = {"transaction": {"merchant": "Subway", "amount": 20, "time": "2019-02-13T11:01:31.000Z"}}
    fifth_transaction = {"transaction": {"merchant": "Burger King", "amount": 30, "time": "2019-02-13T11:02:30.000Z"}}

    first_response = sliding_window.process_high_frequency_small_interval_rule(first_transaction["transaction"])
    second_response = sliding_window.process_high_frequency_small_interval_rule(second_transaction["transaction"])
    third_response = sliding_window.process_high_frequency_small_interval_rule(third_transaction["transaction"])
    fourth_response = sliding_window.process_high_frequency_small_interval_rule(fourth_transaction["transaction"])
    fifth_response = sliding_window.process_high_frequency_small_interval_rule(fifth_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert fourth_response is False
    assert fifth_response is True

    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:00:40.000Z")
    assert sliding_window.successful_transactions == 3
    assert len(sliding_window.transactions) == 3
    assert sliding_window.transactions[0] == second_transaction["transaction"]
    assert sliding_window.transactions[1] == third_transaction["transaction"]
    assert sliding_window.transactions[2] == fifth_transaction["transaction"]


def test_three_successful_and_one_denied_in_less_than_time_window_then_one_successful_and_one_denied_after():
    sliding_window = HighFrequencySlidingWindow()
    first_transaction = {"transaction": {"merchant": "Burger King", "amount": 20, "time": "2019-02-13T11:00:00.000Z"}}
    second_transaction = {"transaction": {"merchant": "Habbib's", "amount": 20, "time": "2019-02-13T11:00:40.000Z"}}
    third_transaction = {"transaction": {"merchant": "McDonald's", "amount": 20, "time": "2019-02-13T11:01:01.000Z"}}
    fourth_transaction = {"transaction": {"merchant": "Subway", "amount": 20, "time": "2019-02-13T11:01:31.000Z"}}
    fifth_transaction = {"transaction": {"merchant": "Burger King", "amount": 30, "time": "2019-02-13T11:02:30.000Z"}}
    sixth_transaction = {"transaction": {"merchant": "Habbib's", "amount": 20, "time": "2019-02-13T11:02:35.000Z"}}

    first_response = sliding_window.process_high_frequency_small_interval_rule(first_transaction["transaction"])
    second_response = sliding_window.process_high_frequency_small_interval_rule(second_transaction["transaction"])
    third_response = sliding_window.process_high_frequency_small_interval_rule(third_transaction["transaction"])
    fourth_response = sliding_window.process_high_frequency_small_interval_rule(fourth_transaction["transaction"])
    fifth_response = sliding_window.process_high_frequency_small_interval_rule(fifth_transaction["transaction"])
    sixth_response = sliding_window.process_high_frequency_small_interval_rule(sixth_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert fourth_response is False
    assert fifth_response is True
    assert sixth_response is False

    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:00:40.000Z")
    assert sliding_window.successful_transactions == 3
    assert len(sliding_window.transactions) == 3
    assert sliding_window.transactions[0] == second_transaction["transaction"]
    assert sliding_window.transactions[1] == third_transaction["transaction"]
    assert sliding_window.transactions[2] == fifth_transaction["transaction"]


def test_three_successful_and_one_denied_in_less_than_time_window_then_three_successful_after():
    sliding_window = HighFrequencySlidingWindow()
    first_transaction = {"transaction": {"merchant": "Burger King", "amount": 20, "time": "2019-02-13T11:00:00.000Z"}}
    second_transaction = {"transaction": {"merchant": "Habbib's", "amount": 20, "time": "2019-02-13T11:00:40.000Z"}}
    third_transaction = {"transaction": {"merchant": "McDonald's", "amount": 20, "time": "2019-02-13T11:01:01.000Z"}}
    fourth_transaction = {"transaction": {"merchant": "Subway", "amount": 20, "time": "2019-02-13T11:01:31.000Z"}}
    fifth_transaction = {"transaction": {"merchant": "Burger King", "amount": 30, "time": "2019-02-13T11:03:02.000Z"}}
    sixth_transaction = {"transaction": {"merchant": "Habbib's", "amount": 20, "time": "2019-02-13T11:03:03.000Z"}}
    seventh_transaction = {"transaction": {"merchant": "McDonald's", "amount": 20, "time": "2019-02-13T11:03:04.000Z"}}

    first_response = sliding_window.process_high_frequency_small_interval_rule(first_transaction["transaction"])
    second_response = sliding_window.process_high_frequency_small_interval_rule(second_transaction["transaction"])
    third_response = sliding_window.process_high_frequency_small_interval_rule(third_transaction["transaction"])
    fourth_response = sliding_window.process_high_frequency_small_interval_rule(fourth_transaction["transaction"])
    fifth_response = sliding_window.process_high_frequency_small_interval_rule(fifth_transaction["transaction"])
    sixth_response = sliding_window.process_high_frequency_small_interval_rule(sixth_transaction["transaction"])
    seventh_response = sliding_window.process_high_frequency_small_interval_rule(seventh_transaction["transaction"])

    assert first_response is True
    assert second_response is True
    assert third_response is True
    assert fourth_response is False
    assert fifth_response is True
    assert sixth_response is True
    assert seventh_response is True

    assert sliding_window.first_transaction_dt == pendulum.parse("2019-02-13T11:03:02.000Z")
    assert sliding_window.successful_transactions == 3
    assert len(sliding_window.transactions) == 3
    assert sliding_window.transactions[0] == fifth_transaction["transaction"]
    assert sliding_window.transactions[1] == sixth_transaction["transaction"]
    assert sliding_window.transactions[2] == seventh_transaction["transaction"]

