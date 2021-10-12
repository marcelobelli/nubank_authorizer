from collections import defaultdict
from copy import deepcopy

import pendulum


class HighFrequencyTransactionProcessor:
    def __init__(self):
        self.transactions = []
        self.time_window_in_secs = 120
        self.max_transactions_permitted = 3

    def reset(self):
        self.transactions = []

    @property
    def first_transaction_dt(self):
        return pendulum.parse(self.transactions[0]["time"])

    @property
    def successful_transactions(self):
        return len(self.transactions)

    def process_transaction(self, transaction):
        next_transaction_dt = pendulum.parse(transaction["time"])

        if not self.transactions:
            return True

        for _ in range(len(self.transactions)):
            time_window_diff = next_transaction_dt.float_timestamp - self.first_transaction_dt.float_timestamp

            if (
                    time_window_diff < self.time_window_in_secs
                    and self.successful_transactions == self.max_transactions_permitted
            ):
                return False

            if time_window_diff < self.time_window_in_secs:
                return True

            del self.transactions[0]

        return True

    def add_transaction(self, transaction):
        self.transactions.append(deepcopy(transaction))


class RepeatedTransactionProcessor:
    def __init__(self):
        self.transactions: list[dict] = []
        self.transactions_counter: dict = defaultdict(int)
        self.time_window_in_secs: int = 120
        self.max_transactions_permitted: int = 1

    def reset(self):
        self.transactions = []
        self.transactions_counter = defaultdict(int)

    @property
    def first_transaction_dt(self):
        return pendulum.parse(self.transactions[0]["time"])

    def process_transaction(self, transaction):
        transaction_dt = pendulum.parse(transaction["time"])
        transaction_key = f"{transaction['merchant']}-{transaction['amount']}"
        if not self.transactions and self.transactions_counter:
            return True

        for _ in range(len(self.transactions)):
            time_window_diff = transaction_dt.float_timestamp - self.first_transaction_dt.float_timestamp
            transaction_counter = self.transactions_counter.get(transaction_key, 0)

            if time_window_diff < self.time_window_in_secs and transaction_counter == self.max_transactions_permitted:
                return False

            if time_window_diff < self.time_window_in_secs and transaction_counter < self.max_transactions_permitted:
                return True

            transaction_key_to_delete = f"{self.transactions[0]['merchant']}-{self.transactions[0]['amount']}"
            transaction_counter_to_delete = self.transactions_counter.get(transaction_key_to_delete)
            if transaction_counter_to_delete <= 1:
                del self.transactions_counter[transaction_key_to_delete]
            else:
                self.transactions_counter[transaction_key_to_delete] -= 1
            del self.transactions[0]

        return True

    def add_transaction(self, transaction):
        transaction_key = f"{transaction['merchant']}-{transaction['amount']}"
        self.transactions_counter[transaction_key] += 1
        self.transactions.append(deepcopy(transaction))


high_frequency = HighFrequencyTransactionProcessor()
doubled_transaction = RepeatedTransactionProcessor()


def process_account_not_initialized(account, transaction):
    if account:
        return ""
    return "account-not-initialized"


def process_high_frequency_transaction(account, transaction):
    if not account or high_frequency.process_transaction(transaction) is True:
        return ""
    return "high-frequency-small-interval"


def process_doubled_transaction(account, transaction):
    if not account or doubled_transaction.process_transaction(transaction) is True:
        return ""
    return "doubled-transaction"


def process_insufficient_limit(account, transaction):
    if not account or transaction["amount"] <= account["available-limit"]:
        return ""
    return "insufficient-limit"


def process_card_active(account, transaction):
    if not account or account["active-card"] is True:
        return ""
    return "card-not-active"


transaction_processors = [
    process_account_not_initialized,
    process_card_active,
    process_insufficient_limit,
    process_high_frequency_transaction,
    process_doubled_transaction,
]
