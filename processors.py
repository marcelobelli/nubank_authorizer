from collections import defaultdict

import pendulum
from pydantic import BaseModel


class HighFrequencyTransactionProcessor(BaseModel):
    transactions: list[dict] = []
    time_window_in_secs: int = 120
    max_transactions_permitted: int = 3

    @property
    def first_transaction_dt(self):
        return pendulum.parse(self.transactions[0]["time"])

    @property
    def successful_transactions(self):
        return len(self.transactions)

    def process_high_frequency_small_interval_rule(self, transaction):
        next_transaction_dt = pendulum.parse(transaction["time"])

        if not self.transactions:
            self.transactions.append(transaction)
            return True

        for _ in range(len(self.transactions)):
            time_window_diff = next_transaction_dt.float_timestamp - self.first_transaction_dt.float_timestamp

            if (
                time_window_diff < self.time_window_in_secs
                and self.successful_transactions == self.max_transactions_permitted
            ):
                return False

            if time_window_diff < self.time_window_in_secs:
                self.transactions.append(transaction)
                return True

            del self.transactions[0]

        self.transactions.append(transaction)
        return True


class RepeatedTransactionProcessor(BaseModel):
    transactions: list[dict] = []
    transactions_counter: dict = defaultdict(int)
    time_window_in_secs: int = 120
    max_transactions_permitted: int = 1

    @property
    def first_transaction_dt(self):
        return pendulum.parse(self.transactions[0]["time"])

    def process_transaction(self, transaction):
        transaction_dt = pendulum.parse(transaction["time"])
        transaction_key = f"{transaction['merchant']}-{transaction['amount']}"
        if not self.transactions and self.transactions_counter:
            self.transactions_counter[transaction_key] += 1
            self.transactions.append(transaction)

            return True

        for _ in range(len(self.transactions)):
            time_window_diff = transaction_dt.float_timestamp - self.first_transaction_dt.float_timestamp
            transaction_counter = self.transactions_counter.get(transaction_key, 0)

            if time_window_diff < self.time_window_in_secs and transaction_counter == self.max_transactions_permitted:
                return False

            if time_window_diff < self.time_window_in_secs and transaction_counter < self.max_transactions_permitted:
                self.transactions_counter[transaction_key] += 1
                self.transactions.append(transaction)

                return True

            transaction_key_to_delete = f"{self.transactions[0]['merchant']}-{self.transactions[0]['amount']}"
            transaction_counter_to_delete = self.transactions_counter.get(transaction_key_to_delete)
            if transaction_counter_to_delete <= 1:
                del self.transactions_counter[transaction_key_to_delete]
            else:
                self.transactions_counter[transaction_key_to_delete] -= 1
            del self.transactions[0]

        self.transactions_counter[transaction_key] += 1
        self.transactions.append(transaction)

        return True
