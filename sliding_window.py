import pendulum
from pydantic import BaseModel


class HighFrequencySlidingWindow(BaseModel):
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

            if time_window_diff < self.time_window_in_secs and self.successful_transactions == self.max_transactions_permitted:
                return False

            if time_window_diff < self.time_window_in_secs:
                self.transactions.append(transaction)
                return True

            del self.transactions[0]

        self.transactions.append(transaction)
        return True
