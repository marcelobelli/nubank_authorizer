import pendulum
from pydantic import BaseModel


class HighFrequencySlidingWindow(BaseModel):
    transactions: list[dict] = []
    time_window_in_secs: int = 120
    max_transactions_in_time_window: int = 3

    @property
    def first_transaction_dt(self):
        return pendulum.parse(self.transactions[0]["time"])

    @property
    def successful_transactions(self):
        return len(self.transactions)

    def process_high_frequency_small_interval_rule(self, next_transaction):
        next_transaction_dt = pendulum.parse(next_transaction["time"])
        if not self.transactions:
            self.transactions.append(next_transaction)
            return True

        for _ in range(len(self.transactions)):
            time_window_in_secs_diff = next_transaction_dt.float_timestamp - self.first_transaction_dt.float_timestamp
            if time_window_in_secs_diff > self.time_window_in_secs:
                if not self.transactions:
                    self.transactions.append(next_transaction)
                    break
                del self.transactions[0]
            else:
                if len(self.transactions) == self.max_transactions_in_time_window:
                    return False
                self.transactions.append(next_transaction)
                break
        else:
            self.transactions.append(next_transaction)

        return True
