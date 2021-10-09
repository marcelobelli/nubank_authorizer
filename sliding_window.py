from copy import deepcopy

import pendulum
from pendulum.datetime import DateTime
from pydantic import BaseModel


class HighFrequencySlidingWindow(BaseModel):
    transactions: list[dict] = []
    time_window_in_secs: int = 120
    successful_transactions: int = 0
    max_transactions_in_time_window: int = 3

    @property
    def first_transaction_dt(self):
        return pendulum.parse(self.transactions[0]["time"])

    def process_high_frequency_small_interval_rule(self, next_transaction):
        next_transaction_dt = pendulum.parse(next_transaction["time"])
        if not self.transactions:
            self.transactions.append(next_transaction)
            self.successful_transactions += 1

            return True

        for _ in range(len(self.transactions)):
            time_window_in_secs_diff = next_transaction_dt.float_timestamp - self.first_transaction_dt.float_timestamp
            if time_window_in_secs_diff > self.time_window_in_secs:
                if not self.transactions:
                    self.transactions.append(next_transaction)
                    self.successful_transactions += 1
                    break
                del self.transactions[0]
                self.successful_transactions -= 1
            else:
                if len(self.transactions) == self.max_transactions_in_time_window:
                    return False
                self.transactions.append(next_transaction)
                self.successful_transactions += 1
                break
        else:
            self.transactions.append(next_transaction)
            self.successful_transactions += 1

        return True

    # Se o tempo do transactions[0] e transactions[-1] for menor que 120, deu ruim

    # Não usar o time_window_in_secs, usar o próprio time do transactions
