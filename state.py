from collections import defaultdict
from copy import deepcopy

import pendulum
from pydantic import BaseModel


class AccountState(BaseModel):
    processors_state = {}


class RepeatedState(BaseModel):
    transactions: list[dict] = []
    transactions_counter: dict = defaultdict(int)

    @property
    def first_transaction_dt(self) -> pendulum.DateTime:
        return pendulum.parse(self.transactions[0]["time"])

    def remove_first_transaction(self) -> None:
        transaction_key = f"{self.transactions[0]['merchant']}-{self.transactions[0]['amount']}"
        transaction_counter = self.transactions_counter.get(transaction_key)
        del self.transactions[0]
        if transaction_counter <= 1:
            del self.transactions_counter[transaction_key]
            return
        self.transactions_counter[transaction_key] -= 1
        return

    def add_transaction(self, transaction):
        transaction = deepcopy(transaction)
        transaction_key = f"{transaction['merchant']}-{transaction['amount']}"
        self.transactions_counter[transaction_key] += 1
        self.transactions.append(transaction)
