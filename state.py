from collections import defaultdict
from copy import deepcopy

import pendulum
from pydantic import BaseModel

from typing import Optional


class AccountState(BaseModel):
    available_limit: Optional[int] = None
    active_card: Optional[bool] = None
    processors_state: dict = {}
    account_initialized: bool = False

    def initialize_account(self, account):
        self.available_limit = account["available-limit"]
        self.active_card = account["active-card"]
        self.account_initialized = True

    def resolve_transaction(self, transaction):
        self.available_limit -= transaction["amount"]
        for state in self.processors_state.values():
            state.add_transaction(transaction)

    def to_dict(self) -> dict:
        if not self.available_limit:
            return {}
        return {"active-card": self.active_card, "available-limit": self.available_limit}


class RepeatedState(BaseModel):
    transactions: list[dict] = []
    transactions_counter: dict = defaultdict(int)

    @property
    def first_transaction_dt(self) -> pendulum.DateTime:
        return pendulum.parse(self.transactions[0]["time"])

    def remove_first_transaction(self):
        transaction_key = f"{self.transactions[0]['merchant']}-{self.transactions[0]['amount']}"
        transaction_counter = self.transactions_counter.get(transaction_key)
        del self.transactions[0]

        if transaction_counter <= 1:
            del self.transactions_counter[transaction_key]
        else:
            self.transactions_counter[transaction_key] -= 1

    def add_transaction(self, transaction):
        transaction = deepcopy(transaction)
        transaction_key = f"{transaction['merchant']}-{transaction['amount']}"
        self.transactions_counter[transaction_key] += 1
        self.transactions.append(transaction)


class FrequencyState(BaseModel):
    transactions: list[dict] = []

    @property
    def first_transaction_dt(self) -> pendulum.DateTime:
        return pendulum.parse(self.transactions[0]["time"])

    @property
    def transactions_qty(self):
        return len(self.transactions)

    def remove_first_transaction(self):
        del self.transactions[0]

    def add_transaction(self, transaction):
        self.transactions.append(deepcopy(transaction))

