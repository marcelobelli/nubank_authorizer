from collections import defaultdict

import pendulum
from pydantic import BaseModel
from copy import deepcopy

from state import AccountState

FREQUENCY_TIME_WINDOW_IN_SECS = 120
FREQUENCY_MAX_TRANSACTIONS_PERMITTED = 3


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
        transaction = deepcopy(transaction)
        self.transactions.append(transaction)


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
        transaction = deepcopy(transaction)
        transaction_key = f"{transaction['merchant']}-{transaction['amount']}"
        self.transactions_counter[transaction_key] += 1
        self.transactions.append(transaction)


def dt_to_float(dt: pendulum.DateTime) -> float:
    return dt.float_timestamp


def str_to_dt(str_dt: str) -> pendulum.DateTime:
    return pendulum.parse(str_dt)


def get_frequency_transaction_state(account_state: AccountState) -> tuple[AccountState, dict]:
    if not account_state.processors_state.get("frequency_transaction"):
        account_state.processors_state["frequency_transaction"] = {"successful_transactions": []}
    return account_state, account_state.processors_state["frequency_transaction"]


def process_frequency_transaction(account_state: AccountState, transaction: dict) -> tuple[AccountState, bool]:
    account_state, state = get_frequency_transaction_state(account_state)
    next_transaction_dt = pendulum.parse(transaction["time"])

    if not state["successful_transactions"]:
        return account_state, True

    for _ in range(len(state["successful_transactions"])):
        time_window_diff = dt_to_float(next_transaction_dt) - dt_to_float(str_to_dt(state["successful_transactions"][0]["time"]))

        if (
            time_window_diff < FREQUENCY_TIME_WINDOW_IN_SECS
            and len(state["successful_transactions"]) == FREQUENCY_MAX_TRANSACTIONS_PERMITTED
        ):
            return account_state, False

        if time_window_diff < FREQUENCY_TIME_WINDOW_IN_SECS:
            return account_state, True

        del state["successful_transactions"][0]

    return account_state, True
