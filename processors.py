import pendulum

from state import AccountState, RepeatedState, FrequencyState

FREQUENCY_TIME_WINDOW_IN_SECS = 120
FREQUENCY_MAX_TRANSACTIONS_PERMITTED = 3

REPEATED_TRANSACTIONS_TIME_WINDOW_IN_SECS = 120
REPEATED_TRANSACTIONS_MAX_PERMITTED = 1


def get_frequency_transaction_state(account_state: AccountState) -> tuple[AccountState, FrequencyState]:
    if not account_state.processors_state.get("frequency_transaction"):
        account_state.processors_state["frequency_transaction"] = FrequencyState()
    return account_state, account_state.processors_state["frequency_transaction"]


def process_frequency_transaction(account_state: AccountState, transaction: dict) -> tuple[AccountState, bool]:
    account_state, state = get_frequency_transaction_state(account_state)
    next_transaction_dt = pendulum.parse(transaction["time"])

    if not state.transactions:
        return account_state, True

    for _ in range(state.transactions_qty):
        time_window_diff = next_transaction_dt.float_timestamp - state.first_transaction_dt.float_timestamp

        if (
                time_window_diff < FREQUENCY_TIME_WINDOW_IN_SECS
                and state.transactions_qty == FREQUENCY_MAX_TRANSACTIONS_PERMITTED
        ):
            return account_state, False

        if time_window_diff < FREQUENCY_TIME_WINDOW_IN_SECS:
            return account_state, True

        state.remove_first_transaction()

    return account_state, True


def get_repeated_transaction_state(account_state: AccountState) -> tuple[AccountState, RepeatedState]:
    if not account_state.processors_state.get("repeated_transaction"):
        account_state.processors_state["repeated_transaction"] = RepeatedState()
    return account_state, account_state.processors_state["repeated_transaction"]


def process_repeated_transaction(account_state: AccountState, transaction: dict) -> tuple[AccountState, bool]:
    account_state, state = get_repeated_transaction_state(account_state)
    transaction_dt = pendulum.parse(transaction["time"])
    transaction_key = f"{transaction['merchant']}-{transaction['amount']}"

    if not state.transactions:
        return account_state, True

    for _ in range(len(state.transactions)):
        time_window_diff = transaction_dt.float_timestamp - state.first_transaction_dt.float_timestamp
        transaction_counter = state.transactions_counter.get(transaction_key, 0)

        if time_window_diff < REPEATED_TRANSACTIONS_TIME_WINDOW_IN_SECS and transaction_counter == REPEATED_TRANSACTIONS_MAX_PERMITTED:
            return account_state, False

        if time_window_diff < REPEATED_TRANSACTIONS_TIME_WINDOW_IN_SECS and transaction_counter < REPEATED_TRANSACTIONS_MAX_PERMITTED:
            return account_state, True

        state.remove_first_transaction()

    return account_state, True
