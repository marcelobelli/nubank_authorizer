import json

from copy import deepcopy
from processors import process_frequency_transaction, process_repeated_transaction
from state import AccountState


def input_operation(line):
    return json.loads(line)


def _generate_single_output(account, violations):
    return {"account": deepcopy(account), "violations": violations}


def high_frequency_rule(account_state: AccountState, transaction: dict, violations: list) -> tuple[AccountState, list]:
    account_state, result = process_frequency_transaction(account_state, transaction)
    if account_state.account_initialized and result is False:
        violations.append("high-frequency-small-interval")

    return account_state, violations


def doubled_transaction_rule(
        account_state: AccountState, transaction: dict, violations: list
) -> tuple[AccountState, list]:
    account_state, result = process_repeated_transaction(account_state, transaction)
    if account_state.account_initialized and result is False:
        violations.append("doubled-transaction")

    return account_state, violations

def card_not_active_rule(account_state: AccountState, transaction: dict, violations: list) -> tuple[AccountState, list]:
    if account_state.account_initialized and account_state.active_card is False:
        violations.append("card-not-active")

    return account_state, violations


def insufficient_limit_rule(account_state: AccountState, transaction: dict, violations: list) -> tuple[AccountState, list]:
    if account_state.account_initialized and transaction["amount"] > account_state.available_limit:
        violations.append("insufficient-limit")

    return account_state, violations


def account_initialized_rule(account_state: AccountState, transaction: dict, violations: list) -> tuple[AccountState, list]:
    if not account_state.account_initialized:
        violations.append("account-not-initialized")

    return account_state, violations


def authorize(data):
    account_state = AccountState()
    output = []
    for line in data:
        match line:
            case {"account": account_data}:
                if not account_state.account_initialized:
                    account_state.initialize_account(account_data)
                    output.append(_generate_single_output(account_state.to_dict(), []))
                else:
                    output.append(_generate_single_output(account_state.to_dict(), ["account-already-initialized"]))
            case {"transaction": transaction_data}:
                violations = []
                account_state, violations = account_initialized_rule(account_state, transaction_data, violations)
                account_state, violations = card_not_active_rule(account_state, transaction_data, violations)
                account_state, violations = insufficient_limit_rule(account_state, transaction_data, violations)
                account_state, violations = high_frequency_rule(account_state, transaction_data, violations)
                account_state, violations = doubled_transaction_rule(account_state, transaction_data, violations)

                if not violations:
                    account_state.available_limit -= transaction_data["amount"]
                    account_state.processors_state["frequency_transaction"].add_transaction(transaction_data)
                    account_state.processors_state["repeated_transaction"].add_transaction(transaction_data)

                output.append(_generate_single_output(account_state.to_dict(), violations))

    return output
