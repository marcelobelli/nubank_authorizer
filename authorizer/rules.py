from authorizer.processors import process_frequency_transaction, process_repeated_transaction
from authorizer.state import AccountState


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


def insufficient_limit_rule(
    account_state: AccountState, transaction: dict, violations: list
) -> tuple[AccountState, list]:
    if account_state.account_initialized and transaction["amount"] > account_state.available_limit:
        violations.append("insufficient-limit")
    return account_state, violations


def account_initialized_rule(
    account_state: AccountState, transaction: dict, violations: list
) -> tuple[AccountState, list]:
    if not account_state.account_initialized:
        violations.append("account-not-initialized")
    return account_state, violations


def account_already_initialized_rule(account_state: AccountState, account_data: dict, violations: list) -> tuple[AccountState, list]:
    if account_state.account_initialized:
        violations.append("account-already-initialized")
    return account_state, violations


account_rules = [account_already_initialized_rule]

transaction_rules = [
    account_initialized_rule,
    card_not_active_rule,
    insufficient_limit_rule,
    high_frequency_rule,
    doubled_transaction_rule,
]
