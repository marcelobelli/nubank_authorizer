import json
from copy import deepcopy

from .rules import account_rules, transaction_rules
from .state import AccountState


def input_operation(transaction):
    return json.loads(transaction)


def output_operation(transaction):
    return json.dumps(transaction)


def _generate_single_output(account, violations):
    return {"account": deepcopy(account), "violations": violations}


def authorize_transaction(account_state: AccountState, transaction: dict) -> tuple[AccountState, dict]:
    violations = []
    match transaction:
        case {"account": account_data}:
            for rule in account_rules:
                account_state, violations = rule(account_state, account_data, violations)

            if not violations:
                account_state.initialize_account(account_data)

            return account_state, _generate_single_output(account_state.to_dict(), violations)

        case {"transaction": transaction_data}:
            for rule in transaction_rules:
                account_state, violations = rule(account_state, transaction_data, violations)

            if not violations:
                account_state.resolve_transaction(transaction_data)

            return account_state, _generate_single_output(account_state.to_dict(), violations)
