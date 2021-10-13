import json
from copy import deepcopy

from rules import transaction_rules
from state import AccountState


def input_operation(line):
    return json.loads(line)


def _generate_single_output(account, violations):
    return {"account": deepcopy(account), "violations": violations}


def authorize_transaction(account_state: AccountState, transaction: dict) -> tuple[AccountState, dict]:
    match transaction:
        case {"account": account_data}:
            if not account_state.account_initialized:
                account_state.initialize_account(account_data)
                return account_state, _generate_single_output(account_state.to_dict(), [])
            else:
                return account_state, _generate_single_output(account_state.to_dict(), ["account-already-initialized"])
        case {"transaction": transaction_data}:
            violations = []
            for rule in transaction_rules:
                account_state, violations = rule(account_state, transaction_data, violations)

            if not violations:
                account_state.resolve_transaction(transaction_data)

            return account_state, _generate_single_output(account_state.to_dict(), violations)
