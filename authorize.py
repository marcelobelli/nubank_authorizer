import json
from copy import deepcopy

from rules import transaction_rules
from state import AccountState


def input_operation(line):
    return json.loads(line)


def _generate_single_output(account, violations):
    return {"account": deepcopy(account), "violations": violations}


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
                for rule in transaction_rules:
                    account_state, violations = rule(account_state, transaction_data, violations)

                if not violations:
                    account_state.available_limit -= transaction_data["amount"]
                    account_state.processors_state["frequency_transaction"].add_transaction(transaction_data)
                    account_state.processors_state["repeated_transaction"].add_transaction(transaction_data)

                output.append(_generate_single_output(account_state.to_dict(), violations))

    return output
