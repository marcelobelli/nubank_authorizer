import json

from copy import deepcopy
from processors import process_frequency_transaction, process_repeated_transaction
from state import AccountState

def input_operation(line):
    return json.loads(line)


def _generate_single_output(account, violations):
    return {"account": deepcopy(account), "violations": violations}


def authorize(data):
    account_state = AccountState()
    account = {}
    output = []
    for line in data:
        match line:
            case {"account": account_data}:
                if not account:
                    account = deepcopy(account_data)
                    output.append(_generate_single_output(account_data, []))
                else:
                    output.append(_generate_single_output(account_data, ["account-already-initialized"]))
            case {"transaction": transaction_data}:
                if not account:
                    output.append(_generate_single_output(account, ["account-not-initialized"]))
                elif account["active-card"] == False:
                    output.append(_generate_single_output(account, ["card-not-active"]))
                elif transaction_data["amount"] <= account["available-limit"]:
                    account_state, repeated_result = process_repeated_transaction(account_state, transaction_data)
                    if repeated_result is False:
                        output.append(_generate_single_output(account, ["doubled-transaction"]))
                    else:
                        account_state, frequency_result = process_frequency_transaction(account_state, transaction_data)
                        if frequency_result is True:
                            account["available-limit"] -= transaction_data["amount"]
                            account_state.processors_state["frequency_transaction"].add_transaction(transaction_data)
                            account_state.processors_state["repeated_transaction"].add_transaction(transaction_data)
                            output.append(_generate_single_output(account, []))
                        else:
                            output.append(_generate_single_output(account, ["high-frequency-small-interval"]))
                else:
                    output.append(_generate_single_output(account, ["insufficient-limit"]))

    return output
