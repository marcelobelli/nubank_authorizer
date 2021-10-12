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
                if not account_state.account_initialized:
                    output.append(_generate_single_output(account_state.to_dict(), ["account-not-initialized"]))
                elif account_state.active_card is False:
                    output.append(_generate_single_output(account_state.to_dict(), ["card-not-active"]))
                elif transaction_data["amount"] <= account_state.available_limit:
                    account_state, repeated_result = process_repeated_transaction(account_state, transaction_data)
                    if repeated_result is False:
                        output.append(_generate_single_output(account_state.to_dict(), ["doubled-transaction"]))
                    else:
                        account_state, frequency_result = process_frequency_transaction(account_state, transaction_data)
                        if frequency_result is True:
                            account_state.available_limit -= transaction_data["amount"]
                            account_state.processors_state["frequency_transaction"].add_transaction(transaction_data)
                            account_state.processors_state["repeated_transaction"].add_transaction(transaction_data)
                            output.append(_generate_single_output(account_state.to_dict(), []))
                        else:
                            output.append(_generate_single_output(account_state.to_dict(), ["high-frequency-small-interval"]))
                else:
                    output.append(_generate_single_output(account_state.to_dict(), ["insufficient-limit"]))

    return output
