import json

from copy import deepcopy
from sliding_windows import HighFrequencySlidingWindow, RepeatedTransactionSlidingWindow


def input_operation(line):
    return json.loads(line)


def _generate_single_output(account, violations):
    return {"account": deepcopy(account), "violations": violations}


def authorize(data):
    high_frequency = HighFrequencySlidingWindow()
    doubled_transaction = RepeatedTransactionSlidingWindow()
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
                    if doubled_transaction.process_transaction(transaction_data) is False:
                        output.append(_generate_single_output(account, ["doubled-transaction"]))
                    else:
                        if high_frequency.process_high_frequency_small_interval_rule(transaction_data) is True:
                            account["available-limit"] -= transaction_data["amount"]
                            output.append(_generate_single_output(account, []))
                        else:
                            output.append(_generate_single_output(account, ["high-frequency-small-interval"]))
                else:
                    output.append(_generate_single_output(account, ["insufficient-limit"]))

    return output
