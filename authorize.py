import json

from copy import deepcopy


def input_operation(line):
    return json.loads(line)


def _generate_single_output(account, violations):
    return {"account": deepcopy(account), "violations": violations}


def authorize(data):
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
                elif transaction_data["amount"] <= account["available-limit"]:
                    account["available-limit"] -= transaction_data["amount"]
                    output.append(_generate_single_output(account, []))
                else:
                    output.append(_generate_single_output(account, ["insufficient-limit"]))

    return output
