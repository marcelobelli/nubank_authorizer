import json
from copy import deepcopy

from processors.account_processors import process_account_already_initialized
from processors.transaction_processors import (
    high_frequency,
    doubled_transaction,
    transaction_processors,
)


def input_operation(line):
    return json.loads(line)


def _generate_single_output(account, violations):
    return {"account": deepcopy(account), "violations": violations}


def authorize(data):
    high_frequency.reset()
    doubled_transaction.reset()
    account = {}
    output = []
    for line in data:
        violations = []
        match line:
            case {"account": account_data}:
                violations = process_account_already_initialized(account, violations)
                if not violations:
                    account = deepcopy(account_data)
                output.append(_generate_single_output(account_data, violations))
            case {"transaction": transaction_data}:
                for processor in transaction_processors:
                    result = processor(account, transaction_data)
                    if result:
                        violations.append(result)

                if not violations:
                    account["available-limit"] -= transaction_data["amount"]
                    high_frequency.add_transaction(transaction_data)
                    doubled_transaction.add_transaction(transaction_data)

                output.append(_generate_single_output(account, violations))

    return output
