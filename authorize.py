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
    results = []
    for line in data:
        account, result = authorize_transaction(account, line)
        results.append(result)

    return results


def authorize_transaction(account, data):
    account = deepcopy(account)
    data = deepcopy(data)
    violations = []
    match data:
        case {"account": account_data}:
            violations = process_account_already_initialized(account, violations)
            if not violations:
                account = deepcopy(account_data)

        case {"transaction": transaction_data}:
            for processor in transaction_processors:
                result = processor(account, transaction_data)
                if result:
                    violations.append(result)

            if not violations:
                account["available-limit"] -= transaction_data["amount"]
                high_frequency.add_transaction(transaction_data)
                doubled_transaction.add_transaction(transaction_data)

    return account, _generate_single_output(account, violations)
