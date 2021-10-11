import json

from copy import deepcopy
from processors import high_frequency, doubled_transaction


def input_operation(line):
    return json.loads(line)


def _generate_single_output(account, violations):
    return {"account": deepcopy(account), "violations": violations}


def process_high_frequency_transaction(transaction, violations):
    violations = deepcopy(violations)
    if high_frequency.process_transaction(transaction) is True:
        return violations
    violations.append("high-frequency-small-interval")
    return violations


def process_doubled_transaction(transaction, violations):
    violations = deepcopy(violations)
    if doubled_transaction.process_transaction(transaction) is True:
        return violations
    violations.append("doubled-transaction")
    return violations


def process_insuficient_limit(account, transaction, violations):
    violations = deepcopy(violations)
    if transaction["amount"] <= account["available-limit"]:
        return violations
    violations.append("insufficient-limit")
    return violations


def process_card_active(account, violations):
    violations = deepcopy(violations)
    if account["active-card"] is True:
        return violations
    violations.append("card-not-active")
    return violations


def process_account_not_initialized(account, violations):
    violations = deepcopy(violations)
    if account:
        return violations
    violations.append("account-not-initialized")
    return violations


def process_account_already_initialized(account, violations):
    violations = deepcopy(violations)
    if not account:
        return violations
    violations.append("account-already-initialized")
    return violations


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
                violations = process_account_not_initialized(account, violations)

                if not violations:
                    violations = process_card_active(account, violations)
                    violations = process_insuficient_limit(account, transaction_data, violations)
                    violations = process_high_frequency_transaction(transaction_data, violations)
                    violations = process_doubled_transaction(transaction_data, violations)

                if not violations:
                    account["available-limit"] -= transaction_data["amount"]
                    high_frequency.add_transaction(transaction_data)
                    doubled_transaction.add_transaction(transaction_data)

                output.append(_generate_single_output(account, violations))

    return output
