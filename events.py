#!/usr/bin/env python3

import argparse
import functools
import itertools
import json
import time

DATA_FILENAME = "data.json"


def parse_options():
    parser = argparse.ArgumentParser(
        prog='EventSourcingPoC',
        description='Event sourcing PoC')

    subparsers = parser.add_subparsers(help='command to run')

    parser_open = subparsers.add_parser(
        'open', help='adds a new open_account event')
    parser_open.add_argument('account_id')
    parser_open.set_defaults(func=do_open)

    parser_deposit = subparsers.add_parser(
        'deposit', help='adds a new deposit event')
    parser_deposit.add_argument('account_id')
    parser_deposit.add_argument('amount', type=int)
    parser_deposit.set_defaults(func=do_deposit)

    parser_state = subparsers.add_parser(
        'state', help='shows the current state of an account')
    parser_state.add_argument('account_id')
    parser_state.set_defaults(func=do_state)

    parser_history = subparsers.add_parser(
        'history', help='shows the history of an account')
    parser_history.add_argument('account_id')
    parser_history.set_defaults(func=do_history)

    parser_history = subparsers.add_parser(
        'fix', help='fixes previous state calculation bugs, if present')
    parser_history.add_argument('account_id')
    parser_history.set_defaults(func=do_fix)

    parser_list = subparsers.add_parser(
        'list', help='lists accounts and their balances')
    parser_list.set_defaults(func=do_list)

    parser_list = subparsers.add_parser(
        'soak', help='runs multithreaded soak test')
    parser_list.set_defaults(func=do_soak)

    args = parser.parse_args()
    args.func(args)


def build_event(event_type, data):
    return {
        "type": event_type, "data": data
    }


def type_from_event(event):
    return event["type"]


def data_from_event(event):
    return event["data"]


def build_record(aggregate_id, event, state):
    return {
        "pk": aggregate_id,
        "event": event,
        "state": state
    }


def aggregate_id_from_record(record):
    return record["pk"]


def event_from_record(record):
    return record["event"]


def state_from_record(record):
    return record["state"]


def calculate_state(events):
    return functools.reduce(apply_event, events, get_initial_state())


def add_event(args, event_type, data=None, skip_noop=False):
    aggregate_id = args.account_id
    data = data or {}
    existing_records = read_records(aggregate_id)

    existing_events = map(event_from_record, existing_records)

    new_event = build_event(event_type, data)

    new_state = calculate_state(itertools.chain(existing_events, [new_event]))

    if skip_noop and len(existing_records) > 0:
        if len(existing_records) > 0:
            current_state = state_from_record(
                existing_records[len(existing_records)-1])

        if current_state == new_state:
            return

    new_record = build_record(
        aggregate_id, new_event, new_state)

    write_record(new_record)


def do_open(args):
    add_event(args, "open_account")


def do_deposit(args):
    add_event(args, "deposit", data={"amount": args.amount})


def do_fix(args):
    add_event(args, "fix", skip_noop=True)


def do_list(args):
    print(json.dumps(read_records(), indent=2))


def do_state(args):
    aggregate_id = args.account_id
    records = read_records(aggregate_id)
    events = map(event_from_record, records)
    current_state = calculate_state(events)
    print(current_state)


def do_history(args):
    aggregate_id = args.account_id
    print(json.dumps(read_records(aggregate_id), indent=2))


def do_soak(args):
    aggregate_id = "soak_test" + time.time()


def write_record(new_record):
    existing_records = read_records()
    all_records = itertools.chain(existing_records, [new_record])
    with open(DATA_FILENAME, 'wt', encoding='utf8') as file:
        file.write(json.dumps({"events": list(all_records)}, indent=2))


def read_records(aggregate_id=None):
    try:
        with open(DATA_FILENAME, 'rt', encoding='utf8') as file:
            records = json.load(file)["events"]

        if aggregate_id:
            records = list(filter(
                lambda x: aggregate_id_from_record(x) == aggregate_id, records))

        return records
    except FileNotFoundError:
        return []


def get_initial_state():
    return None


def apply_event(current_state, event):
    event_type = type_from_event(event)
    data = data_from_event(event)

    if event_type == "open_account":
        current_state = {"balance": 0}
    elif event_type == "deposit":
        current_state = {
            "balance": current_state["balance"] + data["amount"]}
    elif event_type != "fix":
        raise Exception("Unknown event")

    return current_state


parse_options()
