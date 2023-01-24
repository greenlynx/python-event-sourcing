#!/usr/bin/env python3

import argparse
import functools
import itertools
import json
from os.path import exists

DATA_FILENAME = "data.json"


def parse_options():
    parser = argparse.ArgumentParser(
        prog='EventSourcingPoC',
        description='Event sourcing PoC')

    subparsers = parser.add_subparsers(help='command to run')

    parser_a = subparsers.add_parser(
        'add', help='adds a new event to the event store')
    parser_a.add_argument('event_type', choices=[
                          'open_account', 'close_account'])
    parser_a.add_argument('account_id')
    parser_a.set_defaults(func=do_add)

    parser_b = subparsers.add_parser(
        'list', help='lists accounts and their balances')
    parser_b.set_defaults(func=do_list)

    parser_c = subparsers.add_parser(
        'state', help='shows the current application state')
    parser_c.set_defaults(func=do_state)

    args = parser.parse_args()
    args.func(args)


def build_event(event_type, data):
    return {
        "type": event_type, "data": data
    }


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


def do_add(args):
    records = read_records()
    existing_events = map(event_from_record, records)

    aggregate_id = args.account_id
    new_event = build_event(args.event_type, {})
    new_state = calculate_state(itertools.chain(existing_events, [new_event]))
    new_record = build_record(
        aggregate_id, new_event, new_state)

    records.append(new_record)
    write_records(records)


def do_list(args):
    print(json.dumps(read_records(), indent=2))


def do_state(args):
    records = read_records()
    events = map(event_from_record, records)
    current_state = calculate_state(events)
    print(current_state)


def write_records(records):
    with open(DATA_FILENAME, 'wt', encoding='utf8') as file:
        file.write(json.dumps({"events": records}, indent=2))


def read_records():
    if not exists(DATA_FILENAME):
        write_records([])

    with open(DATA_FILENAME, 'rt', encoding='utf8') as file:
        return json.load(file)["events"]


def get_initial_state():
    return {"number_of_accounts": 0}


def apply_event(current_state, event):
    if (event["type"] == "open_account"):
        current_state["number_of_accounts"] += 1
    else:
        raise Exception("Unknown event")

    return current_state


parse_options()
