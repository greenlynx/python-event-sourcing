#!/usr/bin/env bash
rm -f data.json
./events.py open soak_test

./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &
./events.py deposit soak_test 1 &

wait

./events.py state soak_test
