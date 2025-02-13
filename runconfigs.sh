#!/bin/env bash
set -e -u
set -o pipefail

function metric() {
    local name="$1"
    perf stat -j report -i stat.data |& jq -ra "(. | select(.event == \"$name\")  .\"metric-value\")"
}

function configs() {
    ./genconfigs.sh # | sort -R | head -2
}

if [ -f common.csv ]; then
    mv common.csv $(date +'common.csv.bak@%s')
fi

if [ "${1:-}" == "--no-perf" ]; then
    configs | (ulimit -n 32000; ./build/asyncio_benchmark && mv results.csv common.csv)
else
    configs | while read line; do
        {
            ulimit -n 32000;
            perf stat record -o stat.data \
                -e cycles,instructions,context-switches,cache-references,cache-misses \
                -- ./build/asyncio_benchmark <<< "$line"
        }
        if [ ! -f common.csv ]; then
            head -n 1 results.csv | while read line; do
                echo "$line,cycles,instructions,context-switches,cache-references,cache-misses" > common.csv
            done
        fi

        tail -n -1 results.csv | while read line; do
            echo "$line,$(metric cycles),$(metric instructions),$(metric context-switches),$(metric cache-references),$(metric cache-misses)" >> common.csv
        done
        rm stat.data;
    done
fi
