#!/bin/env bash
for base in server server,affinity; do
    for numclients in 1 10 100 1000; do
        for duration in 1 2 3; do
            for client in asio blasio blocking; do
                echo "$base,$client $numclients $duration"
            done
        done
    done
done
