#!/bin/env bash
for serverThreads in 3 4; do
    for clientThreads in 3 4; do
        for opts in '' affinity; do
            for numclients in 32 320; do
                for duration in 2; do
                    for client in asio blasio blocking; do
                        echo "server,$opts,$client $numclients $duration $serverThreads $clientThreads"
                    done
                done
            done
        done
    done
done
