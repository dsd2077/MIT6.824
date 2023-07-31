#!/bin/bash

for i in {3..1000}
do
    echo "Running iteration $i"
    go test -race -run TestChallenge1Concurrent > log${i}
done

