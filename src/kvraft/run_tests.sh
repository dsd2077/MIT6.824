#!/bin/bash

for i in {1..100}
do
    echo "Running iteration $i"
    go test -run TestBasic3A
done
