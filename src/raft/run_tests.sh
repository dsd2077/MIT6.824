#!/bin/bash

for i in {1..1000}
do
    echo "Running iteration $i"
    go test -run TestFigure8Unreliable2C
done
