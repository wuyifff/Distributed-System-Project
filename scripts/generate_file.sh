#!/bin/bash


for i in {1..10000}; do
    cat traffic.csv >> traffic_head.csv
done