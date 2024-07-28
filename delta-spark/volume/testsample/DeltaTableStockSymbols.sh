#!/bin/bash

spark-shell --packages io.delta:delta-core_2.12:2.1.1 <<EOF
:load ./volume/DeltaTableStockSymbols.scala
EOF
