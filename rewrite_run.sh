#!/bin/bash

trap 'echo "Interrupted"; kill 0; exit 130' INT


echo "Starting graph rewrite"
./auto_run.sh graph graph_rewrite 6

echo "Starting lsqb rewrite"
./auto_run.sh lsqb lsqb_rewrite 6

echo "Starting tpch rewrite"
./auto_run.sh tpch tpch_rewrite 6

echo "Starting job rewrite"
./auto_run.sh job job_agg_rewrite 6