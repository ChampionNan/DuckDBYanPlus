#!/bin/bash

trap 'echo "Interrupted"; kill 0; exit 130' INT

# Graph test
# echo "Starting Graph original"
# ./auto_run.sh graph graph 1
# echo "Starting Graph RPT"
# ./auto_run.sh graph graph 2
# echo "Starting Graph Yan+"
# ./auto_run.sh graph graph 4

# LSQB test
# echo "Starting LSQB original"
# ./auto_run.sh lsqb lsqb_test 1
# echo "Starting LSQB RPT"
# ./auto_run.sh lsqb lsqb_test 2
# echo "Starting LSQB Yan+"
# ./auto_run.sh lsqb lsqb_test 3

# JOB test
# echo "Starting JOB agg Part1 original"
# ./auto_run.sh job job_agg_part1 1
# echo "Starting JOB agg Part1 RPT"
# ./auto_run.sh job job_agg_part1 2
# echo "Starting JOB agg Part1 Yan+"
# ./auto_run.sh job job_agg_part1 3

# echo "Starting JOB agg Part2 original"
# ./auto_run.sh job job_agg_part2 1
# echo "Starting JOB agg Part2 RPT"
# ./auto_run.sh job job_agg_part2 2
# echo "Starting JOB agg Part2 Yan+"
# ./auto_run.sh job job_agg_part2 3

# DSB test
# echo "Starting DSB_AGG original"
# ./auto_run.sh dsb dsb_agg 1
# echo "Starting DSB RPT"
# ./auto_run.sh dsb dsb_agg 2
# echo "Starting DSB Yan+"
# ./auto_run.sh dsb dsb_agg 3
# echo "Starting DSB GYO"
# ./auto_run.sh dsb dsb_agg 4

# echo "Starting DSB_SPJ original"
# ./auto_run.sh dsb dsb_spj 1
# echo "Starting DSB RPT"
# ./auto_run.sh dsb dsb_spj 2
# echo "Starting DSB Yan+"
# ./auto_run.sh dsb dsb_spj 3
# echo "Starting DSB GYO"
# ./auto_run.sh dsb dsb_spj 4

# Other
# echo "Starting graph origin"
# ./auto_run.sh graph job_temp 1
# echo "Starting graph RPT"
# ./auto_run.sh graph job_temp 2
# echo "Starting graph YanPlus"
# ./auto_run.sh graph job_temp 3
# echo "Starting graph YanPlus_GYO"
# ./auto_run.sh graph job_temp 4

# bloom_filter test
# echo "Starting BF RPT"
# ./auto_run.sh graph bf 2
# echo "Starting BF Yan+ 4"
# ./auto_run.sh graph bf 5
# echo "Starting BF Yan+ 6"
# ./auto_run.sh graph bf 6
# echo "Starting BF Yan+ 8"
# ./auto_run.sh graph bf 7
# echo "Starting BF Yan+ 10"
# ./auto_run.sh graph bf 8
# echo "Starting BF Yan+ 12"
# ./auto_run.sh graph bf 9
# echo "Starting BF Yan+ 16"
# ./auto_run.sh graph bf 10
# echo "Starting BF Yan+ 20"
# ./auto_run.sh graph bf 11
# echo "Starting BF Yan+ 24"
# ./auto_run.sh graph bf 12

# parallel
# echo "Starting graph origin parallel"
# ./auto_run.sh graph parallel_graph 1 2
# echo "Starting graph RPT"
# ./auto_run.sh graph parallel_graph 2 2
# echo "Starting graph YanPlus"
# ./auto_run.sh graph parallel_graph 3 2
# echo "Starting graph YanPlus_GYO"
# ./auto_run.sh graph parallel_graph 4 2

# echo "Starting job origin parallel"
# ./auto_run.sh job parallel_job 1 72
# echo "Starting job RPT"
# ./auto_run.sh job parallel_job 2 72
# echo "Starting job YanPlus"
# ./auto_run.sh job parallel_job 3 72
# echo "Starting job YanPlus_GYO"
# ./auto_run.sh job parallel_job 4 72

# echo "Starting lsqb origin parallel"
# ./auto_run.sh lsqb parallel_lsqb 1 72
# echo "Starting lsqb RPT"
# ./auto_run.sh lsqb parallel_lsqb 2 72
# echo "Starting lsqb YanPlus"
# ./auto_run.sh lsqb parallel_lsqb 3 72
# echo "Starting lsqb YanPlus_GYO"
# ./auto_run.sh lsqb parallel_lsqb 4 72
