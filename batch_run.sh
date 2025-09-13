#!/bin/bash

trap 'echo "Interrupted"; kill 0; exit 130' INT

# echo "Starting LSQB original"
# ./auto_run.sh lsqb lsqb_test 1
# echo "Starting LSQB RPT"
# ./auto_run.sh lsqb lsqb_test 2
# echo "Starting LSQB Yan+"
# ./auto_run.sh lsqb lsqb_test 3

# echo "Starting JOB_PART_1 original"
# ./auto_run.sh job job_part1 1
# echo "Starting JOB_PART_1 RPT"
# ./auto_run.sh job job_part1 2
# echo "Starting JOB_PART_1 Yan+"
# ./auto_run.sh job job_test 3

# echo "Starting JOB_PART_2 original"
# ./auto_run.sh job job_part2 1
# echo "Starting JOB_PART_2 RPT"
# ./auto_run.sh job job_part2 2
# echo "Starting JOB_PART_2 Yan+"
# ./auto_run.sh job job_part2 3

echo "Starting JOB origin"
./auto_run.sh job job_test 1
echo "Starting JOB RPT"
./auto_run.sh job job_test 2
echo "Starting JOB YanPlus"
./auto_run.sh job job_test 3

# echo "Starting TPCH original"
# ./auto_run.sh tpch tpch_temp 3
# echo "Starting TPCH RPT"
# ./auto_run.sh tpch tpch_temp 2
# echo "Starting TPCH Yan+"
# ./auto_run.sh tpch tpch_temp 1
