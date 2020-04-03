#!/bin/bash

# Fill in these four variables and call the script without any arguments.
# It will download the logs using external logs table for single query.

QUERY_ID="hive_20200403001645_186da65e-cab2-4445-be15-aa36ef4aae94"
COMPUTE_NS="compute-1585858696-m98v"
WH_NS="warehouse-1580339123-xdmn"
CONN_ADDR="jdbc:hive2://hs2-mustafa-tpcds.env-6cwwgq.dwx.workload-dev.cloudera.com/sys;transportMode=http;httpPath=cliservice;ssl=true;"

# Get the hiveserver2 logs 
echo "select ts, msg, structured_data, unmatched, ns, app_name, app, hostname from logs where ns='$COMPUTE_NS' and app='hiveserver2' and structured_data['queryId']='$QUERY_ID';" > /tmp/hive-query.sql.tmp
beeline -n csso_mustafa -p Hive1234! -u "$CONN_ADDR" -f /tmp/hive-query.sql.tmp > hiveserver2.log

# Get start and end of the query

ST=`cat hiveserver2.log | grep "opType=EXECUTE_STATEMENT, queryId=${QUERY_ID}" | awk -F'|' '{ print $2 }' | awk '{$1=$1;print}'`

EN=`cat hiveserver2.log | grep "Completed executing command(queryId=${QUERY_ID}" | awk -F'|' '{ print $2 }' | awk '{$1=$1;print}'`

TS_START=$ST
TS_END=$EN
DT=`date -j -v -1S -f '%Y-%m-%d %H:%M:%S' "$TS_START" +'%Y-%m-%d'`

# Get the other logs

echo "select ts, msg, structured_data, unmatched, ns, app_name, app, hostname from logs where ns='$COMPUTE_NS' and app='query-coordinator-0' and dt >= '$DT' and ts >= '$TS_START' and ts <= '$TS_END' order by ts;" > /tmp/hive-query.sql.tmp
beeline -n csso_mustafa -p Hive1234! -u "$CONN_ADDR" -f /tmp/hive-query.sql.tmp > query-coordinator-0.log

echo "select ts, msg, structured_data, unmatched, ns, app_name, app, hostname from logs where ns='$COMPUTE_NS' and app='query-executor-0' and dt >= '$DT' and ts >= '$TS_START' and ts <= '$TS_END' order by ts;" > /tmp/hive-query.sql.tmp
beeline -n csso_mustafa -p Hive1234! -u "$CONN_ADDR" -f /tmp/hive-query.sql.tmp > query-executor-0.log

echo "select ts, msg, structured_data, unmatched, ns, app_name, app, hostname from logs where ns='$WH_NS' and app='metastore' and dt >= '$DT' and ts >= '$TS_START' and ts <= '$TS_END' order by ts;" > /tmp/hive-query.sql.tmp
beeline -n csso_mustafa -p Hive1234! -u "$CONN_ADDR" -f /tmp/hive-query.sql.tmp > metastore.log
