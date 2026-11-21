#!/bin/bash
# Script to create HBase table for storing telecom real-time usage anomalies

echo "Creating 'telecom_anomalies' table in HBase..."
hbase shell <<EOF
create 'telecom_anomalies', 'alert_details'
list
exit
EOF
echo "HBase initialization complete!"
