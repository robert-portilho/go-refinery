#!/bin/bash
# Wait for SQL Server to be ready
echo "Waiting for SQL Server to be ready..."
sleep 20

# Run init script
echo "Running init.sql..."
/opt/mssql-tools18/bin/sqlcmd -S sqlserver -U sa -P YourStrong!Passw0rd -d master -i /scripts/init.sql -C

echo "SQL Server initialization completed."
