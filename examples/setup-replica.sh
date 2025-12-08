#!/bin/bash
set -e

# 1. Cleanup old data if any (for fresh start)
rm -rf /var/lib/postgresql/data/*

# 2. Wait for primary to be ready
until pg_isready -h postgres-primary -p 5432 -U user; do
  echo "Waiting for primary..."
  sleep 2
done

# 3. Clone data from primary (Base Backup)
echo "Cloning data from primary..."
export PGPASSWORD=reppass
pg_basebackup -h postgres-primary -p 5432 -U repuser -D /var/lib/postgresql/data -Fp -Xs -P -R

# 4. INJECT ARTIFICIAL LAG
# 'recovery_min_apply_delay' forces the replica to wait before applying WAL updates.
echo "Injecting 5 seconds replication lag..."
echo "recovery_min_apply_delay = '5s'" >> /var/lib/postgresql/data/postgresql.conf

# 5. Fix permissions (since we ran as root/bash)
chown -R postgres:postgres /var/lib/postgresql/data
chmod 700 /var/lib/postgresql/data

# 6. Start the Replica Server
echo "Starting Replica..."
exec su-exec postgres postgres