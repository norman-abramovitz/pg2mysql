#!/bin/bash
#
# Entrypoint for docker container

service postgresql start
service mysql start

echo "USE mysql; ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'admin';" | mysql
echo "CREATE ROLE root superuser createdb login;" | su postgres -c psql
exec "$@"