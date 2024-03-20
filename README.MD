# MYSQL REPLICATION

## Configuring MySQL

Check whether the log-bin option is already on:
```sql
// for MySql 5.x
mysql> SELECT variable_value as "BINARY LOGGING STATUS (log-bin) ::"
FROM information_schema.global_variables WHERE variable_name='log_bin';
// for MySql 8.x
mysql> SELECT variable_value as "BINARY LOGGING STATUS (log-bin) ::"
FROM performance_schema.global_variables WHERE variable_name='log_bin';
```
Enable bin log and GTID in mysql configuration if not enabled in my.cnf

```
# enable bin log
server-id         = 223344
log_bin           = mysql-bin
binlog_format     = ROW
binlog_row_image  = FULL
expire_logs_days  = 30
binlog_rows_query_log_events=ON



# enable gtid
gtid_mode=ON
enforce-gtid-consistency=ON
## Create MySQL user for replication
```sql
CREATE USER 'replication-user'@'%' IDENTIFIED BY 'replication-password';
GRANT SELECT,EVENT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replication-user'@'%';
FLUSH PRIVILEGES;
```


# Set up RabbitMQ
## Installation
```shell
sudo apt-get install rabbitmq-server
```

## Create a RabbitMQ user, a virtual host and allow that user access to that virtual host
```shell
sudo rabbitmqctl add_user myuser mypassword
sudo rabbitmqctl add_vhost replication-vhost
sudo rabbitmqctl set_permissions -p replication-vhost myuser ".*" ".*" ".*"
```
Substitute in appropriate values for myuser, mypassword and myvhost above.




# Recovery
## list snapshots
```shell



```
## Get dump
```shell
python admin.py getdump kenbo-pc0266_2024-03-07_11-41-03_snapshot > /tmp/dump.sql

```
## Get logs