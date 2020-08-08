# stage_cleaner
To clean up the application files under staging directories creating by MR tasks.

## USAGE
Basic usage:
* python3 stageCleaner.py schema-hdfs interval
* schema-hdfs means the nameservice of your HDFS cluster
* interval means how long the data you would like to keep

LOCAL: python3 stageCleaner.py schema-hdfs 10

NOHUP: nohup python3 stageCleaner.py schema-hdfs 10

For multi cluster usage, crontab would be the best choice if you don't want to start up the script manually.