# HDFS SUPER USER
SUPER_USER = "hdfs"

# HADOOP NAMENODE PORT
PORT = 50070

# HADOOP NAMENODE SCHEMA
SCHEMA = {
    'schema-hdfs': ["NameNode-Host1", "NameNode-Host2"]
}

# curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"
LIST_URL = "http://{host}:{port}/webhdfs/v1/{path}?user.name={super_user}&op=LISTSTATUS"

# curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE[&recursive=<true|false>]"
DELETE_URL = "http://{host}:{port}/webhdfs/v1/{path}?user.name={super_user}&op=DELETE&recursive={recursive}"

# curl -i -X PUT "<HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>"
RENAME_URL = "http://{host}:{port}/webhdfs/v1/{old_path}?user.name={super_user}&op=RENAME&destination={new_path}"

# DEFAULT PATH
DEFAULT_STAGING_PATH = "/user/{user}/.staging"

# TRASH PATH
TRASH_PATH = "/user/{user}/.Trash/Current"

# USER PATH
USER_PATH = "/user"

# LOGGING
LOG_FORMAT = '[%(asctime)s] [%(filename)s] [%(processName)s] [%(funcName)s] [line-%(lineno)s] [%(levelname)s] %(message)s'

# INTERVAL
INTERVAL = 10

# CMD
MKDIR_RECUR = "export HADOOP_USER_NAME=hdfs;hdfs dfs -mkdir -p hdfs://{schema}/user/{user}/.Trash/Current;hdfs dfs -chown -R {user}:{user} hdfs://{schema}/user/{user}/.Trash/Current"
