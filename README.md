# sync_db
using rsync tool to sync mysql binlog, make sure multi-machine keep the same db status.


# description
sync_db doing the same thing as master-slave mode. 

But when not using master-slave mode or can't using master-slave, you can use sync_db to sync binlog from server to client, this client maybe in private network and the server in public network (or even worse, in other country).

using rsync to sync binlog, fast and high performance.


# usage
befor using this tool, you should modify some parameters. please check:

```
python sync_db.py  -h
```
base usage, then modify or give parameters.

set your own mysql-server host, port, password, rsync command, work_dir 

when set ok ,then run this script.

```
python sync_db.py
```
