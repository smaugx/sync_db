#!/usr/bin/env python
# -*- coding:utf8 -*-

import os
import json
import time
import argparse
from slogging import slog

full_dump_return_code = {
        'error': 0,
        'success': 1,
        'already_load': 2
        }

class SyncDb(object):
    def __init__(self, rsync_cmd, work_dir = '/tmp/syncdb', host = '127.0.0.1', port = 3306, password = 'smaug'):
        self.host_ = host
        self.port_ = port
        self.password_ = password

        self.load_cache_ = {}
        self.init_lock_file_ = '/var/lib/.topargus_db_sync_lock'
        self.work_dir_ = work_dir
        self.rsync_cmd_ = rsync_cmd

        self.work_dir_temp_ = os.path.join(self.work_dir_, 'temp') # /root/smaug/topargus-db-sync/temp
        os.system('mkdir -p {0}'.format(self.work_dir_temp_))
        return

    def load_from_first_full_dump(self, full_sql_file):
        if os.path.exists(self.init_lock_file_):
            slog.warn('already load the full dump')
            return full_dump_return_code.get('already_load')

        binlog_index_file = os.path.join(self.work_dir_, 'mysql-bin.index')
        if not os.path.exists(binlog_index_file):
            slog.error('binlog_index file not found')
            return full_dump_return_code.get('error') 

        first_full_cmd = 'mysql -u root -p{0} -h {1} -P {2} < {3}'.format(self.password_, self.host_, self.port_, full_sql_file)

        last_binlog = None
        last_binlog_sql = None
        with open(binlog_index_file, 'r') as fin:
            for line in fin:
                line = line[:-1]
                binlog = line.split('/')[-1]
                abs_binlog = os.path.join(self.work_dir_, binlog)
                last_binlog  = abs_binlog
                last_binlog_sql = '{0}.sql'.format(os.path.join(self.work_dir_temp_, binlog))

                if first_full_cmd != None:
                    slog.debug(first_full_cmd)
                    os.popen(first_full_cmd)
                    slog.debug('cmd done')
                    first_full_cmd = None

                cmd = 'mysqlbinlog {0} | mysql -u root -p{1} -h {2} -P {3}'.format(abs_binlog, self.password_, self.host_, self.port_)
                slog.debug(cmd)
                os.popen(cmd)
                slog.debug('cmd done')

        if not last_binlog or not last_binlog_sql:
            slog.error('first load, make sure have at least one mysql-bin.00000x data, please run rsync command: {0} manual')
            return full_dump_return_code.get('error')

        cmd = 'mysqlbinlog {0} > {1}'.format(last_binlog, last_binlog_sql)
        slog.debug(cmd)
        os.popen(cmd)
        slog.debug('cmd done')

        cmd = 'tail -n 50 {0} |grep -a end_log_pos |tail -n 1'.format(last_binlog_sql)
        r = os.popen(cmd).readlines()
        if not r:
            slog.error("error: can not grep end_log_pos of last_binlog_sql:{0}".format(last_binlog_sql))
            return full_dump_return_code.get('error') 
        # ['#191231', '21:53:37', 'server', 'id', '321', 'end_log_pos', '2619792', 'Xid', '=', '469098']
        rl = r[-1].split()
        end_log_pos = ''
        for i in range(0, len(rl)):
            item = rl[i]
            if item == 'end_log_pos':
                end_log_pos = rl[i+1]

        if not end_log_pos:
            slog.error("error: can not grep end_log_pos of last_binlog_sql:{0}".format(last_binlog_sql))
            return full_dump_return_code.get('error')

        self.load_cache_ = {
                'last_binlog':last_binlog,
                'end_log_pos': end_log_pos
                }

        with open(self.init_lock_file_, 'w') as fout:
            fout.write(json.dumps(self.load_cache_))
            fout.close()
            slog.info('dump load_cache:{0} to file:{1}'.format(json.dumps(self.load_cache_), self.init_lock_file_))
        return full_dump_return_code.get('success')

    def rsync_binlog(self):
        cmd = self.rsync_cmd_
        slog.debug(cmd)
        result = os.popen(cmd).readlines()
        if result:
            slog.debug('rsync: {0}'.format(json.dumps(result)))
        slog.debug('cmd done')
        return

    def update_load_cache(self):
        try:
            with open(self.init_lock_file_, 'r') as fin:
                self.load_cache_ = json.loads(fin.read())
                slog.debug('update load_cache:{0} from file:{1}'.format(json.dumps(self.load_cache_), self.init_lock_file_))
            return True
        except Exception as e:
            slog.error('catch exception:{0}'.format(e))
            return False
        return False


    def load_from_binlog(self):
        if not self.update_load_cache():
            return False

        binlog_index_file = os.path.join(self.work_dir_, 'mysql-bin.index')
        if not os.path.exists(binlog_index_file):
            slog.error('binlog_index file not found')
            return False

        old_last_binlog = self.load_cache_.get('last_binlog')
        old_end_log_pos = self.load_cache_.get('end_log_pos')

        run = False
        last_binlog = None
        last_binlog_sql = None

        ready_binlog_list = []
        with open(binlog_index_file, 'r') as fin:
            for line in fin:
                line = line[:-1]
                binlog = line.split('/')[-1]
                abs_binlog = os.path.join(self.work_dir_, binlog)
                if abs_binlog == old_last_binlog:
                    run = True

                if not run:
                    continue

                ready_binlog_list.append(abs_binlog)

                last_binlog     = abs_binlog
                last_binlog_sql = '{0}.sql'.format(os.path.join(self.work_dir_temp_, binlog))
            fin.close()


        if not last_binlog or not last_binlog_sql:
            return False

        cmd = 'mysqlbinlog {0} > {1}'.format(last_binlog, last_binlog_sql)
        slog.debug(cmd)
        os.popen(cmd)
        slog.debug('cmd done')

        cmd = 'tail -n 50 {0} |grep -a end_log_pos |tail -n 1'.format(last_binlog_sql)
        r = os.popen(cmd).readlines()
        if not r:
            slog.error("error: can not grep end_log_pos of last_binlog_sql:{0}".format(last_binlog_sql))
            return False
        # ['#191231', '21:53:37', 'server', 'id', '321', 'end_log_pos', '2619792', 'Xid', '=', '469098']
        rl = r[-1].split()
        end_log_pos = ''
        for i in range(0, len(rl)):
            item = rl[i]
            if item == 'end_log_pos':
                end_log_pos = rl[i+1]

        if not end_log_pos:
            slog.error("error: can not grep end_log_pos of last_binlog_sql:{0}".format(last_binlog_sql))
            return False

        self.load_cache_ = {
                'last_binlog':last_binlog,
                'end_log_pos': end_log_pos
                }

        with open(self.init_lock_file_, 'w') as fout:
            fout.write(json.dumps(self.load_cache_))
            fout.close()
            slog.debug('dump load_cache:{0} to file:{1}'.format(json.dumps(self.load_cache_), self.init_lock_file_))

        if last_binlog ==  old_last_binlog and end_log_pos == old_end_log_pos:
            slog.info('binlog and end_log_pos both same with last time, no need load')
            return True

        if last_binlog ==  old_last_binlog and end_log_pos != old_end_log_pos:
            start_position = old_end_log_pos  # TODO(smaug)
            cmd = 'mysqlbinlog {0} --start-position {1} | mysql -u root -p{2} -h {3} -P {4}'.format(abs_binlog, start_position, self.password_, self.host_, self.port_)
            slog.info(cmd)
            os.popen(cmd)
            slog.debug('cmd done')
            return True

        # new binlog sync, start from the second item, index is 1
        for i in range(1, len(ready_binlog_list)):
            abs_binlog = ready_binlog_list[i]
            cmd = 'mysqlbinlog {0} | mysql -u root -p{1} -h {2} -P {3}'.format(abs_binlog, self.password_, self.host_, self.port_)
            slog.debug(cmd)
            os.popen(cmd)
            slog.debug('cmd done')

        return True

    def run(self, full_sql_file = ''):
        result = self.load_from_first_full_dump(full_sql_file)
        if result == full_dump_return_code.get('error'):
            slog.error('error, error, please make sure then run again')
            return False

        slog.debug('first load finished, will run rsync forever')
        time.sleep(2)

        while True:
            slog.debug('wait 20 s, then rsync...')
            time.sleep(20)
            try:
                self.rsync_binlog()
                self.load_from_binlog()
            except Exception as e:
                slog.error('catch exception:{0}'.format(e))
        return




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.description='sync_db ，using rsync to sync binlog'
    parser.add_argument('-H', '--host', help='client mysql host', default='127.0.0.1')
    parser.add_argument('-P', '--port', help='client mysql port', default= 3306)
    parser.add_argument('-a', '--password', help='client mysql password', default='smaug')
    parser.add_argument('-d', '--dir', help='work dir, put rsync files into this dir', default='/root/smaug/topargus-db-sync')
    parser.add_argument('-f', '--file', help="the first time full_dump sql file", default='/root/smaug/topargus-db-sync/topargus_201912311800.sql')
    parser.add_argument('-r', '--rsync', help="rsync command", required=True)
    args = parser.parse_args()

    host     = args.host
    port     = args.port
    password = args.password
    work_dir = args.dir
    #rsync_cmd = 'rsync -avHe "ssh -p 1022 -i /root/.ssh/top-digital-ocean.pem" root@192.168.1.1::dashdb /root/smaug/topargus-db-sync/'
    rsync_cmd = args.rsync
    full_sql_file = args.file

    sync = SyncDb(rsync_cmd = rsync_cmd, work_dir = work_dir, host = host, port = port, password = password)
    sync.run(full_sql_file = full_sql_file)
