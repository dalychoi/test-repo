#!/usr/bin/env python3

import os
import sys
import time
import re
from subprocess import Popen, PIPE
from multiprocessing import Process, Pool
import pprint

conn = 'soe/soe@//krx2a01-ibvip:1522/pkrx2a'

def get_target(sql, conn):
    ses = Popen(['sqlplus', '-s', conn], stdin=PIPE, stdout=PIPE, stderr=PIPE, universal_newlines=True)
    ses.stdin.write('set timing on feedback 1 lines 200 pages 0\n')
    ses.stdin.write(sql)
    result, error = ses.communicate()
    if error:
        print(error)
        exit
    else:
        #res = result.decode('utf-8').split('\n')
        res = result.split('\n')
    return res

def gen_table_list(target):
    table_list = []
    for r in target:
        c = re.split(r'\t+', r)
        if len(c) == 2:
            if c[1].strip() != 'SEQUENCE':
                t = c[0].strip(), c[1].strip()
                table_list.append(t)
    return table_list

def gen_sql_list(table_list):
    sql_list = []
    for r in table_list:
        sql = "select 'Count', count(*) from " + r[0] + ";"
        sql_list.append(sql)
    return sql_list

def run_sql(sql, conn):
    tab = ''
    if re.search(r'select.*from', sql):
        tab = re.sub(r'select.*from', '', sql).strip().rstrip(';')
    print('pid: {0:7}    table: {1:20}'.format(os.getpid(), tab))
    ses = Popen(['sqlplus', '-s', conn], stdin=PIPE, stdout=PIPE, stderr=PIPE, universal_newlines=True)
    ses.stdin.write('set timing on feedback 1 lines 200 pages 0 trim on trimspool on\n')
    ses.stdin.write('prompt ' + sql + '\n')
    ses.stdin.write(sql)
    result, error = ses.communicate()
    tab = ''
    cnt = '0'
    ela = '00:00:00.00'
    if error:
        print(error)
        exit
    else:
        #res = result.decode('utf-8').split('\n')
        res = result.split('\n')
        for r in res:
            if re.search(r'select.*from', r):
                tab = re.sub(r'select.*from', '', r).strip()
            if re.search(r'^Count', r):
                cnt = r.split()[1]
            if re.search(r'^Elapsed:', r):
                ela = r.split()[1]
    print('pid: {0:7}    table: {1:20}    count: {2:>12}    elapsed: {3}'.format(os.getpid(), tab, cnt, ela))
    return res

def run_sql2(sql):
    return run_sql(sql, conn)

def main():
    sql = '''select table_name, num_rows
             from   user_tables
             order by num_rows desc;
          '''

    target = get_target(sql, conn)
    table_list = gen_table_list(target)
    sql_list = gen_sql_list(table_list)

    #for r in target:
    #    print('{}'.format(r))
    pprint.pprint(target, width=80)

    p = Pool(4)

    stime = time.time()
    results = []
    results.append(p.map_async(run_sql2, sql_list).get())
    etime = time.time()

    #for r in results:
    #    for s in r:
    #        for t in s:
    #            print(t)
    print('Duration: {0:.2f}'.format(etime - stime))

if __name__ == '__main__':
    main()
