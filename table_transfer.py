#!/usr/bin/python

from subprocess import PIPE,Popen
import shlex

def dump_table(host_name,database_name,user_name,database_password,table_name):

    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -t public.{3} -Fc -f /tmp/table.dmp'\
    .format(host_name,database_name,user_name,table_name)

    p = Popen(command,shell=True,stdin=PIPE,stdout=PIPE,stderr=PIPE)

    return p.communicate('{}\n'.format(database_password))

def restore_table(host_name,database_name,user_name,database_password):

    #Remove the '<' from the pg_restore command.
    command = 'pg_restore -h {0} -d {1} -U {2} /tmp/table.dmp'\
              .format(host_name,database_name,user_name)

    #Use shlex to use a list of parameters in Popen instead of using the
    #command as is.
    command = shlex.split(command)

    #Let the shell out of this (i.e. shell=False)
    p = Popen(command,shell=False,stdin=PIPE,stdout=PIPE,stderr=PIPE)

    return p.communicate('{}\n'.format(database_password))

def main():
    dump_table('localhost','testdb','user_name','passwd','test_tbl')
    restore_table('localhost','testdb','user_name','passwd')

if __name__ == "__main__":
    main()
