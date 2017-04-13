#!/usr/bin/python

from subprocess import PIPE,Popen
import shlex

def configure():
    '''
    '''
    
    result = {}
    
    result['local_host'] = 'local_ip'
    result['local_db_name'] = 'local_db_name'
    result['local_db_user'] = 'local_db_user'
    result['local_db_pass'] = 'local_db_pass'
    result['local_db_port'] = 'local_db_port'
    
    result['remote_host'] = 'remote_ip'
    result['remote_db_name'] = 'remote_db_name'
    result['remote_db_user'] = 'remote_db_user'
    result['remote_db_pass'] = 'remote_db_pass'
    result['remote_db_port'] = 'remote_db_port'
    
    result['table_name'] = 'table_name'
    
    return result

def dump_table(host_name,database_name,user_name,database_password,table_name):
    '''(str,str,str,str,str) -> str
    Returns the output of the shell command and dumps the named table only using
    the pg_dump shell tool.
    '''

    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -t public.{3} -Fc -f /tmp/table.dmp'\
    .format(host_name,database_name,user_name,table_name)

    p = Popen(command,shell=True,stdin=PIPE,stdout=PIPE,stderr=PIPE)

    #The command prompts for password which is submitted by the user using p.communicate()
    return p.communicate('{}\n'.format(database_password))

def restore_table(host_name,database_name,user_name,database_password):
    '''(str,str,str,str) -> str
    returns the output of the shell command and restores a dumped table from the
    file provided.
    '''

    #Remove the '<' from the pg_restore command.
    command = 'pg_restore -h {0} -d {1} -U {2} /tmp/table.dmp'\
              .format(host_name,database_name,user_name)

    #Use shlex to use a list of parameters in Popen instead of using the
    #command as is.
    command = shlex.split(command)

    #Let the shell out of this (i.e. shell=False). Using shell=True and the whole command
    #(i.e. not using shlex.split returned a password authentication error which suggests
    #that the password was not read correctly by p.communicate()
    p = Popen(command,shell=False,stdin=PIPE,stdout=PIPE,stderr=PIPE)

    #The command prompts for password which is submitted by the user using p.communicate()
    return p.communicate('{}\n'.format(database_password))

def main():
    
    #Configure this script collecting all the necessary parameters,users,passwords etc.
    Configuration = configure()
    
    #Dump the table in /tmp/table.dmp
    dump_table(Configuration.get('local_host'),\
               Configuration.get('local_db_name'),\
               Configuration.get('local_db_user'),\
               Configuration.get('local_db_pass'),\
               Configuration.get('table_name'),\
              )
    
    #Restore the table from /tmp/.table.dmp
    restore_table(Configuration.get('remote_host'),\
                  Configuration.get('remote_db_name'),\
                  Configuration.get('remote_db_user'),\
                  Configuration.get('remote_db_pass'),\
                 )

if __name__ == "__main__":
    main()
