
import os
from os.path import join
import sys
import collections
import argparse
import pymysql
import subprocess
import datetime
import time

from com.glezo.staticFileSystemFunctions.StaticFileSystemFunctions  import StaticFileSystemFunctions
from com.glezo.stringUtils.StringUtils                              import StringUtils

#-------------------------------------------------------------------
def dump_database_structure(cursor,database_name):
    total               =   'CREATE DATABASE IF NOT EXISTS '+database_name+';\n'
    total               +=  'USE '+database_name+';\n\n'
    database_tables     =   []
    database_views      =   []
    database_events     =   []
    database_procedures =   []
    database_functions  =   []
    database_triggers   =   []
    cursor.execute("SELECT TABLE_NAME     FROM INFORMATION_SCHEMA.TABLES   WHERE TABLE_SCHEMA  ='"+database_name+"'")
    for current_row in cursor:  database_tables.append([current_row[0],None])
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS        WHERE TABLE_SCHEMA='"+database_name+"'")
    for current_row in cursor:  database_views.append([current_row[0],None])
    cursor.execute("SELECT EVENT_NAME     FROM INFORMATION_SCHEMA.EVENTS   WHERE EVENT_SCHEMA  ='"+database_name+"'")
    for current_row in cursor:  database_events.append([current_row[0],None])
    cursor.execute("SELECT ROUTINE_NAME   FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA='"+database_name+"' AND ROUTINE_TYPE='PROCEDURE'")
    for current_row in cursor:  database_procedures.append([current_row[0],None])
    cursor.execute("SELECT ROUTINE_NAME   FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA='"+database_name+"' AND ROUTINE_TYPE='FUNCTION'")
    for current_row in cursor:  database_functions.append([current_row[0],None])
    cursor.execute("SELECT TRIGGER_NAME   FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA='"+database_name+"'")
    for current_row in cursor:  database_triggers.append([current_row[0],None])


    for i in database_tables:
        cursor.execute("SHOW CREATE TABLE "+database_name+"."+i[0])
        for j in cursor:    
            create_query                    =   j[1]
            #beautifications
            create_query                    =   create_query.replace('`','')
            create_query                    =   create_query.replace('(\n','\n(\n')
            create_query                    =   create_query.replace('CREATE TABLE','CREATE TABLE IF NOT EXISTS')
            create_query_lines              =   create_query.split('\n')
            create_query_beautified_lines   =   create_query_lines[0:2]
            max_column_name_length          =   0
            for current_line in create_query_lines[2:-1]:
                field_name=current_line.strip().split(' ')[0]
                max_column_name_length=max(max_column_name_length,len(field_name))
            for current_line in create_query_lines[2:-1]:
                tokens = current_line.strip().split(' ')
                if(tokens[0] in ['PRIMARY','KEY','UNIQUE','CONSTRAINT','FOREIGN']):
                    beautified_line='    '+' '.join(tokens)
                else:
                    beautified_line = '    '+tokens[0]+' '
                    for _ in range(len(tokens[0]),max_column_name_length):  beautified_line+=' '
                    beautified_line+=' '.join(tokens[1:])
                create_query_beautified_lines.append(beautified_line)
            create_query_beautified_lines.append(create_query_lines[-1:][0])
            create_query='\n'.join(create_query_beautified_lines)+';'
            i[1]=create_query
            total           +=  create_query+'\n\n'

    for i in database_views:
        cursor.execute("SHOW CREATE VIEW "+database_name+"."+i[0])
        for j in cursor:    
            create_query    =   j[1]
            #beautifications
            create_query    =   create_query.replace('`','')
            create_query    =   create_query.split('VIEW',1)
            aux             =   create_query[1].split('AS',1)
            view_name       =   aux[0]
            #TODO! sqlparser
            view_definition =   aux[1]
            create_query    =   'CREATE VIEW IF NOT EXISTS '+view_name+' AS'+'\n'+view_definition.strip()
            i[1]=create_query
            total           +=  create_query+'\n\n'
            
    for i in database_events:
        cursor.execute("SHOW CREATE EVENT "+database_name+"."+i[0])
        for j in cursor:
            create_query    =   j[3]
            #beautifications
            create_query    =   create_query.replace('`','')
            create_query    =   create_query.split('EVENT',1)
            aux             =   create_query[1].split('DO BEGIN',1)
            create_query    =   'CREATE EVENT IF NOT EXISTS '+aux[0]+' DO\nBEGIN'+aux[1]
            create_query    =   create_query.replace('\r\n','\n')
            create_query    =   'DELIMITER $$\n'+create_query+' $$\nDELIMITER ;'
            i[1]            =   create_query
            total           +=  create_query+'\n\n'

    for i in database_procedures:
        cursor.execute("SHOW CREATE PROCEDURE "+database_name+"."+i[0])
        for j in cursor:
            create_query    =   j[2]
            #beautifications
            create_query    =   create_query.replace('`','')
            create_query    =   create_query.split('PROCEDURE',1)
            create_query    =   'CREATE PROCEDURE IF NOT EXISTS '+create_query[1]
            create_query    =   create_query.replace('\r\n','\n')
            create_query    =   'DELIMITER $$\n'+create_query+' $$\nDELIMITER ;'
            i[1]            =   create_query
            total           +=  create_query+'\n\n'

    for i in database_functions:
        cursor.execute("SHOW CREATE FUNCTION "+database_name+"."+i[0])
        for j in cursor:
            create_query    =   j[2]
            #beautifications
            create_query    =   create_query.replace('`','')
            create_query    =   create_query.split('FUNCTION',1)
            create_query    =   'CREATE FUNCTION IF NOT EXISTS '+create_query[1]
            create_query    =   create_query.replace('\r\n','\n')
            create_query    =   'DELIMITER $$\n'+create_query+' $$\nDELIMITER ;'
            i[1]            =   create_query
            total           +=  create_query+'\n\n'

    for i in database_triggers:
        cursor.execute("SHOW CREATE TRIGGER "+database_name+"."+i[0])
        for j in cursor:    
            create_query                    =   j[2]
            #beautifications
            create_query    =   create_query.replace('`','')
            create_query    =   create_query.split('TRIGGER',1)
            aux             =   create_query[1].split('BEGIN',1)
            create_query    =   'CREATE TRIGGER IF NOT EXISTS '+aux[0]+'BEGIN'+aux[1]
            create_query    =   create_query.replace('\r\n','\n')
            create_query    =   'DELIMITER $$\n'+create_query+' $$\nDELIMITER ;'
            i[1]            =   create_query
            total           +=  create_query+'\n\n'


            
    result              =   collections.OrderedDict()
    result['total']     =   total
    result['tables']    =   database_tables
    result['views']     =   database_views
    result['events']    =   database_events
    result['procedures']=   database_procedures
    #TODO! functions
    result['triggers']  =   database_triggers

    return result
#-------------------------------------------------------------------
def print_usage():
    result= 'mysql_backup.py'+'\n'
    result+='[-h]  [--help]                               prints this help'                                                                                  +'\n'
    result+='-H    --host                      <host>'                                                                                                       +'\n'
    result+='-P    --port                      <port>'                                                                                                       +'\n'
    result+='-u    --user                      <user>'                                                                                                       +'\n'
    result+='-p    --password                  <password>'                                                                                                   +'\n'
    result+='[-D]  [--databases]               <list>     Database(s) to dump     (separated by ,). All databases included if not specified'                 +'\n'
    result+='[-E]  [--exclude-database]        <list>     Database(s) not to dump (separated by ,)'                                                          +'\n'
    result+='[-Ed] [--exclude-database-data]   <list>     Database(s) not to dump data (separated by ,)'                                                     +'\n'
    result+='[-s]  [--structure]                          Dumps database(s) structure'                                                                       +'\n'
    result+='[-d]  [--data]                               Dumps database(s) data'                                                                            +'\n'
    result+='[-m]  [--mysqldump]               <path>     mysqldump binary path'                                                                             +'\n'
    result+='[-J]  [--join-databases]                     If -o specified, writes all databases (structure and data) to the same file. False by default'     +'\n'
    result+='[-o]  [--output]                  <path>     Folder/file output (depending on -J). stdout if not specified'                                     +'\n'
    result+='[-S]  [--suffix]                             If -o specified, will add yyyy-mm-dd_hh.mm.ss to the outer most file/folder'                       +'\n'
    result+='[-z]  [--zip]                                If -o specified, will compress to zip the outer most file/folder, and delete file/folder after'    +'\n'
    result+=                                                                                                                                                 '\n'
    result+='mysql_backup.py -H 192.168.1.2 -P 3306 -u root -p my_password -E mysql,phpmyadmin,information_schema,performance_schema,cr_debug -s -d -m C:/xampp/mysql/bin/mysqldump.exe -S -o C:/data/_di/db_home_backup/db_home_backup -z'
    return result
#-------------------------------------------------------------------
if __name__ == '__main__':
    
    
    argument_parser = argparse.ArgumentParser(usage=print_usage(),add_help=False)
    argument_parser.add_argument('-h'  ,'--help'                    ,action='store_true' ,default=False    ,dest='help'           ,required=False  )
    argument_parser.add_argument('-H'  ,'--host'                    ,action='store'      ,default=False    ,dest='host'           ,required=True   )
    argument_parser.add_argument('-P'  ,'--port'                    ,action='store'      ,default=False    ,dest='port'           ,required=True   )
    argument_parser.add_argument('-u'  ,'--user'                    ,action='store'      ,default=False    ,dest='user'           ,required=True   )
    argument_parser.add_argument('-p'  ,'--password'                ,action='store'      ,default=False    ,dest='password'       ,required=True   )
    argument_parser.add_argument('-D'  ,'--database'                ,action='store'      ,default=None     ,dest='databases'      ,required=False  )
    argument_parser.add_argument('-E'  ,'--exclude-database'        ,action='store'      ,default=None     ,dest='exclude'        ,required=False  )
    argument_parser.add_argument('-Ed' ,'--exclude-database-data'   ,action='store'      ,default=None     ,dest='exclude_data'   ,required=False  )
    argument_parser.add_argument('-s'  ,'--structure'               ,action='store_true' ,default=False    ,dest='structure'      ,required=False  )
    argument_parser.add_argument('-d'  ,'--data'                    ,action='store_true' ,default=False    ,dest='data'           ,required=False  )
    argument_parser.add_argument('-m'  ,'--mysqldump'               ,action='store'      ,default=None     ,dest='mysqldump_path' ,required=False  )
    argument_parser.add_argument('-j'  ,'--join-structure-and-data' ,action='store_true' ,default=False    ,dest='join_s_and_d'   ,required=False  )
    argument_parser.add_argument('-J'  ,'--join-databases'          ,action='store_true' ,default=False    ,dest='join_dbs'       ,required=False  )
    argument_parser.add_argument('-o'  ,'--output'                  ,action='store'      ,default=None     ,dest='output'         ,required=False  )    
    argument_parser.add_argument('-S'  ,'--suffix'                  ,action='store_true' ,default=False    ,dest='suffix'         ,required=False  )    
    argument_parser.add_argument('-z'  ,'--zip'                     ,action='store_true' ,default=False    ,dest='compress'       ,required=False  )    

    argument_parser_result = argument_parser.parse_args()
 
    option_help     =   argument_parser_result.help
    dump_structure  =   argument_parser_result.structure
    dump_data       =   argument_parser_result.data
    output          =   argument_parser_result.output
    join_s_and_d    =   argument_parser_result.join_s_and_d
    join_dbs        =   argument_parser_result.join_dbs
    mysqldump_path  =   argument_parser_result.mysqldump_path
    host            =   argument_parser_result.host
    port            =   argument_parser_result.port
    user            =   argument_parser_result.user
    password        =   argument_parser_result.password
    suffix          =   argument_parser_result.suffix
    compress        =   argument_parser_result.compress
    
    if(option_help):
        print(print_usage())
        sys.exit(0)
    
    if(not dump_structure and not dump_data):
        print(print_usage())
        print('Unless one from -s and -d is required')
        sys.exit(1)
    
    if(output==None and (join_s_and_d or join_dbs)):
        print(print_usage())
        print('If -o/--output is not specified, -j and -J are assumed and not allowed')
        sys.exit(1)
    
    if(output==None and suffix):
        print(print_usage())
        print('If -o/--output is not specified, -s/--suffix just make no sense')
        sys.exit(1)
    
    if(output==None and compress):
        print(print_usage())
        print('If -o/--output is not specified, -z/--zip just make no sense')
        sys.exit(1)
    
    if(join_s_and_d and join_dbs):
        print(print_usage())
        print('If -J is specified, -j are assumed and not allowed')
        sys.exit(1)
    
    if(dump_data and mysqldump_path==None):
        print(print_usage())
        print('If -d is specified, -m/--mysqldump is mandatory')
        sys.exit(1)
    
    
    
    #if -o!=None and     -J, -o is considered as file.   All databases (structure and data) to the same file.
    #if -o!=None and not -J, -o is considered as folder. Each database to the same file (if -j) or two files (otherwise).
    output_type = None
    if(suffix):
        effective_suffix    =   datetime.datetime.now().strftime('%Y-%m-%d_%H.%M.%S')
        output              +=  effective_suffix 
    if(join_dbs):
        output_type =   'file'
        output      +=  '.sql'
        if(StaticFileSystemFunctions.fileExists(output)):
            deleted=StaticFileSystemFunctions.deleteFile(output)
            if(not deleted):
                print('Couldnt delete file '+output+' !!!')
                sys.exit(1)
        created=StaticFileSystemFunctions.createFileIfNotExists(output)
        if(not created):
            print('Couldnt create file '+output+' !!!')
            sys.exit(1)
    elif(output!=None):
        output_type =   'folder'
        if(StaticFileSystemFunctions.folderExists(output)):
            deleted=StaticFileSystemFunctions.deleteFolder(output)
            if(not deleted):
                print('Couldnt delete folder '+output+' !!!')
                sys.exit(1)
        created=StaticFileSystemFunctions.createFolder(output)
        if(not created):
            print('Couldnt create folder '+output+' !!!')
            sys.exit(1)
        
    
    connection              =   pymysql.connect(host=host,port=int(port),user=user,passwd=password)
    cursor                  =   connection.cursor()
    
    databases_to_include        =   argument_parser_result.databases
    databases_to_exclude        =   []
    databases_data_to_exclude   =   []
    if(databases_to_include==None):     databases_to_include=[]
    else:                               databases_to_include=databases_to_include.split(',')
    if(argument_parser_result.exclude!=None):
        databases_to_exclude        =   argument_parser_result.exclude.split(',')
    if(argument_parser_result.exclude_data!=None):
        databases_data_to_exclude   =   argument_parser_result.exclude_data.split(',')
    
    server_databases        =   []
    cursor.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA")
    for current_row in cursor:
        server_databases.append(current_row[0])
 
    server_databases        =   [x for x in server_databases if x not in databases_to_exclude]
    if(databases_to_include!=[]):
        server_databases    =   [x for x in server_databases if x     in databases_to_include]
    
    start_time = time.time()
    i=0
    for current_database in server_databases:
        i+=1
        print('-- ['+str(i)+'/'+str(len(server_databases))+']'+'\t\t'+'Dumping '+current_database)
        #structure
        current_database_structure  =   dump_database_structure(cursor,current_database)
        #data, if appropiate
        StaticFileSystemFunctions.deleteFile('./mysqldump_data.temp')
        if(current_database not in databases_data_to_exclude):
            mysqldump_args = [mysqldump_path,'-h',host,'-P',port,'-u',user,'-p'+password,'--no-create-info','--skip-triggers','--databases',current_database]
            if(output==None):
                print(current_database_structure+'\n')
                os.system(' '.join(mysqldump_args))
            else:
                try:
                    #can't just retrieve the output from subprocess.check_output, since it might be HUGE
                    foo=' '.join(mysqldump_args)+' > ./mysqldump_data.temp'
                    os.system(foo)        
                except subprocess.CalledProcessError:
                    print('Exception when dumping data from database '+current_database)
                    print('Aborting dumping of all databases!')
                    #TODO! cleanup
                    sys.exit(2)
        #output (structure, and maybe data)
        if(join_dbs):
            StaticFileSystemFunctions.appendToFile(output,current_database_structure['total']+'\n')
            StaticFileSystemFunctions.concatenateFiles(output,'./mysqldump_data.temp')
        else:
            if(join_s_and_d):
                current_database_target_file            =   join(output,current_database+'.sql')
                StaticFileSystemFunctions.appendToFile(current_database_target_file,current_database_structure['total']+'\n')
                StaticFileSystemFunctions.concatenateFiles(current_database_target_file,'./mysqldump_data.temp')
            else:
                current_database_structure_target_file  =   join(output,current_database+'_STRUCTURE.sql')   
                current_database_data_target_file       =   join(output,current_database+'_DATA.sql')
                StaticFileSystemFunctions.appendToFile(current_database_structure_target_file,current_database_structure['total']+'\n')
                if(StaticFileSystemFunctions.fileExists('./mysqldump_data.temp')):      #maybe this database was in -Ed 
                    StaticFileSystemFunctions.concatenateFiles(current_database_data_target_file,'./mysqldump_data.temp')
    StaticFileSystemFunctions.deleteFile('./mysqldump_data.temp')
    end_time                =   time.time()
    elapsed_seconds         =   end_time - start_time
    elapsed_seconds_string  =   StringUtils.seconds_to_time_string(elapsed_seconds)

    if(compress):
        if(output_type=='folder'):
            StaticFileSystemFunctions.compress_folder_zip(output)
            StaticFileSystemFunctions.deleteFolder(output)
        else:
            StaticFileSystemFunctions.compress_file_zip(output)
            StaticFileSystemFunctions.deleteFile(output)
    print('-- DONE!'+'\t\t'+'Time elapsed: '+elapsed_seconds_string)
    
    
