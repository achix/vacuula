import psycopg2
import pkgutil
import argparse
import sys
import csv
import os

def chk_autovacuum_global(constr):
    conn = psycopg2.connect(constr)
    conn.set_session(autocommit=True)
    try:
        with conn.cursor() as curs:
            curs.execute("SELECT current_setting('autovacuum');")
            if (curs.fetchone()[0] in ["off","false"]):
                print('ALERT! autovacuum is globally disabled. Enable it now.')
                exit(9)
    finally:
        conn.close()

def process_res(csvfilename,constr, sqlqry, minsize, min_age_overdue):
    conn = psycopg2.connect(constr)
    conn.set_session(autocommit=False)
    
    try:
        with conn.cursor(name="vacurs") as curs:
            #curs.scrollable = True
            i = 0
            curs.execute(sqlqry,(minsize,min_age_overdue))
            recordlist = []
            for record in curs:
                #print(record)
                coid = record[0]
                relkind = record[1]
                fullname = record[2]
                mainrel = record[3]
                mainrel_str = ""
                if (mainrel):
                    mainrel_str = " of table "+mainrel
                relation_size = record[4]
                age = record[5]
                mxid_age = record[6]
                autovacuum__effective = record[7]                
                if (not autovacuum__effective):
                    print("WARNING! autovacuum disabled for table "+fullname+mainrel_str+" Be sure to have VACUUM scheduled in place.", file=sys.stderr)
                    continue
                vacuum_freeze_table_age__pertable = record[8]
                autovacuum_freeze_max_age__pertable = record[9]
                vacuum_freeze_table_age__pertable_global = record[10]
                autovacuum_freeze_max_age__pertable_global = record[11]                
                autovacuum_freeze_max_age__effective = record[12] # == autovacuum_freeze_max_age__pertable_global 
                vacuum_freeze_table_age__effective = record[13]
                
                print ((coid,relkind,fullname,mainrel,relation_size,age,mxid_age,autovacuum__effective, vacuum_freeze_table_age__pertable, autovacuum_freeze_max_age__pertable, vacuum_freeze_table_age__pertable_global, autovacuum_freeze_max_age__pertable_global, autovacuum_freeze_max_age__effective, vacuum_freeze_table_age__effective ))
                recordlist.append((coid,relkind,fullname,mainrel,relation_size,age,mxid_age,autovacuum__effective, vacuum_freeze_table_age__pertable, autovacuum_freeze_max_age__pertable, vacuum_freeze_table_age__pertable_global, autovacuum_freeze_max_age__pertable_global, autovacuum_freeze_max_age__effective, vacuum_freeze_table_age__effective))
                i += 1
            #print(i)
            #curs.scroll(0,mode="absolute")
            os.umask(0o0077)
            csvfile=open(csvfilename,'w',newline='')
            obj=csv.writer(csvfile)
            #obj.writerows(curs.fetchall())
            obj.writerows(recordlist)
            csvfile.close()
            
            print(recordlist)
            
        conn.commit()
    finally:
        conn.close()

def setup(mode):
    
    from pathlib import Path
    homedir = str(Path.home())
    path_dot_vacuula = os.path.join(homedir,".vacuula")
    if (not os.path.exists(path_dot_vacuula)):
        os.mkdir(path_dot_vacuula, 0o700, dir_fd=None) 
    
    path_cvs_file = os.path.join(homedir,".vacuula","vacuum_values.csv")
    #print (path_cvs_file) 
    
    if (mode == "boost" and  os.path.isfile(path_cvs_file)):
        print("ERROR: boost mode given but data file "+ path_cvs_file +" exists")
        exit(8)
    elif (mode == "restore" and not os.path.isfile(path_cvs_file)):
        print("ERROR: restore mode given but data file "+ path_cvs_file +" does not seem to exist")
        exit(8)
            
    return path_cvs_file

def restore():
    pass

def main(args):
    try:
        conn_str = args.conn_str;
        #sqlqrystr = pkgutil.get_data('res', 'vacuum_file.sql')
        sqlqrystr = pkgutil.get_data('res', 'candidate_tables_templ.sql')
        min_size = args.min_size
        min_age_overdue = args.min_age_overdue
        mode = args.mode
    except Exception as e:
        print(e)
        sys.exit(1)
    
    csvfilename = setup(mode)
    chk_autovacuum_global(conn_str)
    if (mode == "boost"):
        process_res(csvfilename, conn_str, sqlqrystr,min_size, min_age_overdue)
    elif (mode == "restore"):
        restore()
    else:
        print("ERROR: impossible mode "+mode)
        exit(8)
        

parser = argparse.ArgumentParser(description='A tool for managing your vacuum')
parser.add_argument('mode', type=str, help='the mode of operation. "boost" to make autovacuum more drastic, "restore" to get the old settings back', choices=['boost','restore'])
parser.add_argument('conn_str', type=str, help='connection string to the DB')
parser.add_argument('--min-size', type=int, help='the table minsize in bytes (default=0)', default=0)
parser.add_argument('--min-age-overdue', type=int, help='min num of xactions elapsed after vacuum_freeze_table_age__effective, alt. age-vacuum_freeze_table_age__effective>=min-age-overdue (default=0)', default=0)
args = parser.parse_args()
main(args)

