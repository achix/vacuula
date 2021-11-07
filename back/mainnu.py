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
            i = 0
            curs.execute("SELECT current_setting('autovacuum');")
            if (curs.fetchone()[0] in ["off","false"]):
                print('ALERT! autovacuum is globally disabled. Enable it now.')
                exit(9)
    finally:
        conn.close()

def process_res(constr, sqlqry, minsize, min_age_overdue):
    conn = psycopg2.connect(constr)
    conn.set_session(autocommit=False)
    from pathlib import Path
    homedir = str(Path.home())
    csvfilename=homedir+'/vacuum_values.csv'
    #csvfile = None
    #try:
    #    csvfile=open(homedir+'/vacuum_values.csv','r')
    #except:
    #    pass
    
    #if (csvfile):
    #    print("ERROR data file "+ csvfile.name +" found")
    #    exit(8)
            
    if (os.path.isfile(homedir+'/vacuum_values.csv')):
        print("ERROR data file "+ csvfilename +" found")
        exit(8)
    
    try:
        with conn.cursor(name="vacurs") as curs:
            curs.scrollable = True
            i = 0
            curs.execute(sqlqry,(minsize,min_age_overdue))
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
                vacuum_freeze_table_age__pertable = record[8]
                autovacuum_freeze_max_age__pertable = record[9]
                vacuum_freeze_table_age__pertable_global = record[10]
                autovacuum_freeze_max_age__pertable_global = record[11]                
                autovacuum_freeze_max_age__effective = record[12] # == autovacuum_freeze_max_age__pertable_global 
                vacuum_freeze_table_age__effective = record[13]
                
                print ((coid,relkind,fullname,mainrel,relation_size,age,mxid_age,autovacuum__effective, vacuum_freeze_table_age__pertable, autovacuum_freeze_max_age__pertable, vacuum_freeze_table_age__pertable_global, autovacuum_freeze_max_age__pertable_global, autovacuum_freeze_max_age__effective, vacuum_freeze_table_age__effective ))
                i += 1
            #print(i)
            curs.scroll(0,mode="absolute")
            
            csvfile=open(csvfilename,'w',newline='')
            obj=csv.writer(csvfile)
            obj.writerows(curs.fetchall())
            csvfile.close()
            
            #with open('~/vacuum_values.csv', newline='') as csvfile:
            #    cvsreader = csv.reader(csvfile)
            #    for row in cvsreader:
            #        print(row)
            
            #with open('vacuum_values.csv', 'w', newline='') as csvfile:
            #    cvswriter = csv.writer(csvfile)
            #    cvswriter.writerows(curs)
            
        conn.commit()
    finally:
        conn.close()

def main(args):
    try:
        conn_str = args.conn_str;
        #sqlqrystr = pkgutil.get_data('res', 'vacuum_file.sql')
        sqlqrystr = pkgutil.get_data('res', 'candidate_tables_templ.sql')
        min_size = args.min_size
        min_age_overdue = args.min_age_overdue
    except Exception as e:
        print(e)
        sys.exit(1)

    chk_autovacuum_global(conn_str)    
    process_res(conn_str, sqlqrystr,min_size, min_age_overdue)

parser = argparse.ArgumentParser(description='A tool for managing your vacuum')
parser.add_argument('conn_str', type=str, help='connection string to the DB')
parser.add_argument('--min-size', type=int, help='the table minsize in bytes (default=0)', default=0)
parser.add_argument('--min-age-overdue', type=int, help='min num of xactions elapsed after vacuum_freeze_table_age__effective, alt. age-vacuum_freeze_table_age__effective>=min-age-overdue (default=0)', default=0)
args = parser.parse_args()
main(args)

