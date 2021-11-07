import psycopg2
import pkgutil
import argparse
import sys

def print_res(constr, sqlqry):
    conn = psycopg2.connect(constr)
    conn.set_session(autocommit=False)

    try:
        with conn.cursor() as curs:
            i = 0
            curs.execute(sqlqry)
            for record in curs:
                print(record[0])
                i += 1
            print(i)

        conn.commit()
    finally:
        conn.close()

def main(args):
    try:
        conn_str = args.conn_str;
        if (args.sql_file):
            f = open(args.sql_file, "r")
            sqlqrystr =  f.read()
        else:
            sqlqrystr = pkgutil.get_data('res', 'vacuum_file.sql')
        min_size = args.min_size
    #except Exception as e:
    #    print(e)
    #    sys.exit(1)
    except Exception as e:
        
        print(e)
        sys.exit(1)
##

    print_res(conn_str, sqlqrystr)

parser = argparse.ArgumentParser(description='A tool for managing your vacuum')
parser.add_argument('conn_str', type=str, help='connection string to the DB')
parser.add_argument('--sql-file', type=str, help='give your custom sql file')
parser.add_argument('--min-size', type=int, help='the table minsize in GB (default=1GB)', default=1)
args = parser.parse_args()
main(args)




