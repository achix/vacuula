import psycopg2
import pkgutil

def print_use():
    print("Usage:", sys.argv[0], "conn_str [sqlfile] [table_size_limitMB]")

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

        ###print("num of args=" + str(len(args)))
        #sqlqrystr=''
        ###print ("==="+args[1]+"___"+args[2]+"===" )

        if (len(args)==3):
            constr = args[1]
            f = open(args[2], "r")
            sqlqrystr =  f.read()
        elif (len(args)==2):
            constr = args[1]
            sqlqrystr = pkgutil.get_data('res', 'vacuum_file.sql')
            #print("sqlqrystr="+str(sqlqrystr))  
        else:
            print_use()
            sys.exit(2)
    #except Exception as e:
    #    print(e)
    #    sys.exit(1)
    except Exception as e:
        print(e)
        sys.exit(1)
##

    print_res(constr, sqlqrystr)

import sys
main(sys.argv)




