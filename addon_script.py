import psycopg2
import csv

def create_table():
    create_table_cmd = (
        """
        CREATE TABLE IF NOT EXISTS sample8451Data (
            bask_no varchar(255), 
            hshd_no varchar(255), 
            purchase varchar(255), 
            prod_num varchar(255),
            spend money, 
            unit int, 
            store varchar(255), 
            wk varchar(255), 
            yr int
        );
        """)

    conn = None
    try:
        conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", password="postgres") # obviously change this line
        cur = conn.cursor()
        print("Connection made!")
        cur.execute(create_table_cmd)
        return cur,conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
def add_data(filename, cur,conn):
    ## open file and read data and commit in to database row by row
    with open(filename, mode='r') as read_file:
        
        read_data = csv.reader(read_file, delimiter=',')
        line = 0
        line_ct = 0
        for lines in read_data:
            if (line_ct > 15):
                break
            elif line_ct == 0: # exclude header row
                pass

            # print([x.strip(' ') for x in lines])
            clean_line = [x.strip(' ') for x in lines]
            # print(clean_line)
            insert_cmd = """INSERT INTO sample8451Data (bask_no, hshd_no, purchase, prod_num, spend, unit, store, wk, yr) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            try:
                print("trying command")
                cur.execute(insert_cmd, tuple(tuple(clean_line),))
                print("command executed")
                # cur.execute(insert_cmd, tuple(clean_line))
                conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
                print("failed commit. Rolling back!")
                conn.rollback()
            line_ct += 1
    cur.close()
    conn.close()
    print("Connection closed.")
    # OPEN CONNECTION AND COMMIT ROWS TO TABLE     

    return True




cursor,connection = create_table()
print("printed table")
add_data("5000_transactions.csv", cursor,connection)

search_data(query)

def search_data(q):
