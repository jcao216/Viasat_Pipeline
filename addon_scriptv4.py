import pgdb
from pgdb import Connection
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import csv
import avro
import datetime
from ksql import KSQLAPI








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
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(2,0,2))
print("Connecting to Kafka Producer...")
for x in range(100):
	producer.send('foobar', b'message has been received',x)


print("Successfully connected KafkaProducer")

# print("Type a SQL command below. \nWhen you are done typing your command, press ENTER on a blank line or type 'stop'.")
# raw_input_lines = []
# while True:
	# raw_input = input()
	# if (raw_input) and (raw_input != 'stop'):
		# raw_input_lines.append(raw_input)
	# else:
		# break
# cmd = ''
# for x in raw_input_lines:
	# cmd = cmd + '\n' + x
# print(raw_input_lines)
# print(cmd)

insert_table_cmd = (
        """
        CREATE TABLE IF NOT EXISTS updates (
			update varchar(255)
        );
        """)


try:
	conn = pgdb.Connection(database='postgres',host='localhost',user='postgres', password='postgres')
	cur = conn.cursor()
	print('Connection successful!')
	client = KSQLAPI('http://127.0.0.1:59090/browser/')
except:
    print('Connection unsuccessful!')

print("Type a SQL command below. \nWhen you are done typing your command, press ENTER on a blank line or type 'stop'.\nTo completely quite out of this interface, type 'quit all'.")
raw_input_lines = []
outer_loop = True
while (outer_loop):
	print("> ")
	inner_loop = True
	while (inner_loop):
		raw_input = input()
		if (raw_input) and (raw_input != 'stop'):
			raw_input_lines.append(raw_input)
		if (raw_input == 'quit all'):
			print("quit all acknowledged")
			inner_loop = False
			outer_loop = False
			break
		else:
			inner_loop = False
	# if (outer_loop == False):
		# break
	cmd = ''
	for x in raw_input_lines:
		cmd = cmd + '\n' + x
	print(raw_input_lines)
	print(cmd)
	try:
		cur.execute(cmd)
		print("Command execution successful!")
		outer_loop = False
	except:
		if (outer_loop == False):
			print("Command execution unsuccessful! Aborting...")
			break
		else:
			print("Command execution unsuccessful! Please try another command")	





def load_data(filename):
	with open(filename, mode='r') as read_file:
			
		read_data = csv.reader(read_file, delimiter=',')
		line = 0
		line_ct = 0
		for lines in read_data:
			if (line_ct > 300):
				break
			elif line_ct == 0: # exclude header row
				pass

			# print([x.strip(' ') for x in lines])
			clean_line = [x.strip(' ') for x in lines]
			# print(clean_line)
			insert_cmd = """INSERT INTO sample8451Data (bask_no, hshd_no, purchase, prod_num, spend, unit, store, wk, yr) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			update_cmd = """INSERT INTO updates (messages) VALUES ('updated')"""
			try:
				# print("trying command")
				cur.execute(insert_cmd, tuple(tuple(clean_line),))
				# print("command executed")
				for _ in range(100):
					producer.send('Added', b'new value')

				
				cur.execute(update_cmd)
				conn.commit()
			except:
				# print("failed commit. Rolling back!")
				conn.rollback()
			# cur.execute(update_cmd)
			line_ct += 1
	print("{} lines were loaded.".format(line_ct))
	return


def find_data(query,location):
# tracks amount spent at a particular location
	search_cmd = """SELECT {} FROM sample8451Data WHERE store = '{}';""".format(str(query),str(location))
	cur.execute(search_cmd)
	obj = cur.fetchall()
	sum = 0.00
	for x in obj:
		raw_val = list(x)[0].replace('$','')
		if (raw_val[0] == '('):
			raw_val = raw_val.replace('(','').replace(')','')
			# print("{} - {}".format(sum,raw_val))
			sum =round(sum - float(raw_val),2)

		else:
			# print("{} + {}".format(sum,raw_val))
			sum = round(sum + float(raw_val),2)

		# print("Sum is now {}".format(sum))
	return sum
	



filename = "5000_transactions.csv"
table_create = 0
if (table_create):
	load_data(filename)
if (input("Do you want to run the query to find the amount of money spent at certain store region?(y/n) ") == 'y'):
	query = "spend"
	location = "SOUTH"
	topic_list = []
	topic_list.append(NewTopic(name="example_topic", num_partitions=1, replication_factor=1))
	result = find_data(query,location)
	print("The total amount spent at locations in {} was {}".format(location,result))
else:
	print("Query was aborted!")



cur.close()
conn.close()
print("Connection closed.")


