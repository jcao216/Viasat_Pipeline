print("Type a SQL command below. \nWhen you are done typing your command, press ENTER on a blank line or type 'stop'.")
raw_input_lines = []
while True:
	raw_input = input()
	if (raw_input) and (raw_input != 'stop'):
		raw_input_lines.append(raw_input)
	else:
		break
cmd = ''
for x in raw_input_lines:
	cmd = cmd + '\n' + x
print(raw_input_lines)
print(cmd)
