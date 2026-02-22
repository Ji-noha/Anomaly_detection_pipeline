# line = '127.0.0.1 - - [09/Feb/2026:12:15:30 +0100] "GET /index.html HTTP/1.1" 200 612'
line= '172.20.0.1 - - [05/Feb/2026:15:54:00 +0000] "GET / HTTP/1.1" 200 615 "-" "curl/8.5.0" "-"'
ip= line.split()[0]
time=line.split('[')[1].split()[0]
method=line.split('"')[1].split()[0]
#endpoint=line.split('"')[1].split()[1]
protocol=line.split('"')[1].split()[1]
real_protocol=line.split('"')[1].split()[2]
status=line.split('"')[2].split()[0]
bytes=line.split('"')[2].split()[1]
demand= line.split('"')[5]



print(ip)
print(time)
print(method)
print(demand)
print(real_protocol)
print(protocol)
print(status)
print(bytes)