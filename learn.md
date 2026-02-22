# questions 
🔑 The 5 questions

What problem does this tool solve?

What goes IN?

What comes OUT?

What happens if it breaks?

Why this tool and not another?

If you can answer these → you understand it.
# kafka_ consumer
Kafka tracks which messages each consumer has already read using offsets.

First run → consumer reads messages from the beginning → prints all logs.

Second run → consumer checks offsets → all messages already “consumed” → no new messages, so it prints only:

Consumer is running and subscribed to nginx-logs topic
This is normal Kafka behavior.
# TL;DR

Kafka doesn’t resend messages by default → offsets track what’s already read.

Consumer prints nothing new because no new messages arrived.

Fix by:

Changing consumer group.id, or

Producing new messages, or

Deleting topic (testing only).
# understand code
re.compile() just prepares a regex pattern so you can use it many times without rewriting it.
Step by step:

re.compile(...) → prepares the regex (cookie cutter)

log_pattern.search(line) → applies the regex to one log line

match.groupdict() → gives a dictionary of named fields: ip, time, method, endpoint, status
=== 
(?P<name>pattern)
means:

“Capture this part of the text and give it a name called name.”

pattern = what you want to match

name = how you will refer to it in Python
# Regex
The 10 you must know:
Symbol	Meaning
\S+	non-space characters
\d	digit
\d{3}	exactly 3 digits
.	any character
.*	anything
[]	literal brackets
()	capture
(?P<name>)	named capture
\[ \]	escaped brackets
+	one or more

# worked but not in general or automatic 
from confluent_kafka import Producer
import re
import json

producer_config ={
    'bootstrap.servers':'localhost:9092'
}

producer = Producer(producer_config)

dict = []

with open("nginx/logs/access.log", "r") as f:
    for line in f.readlines():
        ip = re.search(r'(\d{3}.\d{2}.\d{1}.\d{1})',line)
        timestamp = re.search(r'(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})' ,line)
        method = re.search(r'"(\w{3})' ,line)
        status= re.search(r'" (\d{3})' ,line)

        dict.append({
            "ip": ip.group(1) if ip else None ,
            "timestamp": timestamp.group(1) if timestamp else None ,
            "method": method.group(1) if method else None ,
            "status": status.group(1) if status else None

        })

print(dict)
value= json.dumps(dict).encode("utf-8")
producer.produce(
    topic= "dict",
    value= value
)
# from text to json 
import json
y={}
with open (r"C:\Users\user\mini-projet\nginx\logs\access.log", "r") as f:
    for line in f:
        key, value = line.strip().split(' ')
        y[key] = (value)
print(y)

