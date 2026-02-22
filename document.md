================================
# DATABASE CREATION
## docker-compose
create postgres service in docker compose yml with my own info generate them pswd , username db name 
- docker-compose up
make sure its working
docker ps
docker exec -it postgres psql  -U username -d dbname
if we see dbname=# ...
donc db is created
if we type \du
we will see role name (name od user if its superuer ...)
===
if the port 5432 is already taken
use 5433 for example 
 - "5433:5432" 
** Host (my PC) → 5433, Container (Postgres) → 5432
## vs code
creates a connection configuration, not the database itself.
VS Code (or DBeaver, DataGrip, etc.) can: ❌ NOT create a PostgreSQL database automatically
✅ ONLY connect to an existing database
===
VS Code connects to the HOST, not Docker
VS Code → localhost → mapped port → container 5432

IN VS CODE :
add username,password , db name as in dockercompose 
IN PORT add 5433 (the port of the host my pc {SOO IMPORTANT }either it will say user does not exist)
==== 
NOW DB IS CONNECTED
#Inside Docker → use postgres:5432
#From your PC → use localhost:5433
## kafka connect: will transfer data from kafka to postgresql
it will just write to an existent db we already create by docker compose 
===
config.properties file is NOT read by Kafka directly
It is sent via HTTP to Kafka Connect

# info
#topic is like a channel of messages
#schema is the structure of the data either json or other format
