1. To get started with this example, first download the project with all the additional files
2. Install the necessary docker containers. Go to working directory and run 
```docker-compose up -d``` command
3. Go to Cassandra container bash. 
```docker exec -it cassandra-container-name-or-id bash```
4. Go to the CQL shell. 
```cqlsh```
5. Create keyspace and use it. 
```CREATE KEYSPACE bike WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}; use bike;```
6. Create table
```
CREATE TABLE trip(
ride_id text primary key,
rideable_type text,
started_time timestamp,
ended_time timestamp,
start_lat double,
start_ing double,
end_lat double,
end_ing double,
member_casual text);
```
7. Run ProducerKafka. Make sure the .csv file was downloaded.
8. Run StreamHandler.
9. Check the table. 
```SELECT * FROM trip;```
