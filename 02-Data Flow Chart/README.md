# 2. Data Flow Chart
## step 1: Mysql Source Kafka
- create source connector
```sh
$ curl -i -X POST  -H "Accept: application/json" -H "Content-Type: application/json" http://localhost:8083/connectors/ -d @mysql-source.json
```

<img src="image\mysql-source-kafka.png" width="100%" height="40%">

<img src="image\connectors mysql-source-kafka.png" width="100%" height="40%">

- after the source connector is up and running, three new topics will be automatically created in apache kafka.

<img src="image\new 3 topic.png" width="100%" height="40%">

- kafka-class-db-001 is the topic that stores the database metadata, and the database itself is also named kafka-class-db-001

<img src="image\offset0 kafka-class-db-001.png" width="100%" height="40%">

<img src="image\value kafka-class-db-001.png" width="100%" height="40%">

- movies is the topic that stores the metadata of the table, and the table itself is named movies

<img src="image\offset0 movies.png" width="100%" height="40%">

<img src="image\value movies.png" width="100%" height="40%">

- kafka-class-db-001.demo.movie is the actual topic where the streaming data is published to and stored in apache kafka

<img src="image\offset0 kafka-class-db-001demomovies.png" width="100%" height="40%">

- in apache kafka, one offset represents one record of data in the table

<img src="image\value kafka-class-db-001demomovies.png" width="100%" height="40%">

---

## step 2: Mysql Sink Kafka
- create sink connector
```sh
$ curl -i -X POST -H "Accept: application/json" -H "Content-Type: application/json" http://localhost:8083/connectors/ -d @mysql-sink-kafka.json
```
<img src="image\mysql-sink-kafka.png" width="100%" height="40%">

- result in DBeaver

<img src="image\result in DBeaver.png" width="100%" height="40%">

- open the sql editor in DBeaver for the demo database

<img src="image\open SQL editor.png" width="100%" height="40%">

- Let’s verify that the tables are properly synchronized. If the value in demo is changed, demo2 should be updated accordingly

<img src="image\update The Master Edited.png" width="100%" height="40%">
<img src="image\select to check.png" width="100%" height="40%">
<img src="image\result in demo2 when change.png" width="100%" height="40%">

---

## step 3: Ksqldb
- we need to access the ksqlDB docker container first before proceeding
```sh
$ docker exce -it ksqldb http://localhost:8088
```

<img src="image\ksqlDB docker.png" width="100%" height="40%">

- show topics
```sh
$ show topics;
```

<img src="image\show topics.png" width="80%" height="40%">

- show connectors
```sh
$ show connectors;
```

<img src="image\show connectors.png" width="100%" height="40%">

### step 3.1: Ksqlststream
- create ksqlststream
```sh
$ CREATE STREAM ksqlstream WITH
(KAFKA_TOPIC='kafka-class-db-001.demo.movies', VALUE_FORMAT = 'AVRO');
```

<img src="image\create ksqlststream.png" width="100%" height="40%">

- show streams
```sh
$ show streams;
```

<img src="image\show streams.png" width="100%" height="40%">

- set offset ksql
```sh
$  SET 'auto.offset.reset'='earliest';
```

<img src="image\set offset ksql.png" width="100%" height="40%">

- SELECT query to view the data
```sh
$  SELECT * FROM ksqlstream EMIT CHANGES LIMIT 10;
```

<img src="image\SELECT query.png" width="100%" height="40%">

- create stream trasform data ksql
```sh
$  CREATE STREAM ksqlstream_processed AS 
SELECT 
after->movie_id AS movie_id,
after->title AS title,
2023 - CAST(after->release_year AS INT) AS num_year_release
FROM ksqlstream
WHERE op in ('c','u','r');
```

<img src="image\trasform data.png" width="75%" height="40%">

- show streams
```sh
$ show streams;
```

<img src="image\ksqlstream_processed.png" width="100%" height="40%">

- SELECT query to view the data
```sh
$  SELECT * FROM ksqlstream_processed EMIT CHANGES LIMIT 10;
```

<img src="image\SELECT query to view the data.png" width="100%" height="40%">

- When we create or query a data stream in ksqlDB, a new Kafka topic is generated. This topic stores the transformed data produced by the stream processing logic
```sh
$  SELECT * FROM ksqlstream_processed EMIT CHANGES LIMIT 10;
```

<img src="image\topic ksqlstream_processed.png" width="100%" height="40%">

<img src="image\topic ksqlstream_processed in confluent.png" width="100%" height="40%">

- exiting ksqlDB
```sh
$  exit;
```

<img src="image\exit.png" width="20%" height="40%">

### step 3.2: Mysql Sink Ksql
- create sink connector
```sh
$ curl -X GET  -H "Accept: application/json" -H "Content-Type: application/json" http://localhost:8083/connectors/ -d @mysql-sink-ksqldb.json
```
<img src="image\mysql-sink-ksqldb.png" width="100%" height="40%">
<img src="image\connectors mysql-sink-ksqldb.png" width="100%" height="40%">

- result in DBeaver

<img src="image\movies_title in DBeaver.png" width="100%" height="40%">

- Let’s verify that the tables are properly synchronized. If the value in demo is changed, demo2 should be updated accordingly

<img src="image\update Tarzan.png.png" width="100%" height="40%">
<img src="image\select to check Tarzan.png" width="100%" height="40%">
<img src="image\result in demo2 movies when change.png" width="100%" height="40%">
<img src="image\result in demo2 movies_title when change.png" width="100%" height="40%">

---