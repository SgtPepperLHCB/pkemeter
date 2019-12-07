# pkemeter[^1] - Kafka Transaction Producers

Here is a good overview of [Transactions in Apache Kafka](https://www.confluent.io/blog/transactions-apache-kafka/)

When using transactions to achieve exactly-once delivery of messages, a Producer needs to handle
additional failure conditions (exceptions) that arise from atomic writes. Regular maintenance of the
broker cluster (updates that necessitate broker restarts) can exacerbate these conditions and must
be managed by a Producer.

A Producer may appear to `stall` when a broker is restarted and may never recover or exit when
encountering an unhandled exception.

This project demonstrates a code structure that will recover from these exceptions.  There are two
approaches to coding a `KafkaProducer` as demonstrated by the `SimpleTxnProducer` and
`RobustTxnProducer` classes.

The critical difference is what exceptions are handled and what is done for specific exceptions as
can be seen in the `handleExceptions()` function of each class.

The bottom line is, for a transaction Producer, all applicable exceptions must be caught and it may be
necessary to create and initialize a new KafkaProducer to recover.

# Building and running

## Prerequisites
This project is written in `kotlin` and uses `gradle` and `java 11`.  You can install all three with
[Sdkman!](https://sdkman.io/install)

## Build
To build, clone this repository and run from the repo root:

```shell script
./gradlew clean assemble
```

## Docker Kafka cluster
Start the docker-compose kafka cluster which runs zookeeper, 3 brokers, schema-registry, and kafka-manager

```shell script
docker-compose up
```

## Create topic
Create a topic (default is `zombie-txn` in `src/main/resources/reference.conf`):

```shell script
kafka-topics --bootstrap-server localhost:9092 --create --topic zombie-txn --partitions 12 --replication-factor 3 --config min.insync.replicas=2
```

## Run

There are several `main` entry-points that demostrate simple and robust Producers.  It is necessary
to call out the specific entry-point on the command-line.

The entry-points are:
- pkemeter.TxnMain - runs a simple or robust transaction Producer and (optionally) a Consumer that can be
  transaction-ware
- pkemeter.SimpleMain - runs a simple non-transaction Producer

After building, invoke the desired entry-point (`pkemeter.TxnMain` or `pkemeter.SimpleMain` on the
last line) and (optionally) enable JMX metrics (see below for
description of metrics):

```shell script
java \
  -Dconfig.file=./src/main/resources/application-txn.conf \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote=true \
  -Dcom.sun.management.jmxremote.port=1099 \
  -Djava.rmi.server.hostname=127.0.0.1 \
  -cp build/libs/pkemeter-all.jar pkemeter.TxnMain
```

Producers and consumers log to the console (log level controlled in
`src/main/resources/simplelogger.properties`) indicating messages written and received.

```shell script
2019-12-06T13:13:36.091-0800 [INFO] TxnMain - CROSSING.THE.STREAMS
2019-12-06T13:13:36.098-0800 [INFO] RobustTxnProducer - run
2019-12-06T13:13:36.103-0800 [INFO] SimpleTxnConsumer - run, txn=true
2019-12-06T13:13:36.583-0800 [INFO] RobustTxnProducer - p=  7, o=  0, m=   0:Pi5kkL0OIMYuNshG, n=    0
2019-12-06T13:13:38.139-0800 [INFO] RobustTxnProducer - p=  7, o=  2, m=   1:zXypjBRlroUEVbmU, n=    1
2019-12-06T13:13:40.144-0800 [INFO] RobustTxnProducer - p=  9, o=  0, m=   2:sIDoFYGQKUCeZpHP, n=    2
2019-12-06T13:13:40.156-0800 [INFO] SimpleTxnConsumer - p=  9, o=  0, m=   2:sIDoFYGQKUCeZpHP, n=    1
2019-12-06T13:13:42.157-0800 [INFO] RobustTxnProducer - p= 11, o=  0, m=   3:YfZMTS6NPogKanMn, n=    3
2019-12-06T13:13:42.176-0800 [INFO] SimpleTxnConsumer - p= 11, o=  0, m=   3:YfZMTS6NPogKanMn, n=    2
```

## Simulate broker maintanence

Broker updates are usually done using rolling updates.  You can simulate this by gracefully stopping
a broker and monitoring the logs for failures and recovery.

> You can use the scripts `stop1`, `stop2`, `stop3` and `start1`, `start2`, and `start3` to stop and
> start the brokers.  They can be found in `./scripts`.

### RobustTxnProducer

The `RobustTxnProducer` captures the documented exceptions and recovers from `severe conditions` by
creating and initializing a new KafkaProducer.

Stop a broker:
```shell script
docker-compose exec broker1 kafka-server-stop
```

You will see `schema-registry` go nuts in the `docker-compose` terminal logs.

You will see the Producer log warning messages indicating that it cannot connect to broker1

```shell script
2019-12-06T13:03:13.181-0800 [WARN] NetworkClient - [Producer clientId=producer-109, transactionalId=pkemeter-txn-tid] Connection to node -1 (kafka/127.0.0.1:9091) could not be established. Broker may not be available.
```

Stop another broker to simulate a `severe condition`:
```shell script
docker-compose exec broker2 kafka-server-stop
```

Wait a while, then restart the brokers:
```shell script
docker-compose start broker1
# wait for a bit so the broker can re-join the cluster
docker-compose start broker2
```

The `RobustTxnProducer` will recover and begin producing messages again which you can see log
output.

### SimpleTxnProducer

The `SimpleTxnProducer` does not account for the failure conditions unique to transactions and does
not attempt to recover as recommended. See
[KafkaProducer](https://kafka.apache.org/23/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html)

Set `application.producer.simple_txn=true` in `src/main/resource/application-txn.conf` and
restart the application.

```shell script
2019-12-06T13:21:56.515-0800 [INFO] TxnMain - CROSSING.THE.STREAMS
2019-12-06T13:21:56.520-0800 [INFO] SimpleTxnProducer - run
2019-12-06T13:21:56.526-0800 [INFO] SimpleTxnConsumer - run, txn=true
2019-12-06T13:21:56.944-0800 [INFO] SimpleTxnProducer - p=  1, o= 44, m=   0:9JhCrK6KmsmOEPy7, n=    0
2019-12-06T13:21:58.903-0800 [INFO] SimpleTxnProducer - p= 10, o= 36, m=   1:5cClzOudGsCcZ9Uo, n=    1
2019-12-06T13:21:59.838-0800 [INFO] SimpleTxnConsumer - p=  1, o= 44, m=   0:9JhCrK6KmsmOEPy7, n=    1
2019-12-06T13:21:59.839-0800 [INFO] SimpleTxnConsumer - p= 10, o= 36, m=   1:5cClzOudGsCcZ9Uo, n=    2
2019-12-06T13:22:00.906-0800 [INFO] SimpleTxnProducer - p=  3, o= 36, m=   2:ol8ieWeDsJtZR7g8, n=    2
2019-12-06T13:22:00.909-0800 [INFO] SimpleTxnConsumer - p=  3, o= 36, m=   2:ol8ieWeDsJtZR7g8, n=    3
```
Now stop two brokers, simulating the same `severe condition`:

```shell script
docker-compose exec broker1 kafka-server-stop
# wait a bit...
docker-compose exec broker2 kafka-server-stop
```

The producer will `stall` (more or less `forever`) and you may see `NetworkClient` connection
warning log messages as well.

Restart the brokers as before:
```shell script
docker-compose start broker1
# wait...
docker-compose start broker2
```

Most likely, you'll see a log message from `SimpleTxnProducer` indicating it encountered an error
while trying to complete an in-flight transaction:
```shell script
2019-12-06T13:48:02.254-0800 [ERROR] SimpleTxnProducer - RUN-EX: java.util.concurrent.ExecutionException: org.apache.kafka.common.errors.TimeoutException: Expiring 1 record(s) for zombie-txn-10:36988 ms has passed since batch creation, cause=org.apache.kafka.common.errors
.TimeoutException: Expiring 1 record(s) for zombie-txn-10:36988 ms has passed since batch creation
```

Since the simple approach does not account for this and other exceptions, the program exits :(

## JMX Metrics

Along with the `kafka.producer` and `kafka.consumer` metrics provided by the Kafka client, the
application uses JMX counters to keep track of Producer attempts, errors, successes and attribute
specific exceptions to counters.

These can be found as the `pkemeter-robust-txn-producer` or `pkemeter-simple-txn-producer` MBean
which can be viewed using `jconsole` or `visualvm`.


## Application configuration

Configuration uses the [Typesafe Config](https://github.com/lightbend/config) library and first
reads default values from `src/main/resources/reference.conf` then overrides the values with those
found if `application.conf` OR, as shown above in the first line, you specify
`-Dconfig.file=./src/main/resources/application-txn.conf`.

To choose between `RobustTxnProducer` and `SimpleTxnProducer`, change `application.producer.simple_txn`;
`true` means use `SimpleTxnProducer`.

You can also manage other producer and consumer parameters in the `kafka.producer`/`kafka.consumer`
(non-transaction) and `kafka.txn-producer`/`kafka.txn-consumer` sections.

---

[^1]: pkemeter - Psychokinetic Energy Meter
