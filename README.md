# cassandra-driver-sample

sample code to use cassandra driver

## How to run

modify your cassandra cluster settings. see also [Configuration](https://docs.datastax.com/en/developer/java-driver/4.3/manual/core/configuration/).

```bash
$ vi src/main/resources/application.conf
```

gradle's run task uses above `application.conf` and connect to cassandra cluster.

### query cql

```bash
$ ./gradlew run
...
> Task :run
start
INFO  Use CqlSession config: /home/t2y/work/repo/cassandra-driver-sample/src/main/resources/application.conf
INFO  DataStax Java driver for Apache Cassandra(R) (com.datastax.oss:java-driver-core) version 4.3.1
INFO  Using native clock for microsecond precision
INFO  cluster_name: test, release_version: 3.11.2
end
```

you can see cluster name and release version!

another example is to query any cq using system property.

```bash
$ ./gradlew run -Dcql="select * from keyspace.table"
...
INFO  select * from keyspace.table
INFO  Using native clock for microsecond precision
INFO  row: xxx, yyy, zzz
...
```

to query asynchronously.

```bash
$ ./gradlew run -Dcql="select * from keyspace.table" -Dasync=true
...
INFO  async: select * from keyspace.table
...
```

### provide metrics via jmx

```bash
$ ./gradlew run --args='jmx'

> Task :run
start
...
DEBUG com.datastax.oss.driver:name=s0.nodes.127_0_0_2-9042.bytes-received,type=meters
DEBUG com.datastax.oss.driver:name=s0.nodes.127_0_0_3-9042.pool.in-flight,type=gauges
DEBUG com.datastax.oss.driver:name=s0.throttling.errors,type=counters
DEBUG com.datastax.oss.driver:name=s0.nodes.127_0_0_2-9042.pool.in-flight,type=gauges
DEBUG com.datastax.oss.driver:name=s0.nodes.127_0_0_1-9042.pool.open-connections,type=gauges
DEBUG com.datastax.oss.driver:name=s0.connected-nodes,type=gauges
DEBUG com.datastax.oss.driver:name=s0.nodes.127_0_0_1-9042.bytes-sent,type=meters
DEBUG com.datastax.oss.driver:name=s0.throttling.queue-size,type=gauges
DEBUG com.datastax.oss.driver:name=s0.cql-requests,type=timers
DEBUG com.datastax.oss.driver:name=s0.nodes.127_0_0_1-9042.pool.in-flight,type=gauges
DEBUG com.datastax.oss.driver:name=s0.nodes.127_0_0_2-9042.bytes-sent,type=meters
DEBUG com.datastax.oss.driver:name=s0.throttling.delay,type=timers
DEBUG com.datastax.oss.driver:name=s0.nodes.127_0_0_3-9042.pool.open-connections,type=gauges
DEBUG com.datastax.oss.driver:name=s0.nodes.127_0_0_2-9042.pool.open-connections,type=gauges
DEBUG com.datastax.oss.driver:name=s0.nodes.127_0_0_3-9042.bytes-received,type=meters
DEBUG com.datastax.oss.driver:name=s0.nodes.127_0_0_3-9042.bytes-sent,type=meters
DEBUG com.datastax.oss.driver:name=s0.nodes.127_0_0_1-9042.bytes-received,type=meters
INFO  start JMX Reporter for Cassandra driver's metrics
```

attach a local process with jconsole

```bash
$ jconsole
```
