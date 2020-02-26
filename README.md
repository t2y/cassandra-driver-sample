# cassandra-driver-sample

sample code to use cassandra driver

## How to run

modify your cassandra cluster settings. see also [Configuration](https://docs.datastax.com/en/developer/java-driver/4.3/manual/core/configuration/).

```bash
$ vi src/main/resources/application.conf
```

gradle's run task uses above `client.properties` and connect to cassandra cluster.

```bash
$ ./gradlew run
...
> Task :run
start
INFO  config: /Users/t2y/work/repo/cassandra-driver-sample/build/resources/main/client.properties
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
