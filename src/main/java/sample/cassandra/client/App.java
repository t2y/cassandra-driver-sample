package sample.cassandra.client;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.datastax.oss.driver.api.core.CqlSession;

import java.util.Optional;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class App {

  private static void queryToCassandra(CassandraClient client) {
    client.showReleaseVersion();
    val opt = Optional.ofNullable(System.getProperty(Constants.CQL));
    if (opt.isPresent()) {
      val async = System.getProperty(Constants.ASYNC);
      if (async == null) {
        client.queryCql(opt.get());
      } else {
        client.queryCqlAsync(opt.get());
      }
    }
  }

  private static final String CASSANDRA_REGISTRY_DOMAIN = "com.datastax.oss.driver";

  private static void startJmxReporter(CassandraClient client) {
    CqlSession session = client.getSession();
    MetricRegistry registry =
        session
            .getMetrics()
            .orElseThrow(() -> new IllegalStateException("Metrics are disabled"))
            .getRegistry();

    JmxReporter reporter =
        JmxReporter.forRegistry(registry).inDomain(CASSANDRA_REGISTRY_DOMAIN).build();
    reporter.start();
    log.info("start JMX Reporter for Cassandra driver's metrics");
  }

  public static void main(String[] args) throws InterruptedException {
    System.out.println("start");
    try (val client = new CassandraClient()) {
      queryToCassandra(client);
      if (args.length > 0 && args[0].equals("jmx")) {
        startJmxReporter(client);
        while (true) {
          Thread.sleep(10000);
        }
      }
    }
    System.out.println("end");
  }
}
