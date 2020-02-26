package sample.cassandra.client;

import java.util.Optional;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class App {

  private static void queryToCassandra() {
    try (val client = new CassandraClient()) {
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
  }

  public static void main(String[] args) {
    System.out.println("start");
    queryToCassandra();
    System.out.println("end");
  }
}
