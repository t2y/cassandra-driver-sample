package sample.cassandra.client;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CassandraClient implements AutoCloseable {

  private static final String DEFAULT_CONF_PATH = "src/main/resources/application.conf";

  @Getter private final CqlSession session;

  public CassandraClient(CqlSession session) {
    this.session = session;
  }

  public CassandraClient() {
    this.session = this.createSession();
  }

  private Path getAbsolulePath(String filePath) {
    val absolutePath = Paths.get(filePath).toAbsolutePath();
    if (Files.isReadable(absolutePath)) {
      return absolutePath;
    }
    val message = String.format("%s is not exist/readable", absolutePath);
    throw new IllegalArgumentException(message);
  }

  private CqlSession createSession() {
    val opt = Optional.ofNullable(System.getProperty(Constants.CONF));
    val confPath = this.getAbsolulePath(opt.orElse(DEFAULT_CONF_PATH));
    log.info("Use CqlSession config: {}", confPath.toString());
    return CqlSession.builder()
        .withConfigLoader(DriverConfigLoader.fromFile(confPath.toFile()))
        .build();
  }

  private ResultSet query(String cql) {
    return session.execute(cql);
  }

  private CompletionStage<AsyncResultSet> queryAsync(String cql) {
    return session.executeAsync(cql);
  }

  public void showReleaseVersion() {
    val rs = this.query("select cluster_name, release_version from system.local");
    val row = rs.one();
    log.info(
        String.format(
            "cluster_name: %s, release_version: %s",
            row.getString("cluster_name"), row.getString("release_version")));
  }

  public void queryCql(String cql) {
    log.info(cql);
    val rs = this.query(cql);
    for (val row : rs) {
      val values = new ArrayList<String>();
      for (val column : row.getColumnDefinitions()) {
        values.add(row.getObject(column.getName()).toString());
      }
      log.info(String.format("row: %s", String.join(", ", values)));
    }
  }

  public void queryCqlAsync(String cql) {
    log.info("async: {}", cql);
    val cs = this.queryAsync(cql);
    cs.whenComplete(
        (resultSet, throwable) -> {
          do {
            for (val row : resultSet.currentPage()) {
              val values = new ArrayList<String>();
              for (val column : row.getColumnDefinitions()) {
                values.add(row.getObject(column.getName()).toString());
              }
              log.info(String.format("row: %s", String.join(", ", values)));
            }
          } while (resultSet.hasMorePages());
        });
  }

  public void close() {
    log.debug("close was called");
    if (this.session != null) {
      this.session.close();
    }
  }
}
