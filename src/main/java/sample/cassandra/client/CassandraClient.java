package sample.cassandra.client;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.typesafe.config.Config;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CassandraClient {

  private Config config;
  private final CqlSession session;

  private final String SSL = "SSL";
  private final String JKS = "JKS";

  public CassandraClient(CqlSession session) {
    this.session = session;
  }

  public CassandraClient(Config config) {
    this.config = config;
    this.session = this.createSession();
  }

  private SSLContext createSslContext(String tsPath, String tsPasswd) {
    try {
      val context = SSLContext.getInstance(SSL);
      val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      try (InputStream tsf = Files.newInputStream(Paths.get(tsPath))) {
        val ts = KeyStore.getInstance(JKS);
        ts.load(tsf, tsPasswd.toCharArray());
        tmf.init(ts);
      }
      context.init(null, tmf.getTrustManagers(), new SecureRandom());
      return context;
    } catch (Exception e) {
      throw new AssertionError("Unexpected error while creating SSL context", e);
    }
  }

  private CqlSession createSession() {
    val host = config.getString(Constants.CassandraConfig.HOST.getKey());
    val port = config.getInt(Constants.CassandraConfig.PORT.getKey());
    val dc = config.getString(Constants.CassandraConfig.DATA_CENTER.getKey());

    val builder =
        CqlSession.builder()
            .addContactPoint(new InetSocketAddress(host, port))
            .withLocalDatacenter(dc);

    val passwd = config.getString(Constants.CassandraConfig.PASSWORD.getKey());
    if (!passwd.isEmpty()) {
      val user = config.getString(Constants.CassandraConfig.USER.getKey());
      log.info("use credential with " + user);
      builder.withAuthCredentials(user, passwd);
    }

    val tsPath = config.getString(Constants.CassandraConfig.TRUSTSTORE_PATH.getKey());
    if (!tsPath.isEmpty()) {
      val tsPasswd = config.getString(Constants.CassandraConfig.TRUSTSTORE_PASSWORD.getKey());
      val sslContext = this.createSslContext(tsPath, tsPasswd);
      log.info("use ssl context with " + tsPath);
      builder.withSslContext(sslContext);
    }
    return builder.build();
  }

  private ResultSet query(String cql) {
    return session.execute(cql);
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

  public void close() {
    if (this.session != null) {
      this.session.close();
    }
  }
}
