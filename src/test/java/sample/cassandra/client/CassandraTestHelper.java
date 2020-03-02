package sample.cassandra.client;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Optional;

import lombok.val;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import sample.cassandra.repository.entity.Table;

public class CassandraTestHelper {

  static void createKeyspace(CqlSession session, String name) {
    val createKeyspace = SchemaBuilder.createKeyspace(name).ifNotExists().withSimpleStrategy(1);
    session.execute(createKeyspace.asCql());
  }

  static void createTable(CqlSession session, String keyspace, Table table) {
    session.execute(table.getCreateTable(keyspace).asCql());
  }

  static void dropTable(CqlSession session, String keyspace, Table table) {
    session.execute(table.getDropTable(keyspace).asCql());
  }

  public static void truncateTable(CqlSession session, String ks, String table) {
    session.execute(QueryBuilder.truncate(ks, table).asCql());
  }

  static CqlSession getSession() throws Exception {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra();
    return EmbeddedCassandraServerHelper.getSession();
  }

  public static CqlSession getSession(String keyspace, Table... tables) throws Exception {
    val session = getSession();
    createKeyspace(session, keyspace);
    for (int i = 0; i < tables.length; i++) {
      createTable(session, keyspace, tables[i]);
    }
    return session;
  }

  public static Optional<InetSocketAddress> getInetSocketAddress(CqlSession session) {
    val addresses = MiscUtils.getInetSocketAddresses(session);
    if (addresses.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(addresses.get(0));
  }

  private static String _DATACENTER = "datacenter1";

  public static CqlSession getSession(CqlSession session, Path config) {
    val address = getInetSocketAddress(session).orElseThrow();
    val configLoader = DriverConfigLoader.fromFile(config.toFile());
    return CqlSession.builder()
        .addContactPoint(address)
        .withConfigLoader(configLoader)
        .withLocalDatacenter(_DATACENTER)
        .build();
  }

  public static Path getResource(String name) throws URISyntaxException {
    val loader = CassandraTestHelper.class.getClassLoader();
    val url = loader.getResource(name);
    return Path.of(url.toURI());
  }
}
