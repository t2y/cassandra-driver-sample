package sample.cassandra.client;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.IntStream;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import sample.cassandra.repository.dao.UserDao;
import sample.cassandra.repository.entity.Table;
import sample.cassandra.repository.entity.User;

@Slf4j
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

  static void truncateTable(CqlSession session, String ks, String table) {
    session.execute(QueryBuilder.truncate(ks, table).asCql());
  }

  static CqlSession getSession() throws Exception {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra();
    return EmbeddedCassandraServerHelper.getSession();
  }

  static CqlSession getSession(String keyspace, Table... tables) throws Exception {
    val session = getSession();
    createKeyspace(session, keyspace);
    for (int i = 0; i < tables.length; i++) {
      createTable(session, keyspace, tables[i]);
    }
    return session;
  }

  static Optional<InetSocketAddress> getInetSocketAddress(CqlSession session) {
    val addresses = MiscUtils.getInetSocketAddresses(session);
    if (addresses.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(addresses.get(0));
  }

  private static String _DATACENTER = "datacenter1";

  static CqlSession getSession(CqlSession session, Path config) {
    val address = getInetSocketAddress(session).orElseThrow();
    val configLoader = DriverConfigLoader.fromFile(config.toFile());
    return CqlSession.builder()
        .addContactPoint(address)
        .withConfigLoader(configLoader)
        .withLocalDatacenter(_DATACENTER)
        .build();
  }

  static Path getConfigPath(String name) throws URISyntaxException {
    val loader = CassandraTestHelper.class.getClassLoader();
    val url = loader.getResource(name);
    return Path.of(url.toURI());
  }

  static List<User> createUsers(int start, int end) {
    val users = new ArrayList<User>(end - start);
    IntStream.range(start, end)
        .forEach(
            i -> {
              val user = new User(UUID.randomUUID(), "user" + i, i, true, LocalDate.now());
              users.add(user);
            });
    return users;
  }

  private static final int PARALLELISM = 2;

  static void insertDataAsynchronously(int start, int end, UserDao dao) {
    val users = CassandraTestHelper.createUsers(start, end);
    for (val user : users) {
      val future = dao.saveAsync(user);
      future.whenComplete(
          (dummy, throwable) -> {
            if (throwable != null) {
              log.info("failed to insert: {}", user.getName());
              log.info(throwable.getMessage());
            }
          });
    }
  }

  static void insertAsyncLotsOfData(int total, UserDao dao) {
    val pool = new ForkJoinPool(PARALLELISM);
    val range = total / PARALLELISM;
    val futures = new ArrayList<ForkJoinTask<?>>(PARALLELISM);
    for (var i = 0; i < total; i += range) {
      val start = i;
      val future =
          pool.submit(
              () -> {
                insertDataAsynchronously(start, start + range, dao);
              });
      futures.add(future);
    }
    pool.shutdown();
    for (val future : futures) {
      future.join();
    }
  }
}
