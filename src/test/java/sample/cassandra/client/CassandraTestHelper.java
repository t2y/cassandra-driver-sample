package sample.cassandra.client;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

import lombok.val;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import sample.cassandra.repository.entity.Table;

public class CassandraTestHelper {

  static void createKeyspace(CqlSession session, String name) {
    val createKeyspace = SchemaBuilder.createKeyspace(name).ifNotExists().withSimpleStrategy(1);
    session.execute(createKeyspace.asCql());
  }

  static void createTable(CqlSession session, Table table) {
    session.execute(table.getCreateTable().asCql());
  }

  static void dropTable(CqlSession session, Table table) {
    session.execute(table.getDropTable().asCql());
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
      createTable(session, tables[i]);
    }
    return session;
  }
}
