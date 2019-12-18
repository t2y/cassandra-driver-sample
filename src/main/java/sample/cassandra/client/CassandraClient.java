package sample.cassandra.client;

import java.net.InetSocketAddress;
import java.util.ArrayList;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class CassandraClient {

    private final Config config;

    private CqlSession createSession() {
        val host = config.getString(Constants.CASSANDA_HOST);
        val port = config.getInt(Constants.CASSANDRA_PORT);
        val dc = config.getString(Constants.CASSANDRA_DATA_CENTER);
        val user = config.getString(Constants.CASSANDRA_USER);
        val passwd = config.getString(Constants.CASSANDRA_PASSWORD);

        val builder = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter(dc);

        if (!passwd.isEmpty()) {
            builder.withAuthCredentials(user, passwd);
        }

        return builder.build();
    }

    private ResultSet query(String cql) {
        try (val session = this.createSession()) {
            return session.execute(cql);
        }
    }

    public void showReleaseVersion() {
        val rs = this.query("select cluster_name, release_version from system.local");
        val row = rs.one();
        log.info(String.format("cluster_name: %s, release_version: %s",
                row.getString("cluster_name"),
                row.getString("release_version")));
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
}
