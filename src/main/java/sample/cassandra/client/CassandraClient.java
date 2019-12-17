package sample.cassandra.client;

import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.CqlSession;
import com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.val;

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

    public void showReleaseVersion() {
        try (val session = this.createSession()) {
            val rs = session.execute("select release_version from system.local");
            val row = rs.one();
            System.out.println(row.getString("release_version"));
        }
    }

}
