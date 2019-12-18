package sample.cassandra.client;

public class Constants {

    public static final String DEFULT_PROP = "client.properties";
    public static final String CONFIG = "config";
    public static final String CQL = "cql";

    public enum CassandraConfig {
        HOST, PORT, USER, PASSWORD, DATA_CENTER, TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD;

        public String getKey() {
            return "CASSANDRA_" + this.name();
        }
    }
}
