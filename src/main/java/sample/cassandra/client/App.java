package sample.cassandra.client;

import java.io.File;
import java.util.Optional;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class App {

    private static Config config() {
        val defaultConfig = ConfigFactory.load();

        val prop = App.class.getClassLoader().getResource(Constants.DEFULT_PROP);
        val defaultPath = prop.getPath();
        val externalConfigFile = Optional.ofNullable(defaultPath).map(File::new);
        if (externalConfigFile.isPresent() && !externalConfigFile.get().exists()) {
            throw new RuntimeException(
                    "external config file " + externalConfigFile.get().getAbsolutePath() + " not found");
        }

        return externalConfigFile
                .map(ConfigFactory::parseFile)
                .map(c -> c.withFallback(defaultConfig))
                .orElse(defaultConfig);
    }

    public static void main(String[] args) {
        System.out.println("start");

        val config = config();
        val client = new CassandraClient(config);
        client.showReleaseVersion();

        val opt = Optional.ofNullable(System.getProperty("cql"));
        if (opt.isPresent()) {
            client.queryCql(opt.get());
        }
        System.out.println("end");
    }
}
