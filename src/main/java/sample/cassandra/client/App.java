package sample.cassandra.client;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.util.Optional;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class App {

  @VisibleForTesting
  static String getConfigPath() {
    String path;
    val opt = Optional.ofNullable(System.getProperty(Constants.CONFIG));
    if (opt.isPresent()) {
      path = opt.get();
    } else {
      val prop = App.class.getClassLoader().getResource(Constants.DEFULT_PROP);
      if (prop == null) {
        throw new IllegalArgumentException("Use -Dconfig=path/to/client.properties");
      }
      path = prop.getPath();
    }
    log.info("config: " + path);
    return path;
  }

  @VisibleForTesting
  static Config getConfig() {
    val defaultConfig = ConfigFactory.load();
    val externalConfigFile = Optional.ofNullable(getConfigPath()).map(File::new);
    if (externalConfigFile.isPresent() && !externalConfigFile.get().exists()) {
      throw new RuntimeException(
          "external config file " + externalConfigFile.get().getAbsolutePath() + " not found");
    }
    return externalConfigFile
        .map(ConfigFactory::parseFile)
        .map(c -> c.withFallback(defaultConfig))
        .orElse(defaultConfig);
  }

  private static void queryToCassandra() {
    val config = getConfig();
    try (val client = new CassandraClient(config)) {
      client.showReleaseVersion();

      val opt = Optional.ofNullable(System.getProperty(Constants.CQL));
      if (opt.isPresent()) {
        client.queryCql(opt.get());
      }
    }
  }

  public static void main(String[] args) {
    System.out.println("start");
    queryToCassandra();
    System.out.println("end");
  }
}
