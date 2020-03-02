package sample.cassandra.client;

public enum Property {
  CONFIG_PATH;

  public String get() {
    return this.name().toLowerCase().replace("_", ".");
  }
}
