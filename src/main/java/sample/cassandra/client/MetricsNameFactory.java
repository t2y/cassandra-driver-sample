package sample.cassandra.client;

import com.codahale.metrics.jmx.ObjectNameFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricsNameFactory implements ObjectNameFactory {

  private static final String ROOT = "root";
  private static final String NAME = "name";
  private static final String TYPE = ",type";
  private static final String COLON = ":";
  private static final String HYPHEN = "-";
  private static final String EQUAL = "=";

  @Override
  public ObjectName createName(String type, String domain, String name) {
    val typeName = type == null ? ROOT : type;
    val objectName =
        new StringBuilder(domain)
            .append(COLON)
            .append(NAME)
            .append(EQUAL)
            .append(name.replace(COLON, HYPHEN))
            .append(TYPE)
            .append(EQUAL)
            .append(typeName)
            .toString();
    log.debug(objectName);
    try {
      return new ObjectName(objectName);
    } catch (MalformedObjectNameException e) {
      log.error(e.getMessage(), e);
      throw new IllegalStateException(e);
    }
  }
}
