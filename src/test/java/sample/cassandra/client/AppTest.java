package sample.cassandra.client;

import static org.junit.Assert.assertTrue;

import lombok.val;

import org.junit.Test;

public class AppTest {
  @Test
  public void testGetConfigPath() {
    val path = App.getConfigPath();
    assertTrue(path.endsWith("resources/main/client.properties"));
  }
}
