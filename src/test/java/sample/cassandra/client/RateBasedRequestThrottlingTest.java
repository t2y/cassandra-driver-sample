package sample.cassandra.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import sample.cassandra.repository.entity.User;
import sample.cassandra.repository.mapper.UserMapper;

@Slf4j
public class RateBasedRequestThrottlingTest {

  private static final String CONF_NAME = "rate-based-cassandra-session.conf";

  private static String KEYSPACE = "rate";
  private static CqlSession SESSION;
  private static UserMapper USER_MAPPER;

  @BeforeAll
  static void setup() throws Exception {
    val embeddedSession = CassandraTestHelper.getSession(KEYSPACE, new User());
    val conf = CassandraTestHelper.getResource(CONF_NAME);
    log.info("Use config file: {}", conf.toString());
    SESSION = CassandraTestHelper.getSession(embeddedSession, conf);
    embeddedSession.close();
  }

  @AfterAll
  static void tearDown() {
    log.info("tearDown");
    CassandraTestHelper.dropTable(SESSION, KEYSPACE, new User());
    SESSION.close();
  }

  @Test
  void testNumberOfConnectionMaxRequests() throws Exception {
    val config = SESSION.getContext().getConfig();
    val profile = config.getDefaultProfile();
    assertEquals(33, profile.getInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS));
  }
}
