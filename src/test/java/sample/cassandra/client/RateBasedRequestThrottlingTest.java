package sample.cassandra.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;

import java.util.concurrent.TimeUnit;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sample.cassandra.repository.entity.User;
import sample.cassandra.repository.mapper.UserMapper;
import sample.cassandra.repository.mapper.UserMapperBuilder;

@Slf4j
public class RateBasedRequestThrottlingTest {

  private static final String CONF_NAME = "rate-based-cassandra-session.conf";

  private static String KEYSPACE = "rate";
  private static CqlSession SESSION;
  private static UserMapper USER_MAPPER;

  @BeforeAll
  static void setup() throws Exception {
    val embeddedSession = CassandraTestHelper.getSession(KEYSPACE, new User());
    val config = CassandraTestHelper.getConfigPath(CONF_NAME);
    log.info("Use config file: {}", config);
    SESSION = CassandraTestHelper.getSession(embeddedSession, config);
    embeddedSession.close();
    USER_MAPPER = new UserMapperBuilder(SESSION).build();
  }

  @AfterAll
  static void tearDown() {
    // Don't requests since RequestThrottlingException might occur
    // CassandraTestHelper.dropTable(SESSION, KEYSPACE, new User());
    SESSION.close();
  }

  @BeforeEach
  void truncate() {
    CassandraTestHelper.truncateTable(SESSION, KEYSPACE, "user");
  }

  @Test
  void testNumberOfConnectionMaxRequests() throws Exception {
    val config = SESSION.getContext().getConfig();
    val profile = config.getDefaultProfile();
    assertEquals(24, profile.getInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS));
  }

  @Test
  public void testInsertWithinRateLimit() {
    val total = 30;
    val dao = USER_MAPPER.userDao(CqlIdentifier.fromCql(KEYSPACE));
    CassandraTestHelper.insertAsyncLotsOfData(total, dao);
    Awaitility.await()
        .atMost(3, TimeUnit.SECONDS)
        .pollDelay(100, TimeUnit.MILLISECONDS)
        .until(
            () -> {
              val findUsersNum = dao.all().all().size();
              log.info("number of users: {}", findUsersNum);
              if (total == findUsersNum) {
                assertEquals(total, findUsersNum);
                return true;
              }
              return false;
            });
  }

  @Test
  public void testOccurLotsOfInsertAsync() {
    val total = 144;
    val dao = USER_MAPPER.userDao(CqlIdentifier.fromCql(KEYSPACE));
    CassandraTestHelper.insertAsyncLotsOfData(total, dao);
    Awaitility.await()
        .atMost(3, TimeUnit.SECONDS)
        .pollDelay(50, TimeUnit.MILLISECONDS)
        .until(
            () -> {
              assertThrows(
                  RequestThrottlingException.class,
                  () -> {
                    val findUsersNum = dao.all().all().size();
                    log.info("number of users: {}", findUsersNum);
                  });
              return true;
            });
  }
}
