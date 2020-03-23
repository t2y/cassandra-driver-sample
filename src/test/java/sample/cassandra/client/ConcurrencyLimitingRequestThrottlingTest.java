package sample.cassandra.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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
public class ConcurrencyLimitingRequestThrottlingTest {

  private static final String CONF_NAME = "concurrency-limiting-cassandra-session.conf";

  private static String KEYSPACE = "concurrency";
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
    // Don't requests since RequestThrottlingException might occurre
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
  public void testInsertWithinConcurrency() {
    val total = 36;
    val dao = USER_MAPPER.userDao(CqlIdentifier.fromCql(KEYSPACE));
    // expects no errors occurred
    CassandraTestHelper.insertAsyncLotsOfData(total, dao);
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
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
  public void testOccurLotsOfInsertAsync() throws InterruptedException {
    val total = 360;
    val dao = USER_MAPPER.userDao(CqlIdentifier.fromCql(KEYSPACE));
    // expects some errors occurred
    val users = CassandraTestHelper.createUsers(0, total);
    val result = new int[] {0, 0};
    IntStream.range(0, total)
        .forEach(
            i -> {
              val future = dao.saveAsync(users.get(i));
              future.whenComplete(
                  (dummy, throwable) -> {
                    if (throwable != null) {
                      var cause = throwable.getCause();
                      do {
                        if (cause instanceof RequestThrottlingException) {
                          result[0] = 1;
                        }
                        if (cause instanceof AllNodesFailedException) {
                          result[1] = 1;
                        }
                      } while ((cause = cause.getCause()) != null);
                      log.info("failed to insert: {}", i);
                      log.info(throwable.getMessage());
                    }
                  });
            });
    log.info("finished to insert");
    Thread.sleep(3000);
    assertTrue(result[0] == 1 && result[1] == 1);
  }
}
