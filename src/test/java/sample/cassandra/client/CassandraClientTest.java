package sample.cassandra.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import sample.cassandra.repository.dao.UserDao;
import sample.cassandra.repository.entity.User;
import sample.cassandra.repository.mapper.UserMapper;
import sample.cassandra.repository.mapper.UserMapperBuilder;

@Slf4j
public class CassandraClientTest {

  private static String KEYSPACE = "client";
  private static CqlSession SESSION;
  private static UserMapper USER_MAPPER;

  @BeforeAll
  static void setup() throws Exception {
    SESSION = CassandraTestHelper.getSession(KEYSPACE, new User());
    log.info(SESSION.toString());
    USER_MAPPER = new UserMapperBuilder(SESSION).build();
  }

  @AfterAll
  static void tearDown() {
    log.info("tearDown");
    CassandraTestHelper.dropTable(SESSION, KEYSPACE, new User());
    SESSION.close();
  }

  @Test
  @SuppressWarnings("resource")
  public void testInsert() throws Exception {
    val client = new CassandraClient(SESSION);
    assertDoesNotThrow(
        () -> {
          client.showReleaseVersion();
        });
  }

  @Test
  public void testInsertByMapper() throws Exception {
    UserDao dao = USER_MAPPER.userDao(CqlIdentifier.fromCql(KEYSPACE));
    val user = new User(UUID.randomUUID(), "user1", 33, true, LocalDate.now());
    dao.save(user);

    val actual = dao.findById(user.getId());
    log.info(actual.toString());

    assertEquals(user, actual);
  }

  @Test
  public void testInsertAsyncByMapper() {
    UserDao dao = USER_MAPPER.userDao(CqlIdentifier.fromCql(KEYSPACE));
    val user = new User(UUID.randomUUID(), "user1", 33, true, LocalDate.now());
    val future = dao.saveAsync(user);
    future.whenComplete(
        (dummy, throwable) -> {
          assertNull(dummy);
          assertNull(throwable);
          log.info("complete");
        });
    future.join();

    Awaitility.await()
        .atMost(3, TimeUnit.SECONDS)
        .pollDelay(100, TimeUnit.MILLISECONDS)
        .until(
            () -> {
              val actual = dao.findById(user.getId());
              log.info(actual.toString());
              assertEquals(user, actual);
              log.info("assert getting the user");
              return true;
            });

    log.info("async insert and assert were completed");
  }

  @Test
  public void testLotsOfInsertAsync() {
    val total = 1000;
    val users = CassandraTestHelper.createUsers(0, total);
    UserDao dao = USER_MAPPER.userDao(CqlIdentifier.fromCql(KEYSPACE));
    IntStream.range(0, total)
        .forEach(
            i -> {
              dao.saveAsync(users.get(i));
            });

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
}
