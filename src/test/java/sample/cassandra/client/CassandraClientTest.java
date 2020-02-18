package sample.cassandra.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;

import java.time.LocalDate;
import java.util.UUID;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import sample.cassandra.repository.dao.UserDao;
import sample.cassandra.repository.entity.User;
import sample.cassandra.repository.mapper.UserMapper;
import sample.cassandra.repository.mapper.UserMapperBuilder;

@Slf4j
public class CassandraClientTest {

  private static CqlSession SESSION;

  @BeforeAll
  static void setup() throws Exception {
    SESSION = CassandraTestHelper.getSession("test", new User());
    log.info(SESSION.toString());
  }

  @AfterAll
  static void tearDown() {
    log.info("tearDown");
    CassandraTestHelper.dropTable(SESSION, new User());
    SESSION.close();
  }

  @Test
  public void testInsert() throws Exception {
    val client = new CassandraClient(SESSION);
    assertDoesNotThrow(
        () -> {
          client.showReleaseVersion();
        });
  }

  @Test
  public void testInsertByMapper() throws Exception {
    UserMapper userMapper = new UserMapperBuilder(SESSION).build();
    UserDao dao = userMapper.userDao(CqlIdentifier.fromCql("test"));
    val user = new User(UUID.randomUUID(), "user1", 33, true, LocalDate.now());
    dao.save(user);

    val actual = dao.findById(user.getId());
    log.info(actual.toString());

    assertEquals(user, actual);
  }
}
