package sample.cassandra.repository.dao;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Select;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import sample.cassandra.repository.entity.User;

@Dao
public interface UserDao {

  @Select
  PagingIterable<User> all();

  @Select
  User findById(UUID userId);

  @Insert(ifNotExists = false)
  void save(User user);

  @Insert(ifNotExists = false)
  CompletableFuture<Void> saveAsync(User user);

  @Delete
  void delete(User user);
}
