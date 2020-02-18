package sample.cassandra.repository.dao;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Select;

import java.util.UUID;

import sample.cassandra.repository.entity.User;

@Dao
public interface UserDao {

  @Select
  User findById(UUID userId);

  @Insert
  void save(User user);

  @Delete
  void delete(User user);
}
