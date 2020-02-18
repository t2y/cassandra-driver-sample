package sample.cassandra.repository.mapper;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;

import sample.cassandra.repository.dao.UserDao;

@Mapper
public interface UserMapper {

  @DaoFactory
  UserDao userDao(@DaoKeyspace CqlIdentifier keyspace);
}
