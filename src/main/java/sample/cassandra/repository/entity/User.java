package sample.cassandra.repository.entity;

import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.SNAKE_CASE_INSENSITIVE;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;

import java.time.LocalDate;
import java.util.UUID;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
@Entity
@NamingStrategy(convention = SNAKE_CASE_INSENSITIVE)
public class User implements Table {

  public User() {}

  public User(UUID id, String name, int age, boolean loggedIn, LocalDate birthDate) {
    this.id = id;
    this.name = name;
    this.age = age;
    this.loggedIn = loggedIn;
    this.birthDate = birthDate;
  }

  @PartitionKey private UUID id;
  @ClusteringColumn private String name;
  private int age;
  private boolean loggedIn;
  private LocalDate birthDate;

  @Override
  public CreateTable getCreateTable(String keyspace) {
    return SchemaBuilder.createTable(keyspace, "user")
        .ifNotExists()
        .withPartitionKey("id", DataTypes.UUID)
        .withClusteringColumn("name", DataTypes.ASCII)
        .withColumn("age", DataTypes.INT)
        .withColumn("logged_in", DataTypes.BOOLEAN)
        .withColumn("birth_date", DataTypes.DATE);
  }

  @Override
  public Drop getDropTable(String keyspace) {
    return SchemaBuilder.dropTable(keyspace, "user").ifExists();
  }
}
