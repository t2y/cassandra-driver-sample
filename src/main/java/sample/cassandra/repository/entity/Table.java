package sample.cassandra.repository.entity;

import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;

public interface Table {
  public CreateTable getCreateTable();

  public Drop getDropTable();
}
