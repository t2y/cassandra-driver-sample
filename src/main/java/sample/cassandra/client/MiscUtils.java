package sample.cassandra.client;

import com.datastax.oss.driver.api.core.CqlSession;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import lombok.val;

public class MiscUtils {

  private MiscUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static Path getAbsolulePath(String filePath) {
    Path absolutePath = Paths.get(filePath).toAbsolutePath();
    if (Files.isReadable(absolutePath)) {
      return absolutePath;
    }
    String message = String.format("%s is not exist/readable", absolutePath);
    throw new IllegalArgumentException(message);
  }

  public static List<InetSocketAddress> getInetSocketAddresses(CqlSession session) {
    val nodes = session.getMetadata().getNodes();
    val addresses = new ArrayList<InetSocketAddress>(nodes.size());
    for (val node : nodes.entrySet()) {
      val address = (InetSocketAddress) node.getValue().getEndPoint().resolve();
      addresses.add(address);
    }
    return addresses;
  }
}
