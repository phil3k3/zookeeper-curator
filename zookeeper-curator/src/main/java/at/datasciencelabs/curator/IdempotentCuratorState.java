package at.datasciencelabs.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;

import java.nio.file.Path;

/**
 * A simple abstraction to avoid having to check if each path element exists on Zookeeper,
 * I noticed that there is the @see {@link org.apache.curator.framework.api.CreateBuilder#creatingParentsIfNeeded()} method,
 * but it only creates the parents and does not protect against existing leaf nodes.
 */
class IdempotentCuratorState {

    private final Path path;
    private final byte[] data;

    IdempotentCuratorState(Path path, byte[] data) {
        this.path = path;
        this.data = data;
    }

    void write(CuratorFramework client) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("");
        for (Path pathItem : path) {
            stringBuilder.append("/");
            stringBuilder.append(pathItem.toString());
            Stat stat = client.checkExists().forPath(stringBuilder.toString());
            if (stat == null) {
                if (("/" + path.toString()).equals(stringBuilder.toString())) {
                    client.create().forPath(stringBuilder.toString(), data);
                } else {
                    client.create().forPath(stringBuilder.toString());
                }
            }
            else {
                if (("/" + path.toString()).equals(stringBuilder.toString())) {
                    client.setData().forPath(stringBuilder.toString(), data);
                }
            }
        }
    }

    String getPath() {
        return "/" + path.toString();
    }
}
