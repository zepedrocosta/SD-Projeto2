package utils.kafka;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class Zookeeper implements Watcher {

	private static Logger Log = Logger.getLogger( Zookeeper.class.getName() );
	
	private ZooKeeper _client;
	private final int TIMEOUT = 5000;

	public Zookeeper(String servers) throws Exception {
		this.connect(servers, TIMEOUT);
	}

	public synchronized ZooKeeper client() {
		if (_client == null || !_client.getState().equals(ZooKeeper.States.CONNECTED)) {
			throw new IllegalStateException("ZooKeeper is not connected.");
		}
		return _client;
	}

	public void registerWatcher( Watcher w ) {
		client().register( w );
	}
	
	private void connect(String host, int timeout) throws IOException, InterruptedException {
		var connectedSignal = new CountDownLatch(1);
		_client = new ZooKeeper(host, TIMEOUT, (e) -> {
			if (e.getState().equals(Event.KeeperState.SyncConnected)) {
				connectedSignal.countDown();
			}
		});
		connectedSignal.await();
	}

	public String createNode(String path, byte[] data, CreateMode mode) {
		try {
			return client().create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
		} catch (KeeperException.NodeExistsException x) {
			return path;
		} catch (Exception x) {
			throw new RuntimeException(x);
		}
	}

	public List<String> getChildren(String path) {
		try {
			return client().getChildren(path, false);
		} catch (Exception x) {
			throw new RuntimeException(x);
		}
	}
	
	public List<String> getAndWatchChildren(String path) {
		try {
			return client().getChildren(path, true);
		} catch (Exception x) {
			throw new RuntimeException(x);
		}
	}

	@Override
	public void process(WatchedEvent event) {
		if( event.getType() == EventType.NodeChildrenChanged ) {
			var path = event.getPath();
			Log.info("Got a path changed event:" + path);
			Log.info("Updated children:" + getChildren( path ));
			getAndWatchChildren(path);
		}
	}
	
	public static void main(String[] args) throws Exception {

		String host = args.length == 0 ? "localhost" : args[0];
		
		var zk = new Zookeeper(host);

		zk.registerWatcher( zk );
		
		String root = "/directory";

		var path = zk.createNode(root, new byte[0], CreateMode.PERSISTENT);
		Log.info("Created node:" + path);
				

		var childPath = zk.createNode(root + "/", new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL);
		Log.info("Created child node:" + childPath);

		
		//Monitor path
		var children = zk.getAndWatchChildren(root);
		Log.info("Get children:" + children);

		Thread.sleep(Long.MAX_VALUE);
	}
}
