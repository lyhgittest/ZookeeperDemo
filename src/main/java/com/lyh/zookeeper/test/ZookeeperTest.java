package com.lyh.zookeeper.test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

public class ZookeeperTest {

	private static String connectString = "hadoop102:2192,hadoop103:2192,hadoop104:2192";
	private static int sessionTimeout = 2000;
	private ZooKeeper zkClient = null;

	@Before
	public void init() throws IOException {
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				// 收到时间通知后回调函数
				System.out.println(event.getType() + "--" + event.getPath());
				try {
					// 再次启动监听
					zkClient.getChildren("/", true);
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}
			}

		});
	}

	/**
	 * 此处创建一个客户端连接后都会关闭这个连接，所以如果是创建一个临时的节点的话，在其他地方打开有可能是看不到的
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	@Test
	public void createNode() throws KeeperException, InterruptedException {
		waitUntilConnected(zkClient);
		String path = "/servers";
		String name = zkClient.create(path, "servers".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		System.out.println(name);
	}

	@Test
	public void getNode() throws KeeperException, InterruptedException {
		waitUntilConnected(zkClient);
		List<String> children = zkClient.getChildren("/", true);
		
		for (String child : children) {
			System.out.println(child);
			byte[] data = zkClient.getData("/"+child, false, null);//这是获取节点的值
			System.out.println(new String(data));
		}
		// 延时阻塞
		// Thread.sleep(Long.MAX_VALUE);
	}

	@Test
	public void exists() throws KeeperException, InterruptedException {
		waitUntilConnected(zkClient);
		Stat stat = zkClient.exists("/lyh2/mvc", false);
		System.out.println(stat == null ? "not exists!" : "exists");
	}

	/**
	 * new zookeeper之后，zookeeper的还没有连接好，就去调用，当然会抛错,此方法用于等待让zookeeper连接好
	 * 如果没有连接好久启用zookeeper可能会报异常：org.apache.zookeeper.
	 * KeeperException$ConnectionLossException: KeeperErrorCode = ConnectionLoss
	 * 
	 * @param zooKeeper
	 */
	public static void waitUntilConnected(ZooKeeper zooKeeper) {
		/**
		 * CountDownLatch是一个同步工具类，它允许一个或多个线程一直等待，直到其他线程的操作执行完后再执行
		 * 2、构造函数：CountDownLatch(int
		 * count)//初始化count数目的同步计数器，只有当同步计数器为0，主线程才会向下执行 主要方法：void
		 * await()//当前线程等待计数器为0 boolean await(long timeout, TimeUnit
		 * unit)//与上面的方法不同，它加了一个时间限制。 void countDown()//计数器减1 long
		 * getCount()//获取计数器的值
		 * 
		 * 3.它的内部有一个辅助的内部类：sync.
		 */
		CountDownLatch connectedLatch = new CountDownLatch(1);

		Watcher watcher = new ConnectedWatcher(connectedLatch);
		/**
		 * 为zookeeper的连接指定默认的监听器
		 */
		zooKeeper.register(watcher);
		if (States.CONNECTING == zooKeeper.getState()) {
			try {
				connectedLatch.await();
			} catch (InterruptedException e) {
				throw new IllegalStateException(e);
			}
		}
	}

	static class ConnectedWatcher implements Watcher {

		private CountDownLatch connectedLatch;

		ConnectedWatcher(CountDownLatch connectedLatch) {
			this.connectedLatch = connectedLatch;
		}

		@Override
		public void process(WatchedEvent event) {
			if (event.getState() == KeeperState.SyncConnected) {
				connectedLatch.countDown();
			}
		}
	}

}
