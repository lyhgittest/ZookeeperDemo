package com.lyh.zookeeper.watchNodeOnOrOffLine;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;

public class WatchNodeOnOrOffLineServer {

	private static String connectString = "hadoop102:2192,hadoop103:2192,hadoop104:2192";
	private static int sessionTimeout = 2000;
	private ZooKeeper zkClient = null;
	private String parentNode="/servers";
	//创建连接到zk的客户端
	public void getConnect() throws IOException{
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				
			}
		});
	}
	//注册服务器
	public void registServer(String hostName) throws KeeperException, InterruptedException{
		waitUntilConnected(zkClient);
		String create = zkClient.create(parentNode+"/server", hostName.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(hostName +"is notline" + create);
	}
	//业务功能
	public void business(String hostName) throws InterruptedException{
		System.out.println(hostName +"is working!");
		Thread.sleep(Long.MAX_VALUE);
	}
	
	public static void waitUntilConnected(ZooKeeper zooKeeper) {
		CountDownLatch connectedLatch = new CountDownLatch(1);
		Watcher watcher = new ConnectedWatcher(connectedLatch);
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
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		//获取zk连接
		WatchNodeOnOrOffLineServer server = new WatchNodeOnOrOffLineServer();
		server.getConnect();
		server.registServer(args[0]);
		//启动业务
		server.business(args[0]);
	}
}
