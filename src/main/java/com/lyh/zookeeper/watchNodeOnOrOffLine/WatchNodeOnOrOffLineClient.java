package com.lyh.zookeeper.watchNodeOnOrOffLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class WatchNodeOnOrOffLineClient {

	private static String connectString = "hadoop102:2192,hadoop103:2192,hadoop104:2192";
	private static int sessionTimeout = 2000;
	private ZooKeeper zkClient = null;
	private String parentNode="/servers";
	private volatile List<String> serverList = new ArrayList<>();
	
	//创建连接到zk的客户端
		public void getConnect() throws IOException{
			zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					//启动监听
					
				}
			});
		}
	
		public void getServerList() throws KeeperException, InterruptedException{
			//获取服务器子节点信息，并且对父节点进行监听
			WatchNodeOnOrOffLineServer.waitUntilConnected(zkClient);
			List<String> children = zkClient.getChildren(parentNode, true);
			List<String> servers = new ArrayList<>();
			
			for (String child : children) {
				byte[] data = zkClient.getData(parentNode + "/" +child, false, null);
				servers.add(new String(data));
			}
			//把server赋值给成员serverList，已提供给各业务线程使用
			serverList = servers;
			System.out.println(serverList);
			
		}
		//业务功能
		public void bussiness() throws InterruptedException{
			System.out.println("client is working....");
			Thread.sleep(Long.MAX_VALUE);
		}
		public static void main(String[] args) throws Exception {
			//获取zk连接
			WatchNodeOnOrOffLineClient client = new WatchNodeOnOrOffLineClient();
			client.getConnect();
			
			//获取servers的子节点信息，从中获取服务器信息列表
			client.getServerList();
			
			//业务进程启动
			client.bussiness();
		}
}
