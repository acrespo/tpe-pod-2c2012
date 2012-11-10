package ar.edu.itba.pod.legajo50758.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;

import ar.edu.itba.pod.legajo50758.api.NodeStats;
import ar.edu.itba.pod.legajo50758.api.Result;
import ar.edu.itba.pod.legajo50758.api.Result.Item;
import ar.edu.itba.pod.legajo50758.api.SPNode;
import ar.edu.itba.pod.legajo50758.api.Signal;
import ar.edu.itba.pod.legajo50758.api.SignalProcessor;

public class Node implements SignalProcessor, SPNode {

	private final int THREADS;
	private final ConcurrentHashMap<Integer, BlockingQueue<SignalInfo>> map = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<Address, BlockingQueue<SignalInfo>> replicas = new ConcurrentHashMap<>();
	private final AtomicInteger replSize = new AtomicInteger(0);
	private final AtomicInteger mapSize = new AtomicInteger(0);
	private final AtomicInteger nextInLine = new AtomicInteger(0);
	
	private final JChannel channel; 

	private final AtomicInteger receivedSignals = new AtomicInteger(0);
	private String cluster = null;
	
	private final BlockingQueue<Message> msgQueue = new LinkedBlockingQueue<Message>();
	private final MyWorker worker;
	private Thread workerThread;
	private final AtomicBoolean degradedMode = new AtomicBoolean(true);
	
	private final MyMessageDispatcher dispatcher;
	private final MyReceiverAdapter receiver;
	
	public Node(int nThreads) throws Exception {
		
		THREADS = nThreads;
		channel = new JChannel("jgroups.xml");
		
		for(int i = 0; i < THREADS; i++) {
			map.put(i, new LinkedBlockingQueue<SignalInfo>());
		}
		dispatcher = new MyMessageDispatcher(channel);
		worker = new MyWorker(msgQueue, channel, map, replicas, mapSize, nextInLine, dispatcher, THREADS, replSize, degradedMode);
		receiver = new MyReceiverAdapter(channel, msgQueue, worker, workerThread, degradedMode);
		channel.setReceiver(receiver);
	}
	
	@Override
	public void join(String clusterName) throws RemoteException {
		
		if (cluster != null) {
			throw new IllegalStateException("Already in cluster " + cluster);
		}
		
		if (!isEmpty()) {
			throw new IllegalStateException("Can't join a cluster because there are signals already stored");
		}
		
		cluster = clusterName;
		try {
			channel.connect(cluster);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException();
		}
		System.out.println("connecting with cluster:" + cluster);
	}

	private boolean isEmpty() {
		
		for(BlockingQueue<SignalInfo> list : map.values()) {	
			if (!list.isEmpty()) {
				return false;
			}
		}
		return true;
	}

	@Override
	public void exit() throws RemoteException {
				
		clear();
		receivedSignals.set(0);
		cluster = null;
		
		System.out.println("signals cleared!");
		System.out.println("channel.isConnected?: " + channel.isConnected());
		if (channel.isConnected()) {
			try {
//				channel.send(new Message(null, null, "Channel disconnected"));
			} catch (Exception e) {
				e.printStackTrace();
				throw new RemoteException();
			}
			channel.disconnect();
			workerThread.interrupt();
			workerThread = null;
			receiver.resetView();
			System.out.println("channel disconnected");
		}
		
		degradedMode.set(true);
	}

	private void clear() {
		for (BlockingQueue<SignalInfo> list: map.values()) {
			list.clear();
		}		
		for (BlockingQueue<SignalInfo> list: replicas.values()) {
			list.clear();
		} 
		mapSize.set(0);
		replSize.set(0);
	}

	@Override
	public NodeStats getStats() throws RemoteException {
		return new NodeStats(
				cluster == null ? "standalone" : "cluster " + cluster, 
				receivedSignals.longValue(),
				mapSize.longValue(), 
				replSize.get(), 
				degradedMode.get());
	}

	@Override
	public void add(Signal signal) throws RemoteException {
		
		List<Address> members = channel.getView().getMembers();
		Tuple<Address, Address> tuple = Utils.chooseRandomMember(members);
		Address primaryCopyAddress = tuple.getFirst();
		Address backupAddress = tuple.getSecond();
		
		LinkedList<Future<Object>> futures = new LinkedList<>();
		try {
			futures.add(dispatcher.sendMessage(primaryCopyAddress, new MyMessage<Signal>(signal, Operation.ADD, false, backupAddress)));
			futures.add(dispatcher.sendMessage(backupAddress, new MyMessage<Signal>(signal, Operation.ADD, true, primaryCopyAddress)));
//			send(new MyMessage<Signal>(signal, Operation.ADD, false, backupAddress), primaryCopyAddress);
//			send(new MyMessage<Signal>(signal, Operation.ADD, true, primaryCopyAddress), backupAddress);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RemoteException();
		}
				
		for (final Future<Object> future : futures) {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public Result findSimilarTo(Signal signal) throws RemoteException {
		
		receivedSignals.incrementAndGet();
				
		if (channel.isConnected()) {
			List<Future<Result>> futureResults = new ArrayList<>();
			List<Result> results = new LinkedList<>();
			for (final Address address : channel.getView().getMembers()) {
				futureResults.add(dispatcher.<Result>sendMessage(address, new MyMessage<Signal>(signal, Operation.QUERY)));
			}
			
			for (final Future<Result> future : futureResults) {
				try {
					results.add(future.get());
				} catch (InterruptedException | ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
			}
			
			Result result = results.remove(0);
			for (Result res: results) {
				Iterable<Item> items = res.items();
				for (Item item: items) {
					result = result.include(item);
				}
			}
			return result;
		}	
		return null;
	}
}
