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
	private ConcurrentHashMap<Integer, BlockingQueue<SignalInfo>> map = new ConcurrentHashMap<>();
	private ConcurrentHashMap<Address, BlockingQueue<SignalInfo>> replicas = new ConcurrentHashMap<>();
	private AtomicInteger replSize = new AtomicInteger(0);
	private AtomicInteger mapSize = new AtomicInteger(0);
	private AtomicInteger nextInLine = new AtomicInteger(0);
	
	private final JChannel channel; 

	private AtomicInteger receivedSignals = new AtomicInteger(0);
	private String cluster = null;
	
	private BlockingQueue<Message> msgQueue = new LinkedBlockingQueue<Message>();
	private final Thread worker;
	
	private final MyMessageDispatcher dispatcher;
//	private AtomicBoolean standalone = new AtomicBoolean(true);
	
	public Node(int nThreads) throws Exception {
		
		THREADS = nThreads;
		channel = new JChannel("jgroups.xml");
		channel.setReceiver(new MyReceiverAdapter(channel, msgQueue));
		
		for(int i = 0; i < THREADS; i++) {
			map.put(i, new LinkedBlockingQueue<SignalInfo>());
		}
		dispatcher = new MyMessageDispatcher(channel);
		this.worker = new Thread(new MyWorker(msgQueue, channel, map, replicas, mapSize, nextInLine, dispatcher, THREADS, replSize));
		worker.start();
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
		
		//TODO REBALANCEARRRRRR
		
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
			System.out.println("channel disconnected");
		}
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
				true);
	}

	@Override
	public void add(Signal signal) throws RemoteException {
		//elegir quien la almacena y enviarsela
		//idem para almacenar la replica
		//DONE
		//TODO handlear el caso en que me mandan un findsimiliarTo antes de que la señal se haya
		//agregado efectivamente (read your writes). Guardate la señal vos hasta que te llegue un ACK.
		//EN MODO DEGRADADO! ;)
		
		
		List<Address> members = channel.getView().getMembers();
		Tuple<Address, Address> tuple = Utils.chooseRandomMember(members);
		Address primaryCopyAddress = tuple.getFirst();
		Address backupAddress = tuple.getSecond();
		
		try {
			send(new MyMessage<Signal>(signal, Operation.ADD, false, backupAddress), primaryCopyAddress);
			send(new MyMessage<Signal>(signal, Operation.ADD, true, primaryCopyAddress), backupAddress);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RemoteException();
		}
	}

	@Override
	public Result findSimilarTo(Signal signal) throws RemoteException {
		
		receivedSignals.incrementAndGet();
				
		if (channel.isConnected()) {
			List<Future<Result>> futureResults = new ArrayList<>();
			List<Result> results = new LinkedList<>();
			for (final Address address : channel.getView().getMembers()) {
	
	//			if (address != channel.getAddress()) {
					futureResults.add(dispatcher.<Result>sendMessage(address, new MyMessage<Signal>(signal, Operation.QUERY)));
	//			}
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
	
	private <T> void send(MyMessage<T> myMessage, Address destAddress) throws Exception {
		channel.send(new Message(destAddress, null, myMessage));
	}
	
}
