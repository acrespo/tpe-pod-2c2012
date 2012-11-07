package ar.edu.itba.pod.legajo50758.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.util.NotifyingFuture;

import ar.edu.itba.pod.legajo50758.api.Signal;

public class NewNodeTask implements Runnable {

	private ConcurrentHashMap<Integer, BlockingQueue<SignalInfo>> map;
	private ConcurrentHashMap<Address, BlockingQueue<SignalInfo>> replicas;
	private AtomicInteger nextInLine;
	private AtomicInteger mapSize;
	private final int THREADS;
	private AtomicInteger replSize;
	private MyMessageDispatcher dispatcher;
	private Address newMember;
	private List<Address> members;
	
	public NewNodeTask(
			ConcurrentHashMap<Integer, BlockingQueue<SignalInfo>> map,
			AtomicInteger mapSize, AtomicInteger nextInLine,
			ConcurrentHashMap<Address, BlockingQueue<SignalInfo>> replicas,
			AtomicInteger replSize, int THREADS, MyMessageDispatcher dispatcher, Tuple<Address, List<Address>> tuple) {

		this.map = map;
		this.replicas = replicas;
		this.nextInLine = nextInLine;
		this.mapSize = mapSize;
		this.dispatcher = dispatcher;
		this.THREADS = THREADS;
		this.replSize = replSize;
		this.newMember = tuple.getFirst();
		this.members = tuple.getSecond();
	}

	@Override
	public void run() {
		
		balancePrimaries();
		balanceReplicas();
	}

	private void balanceReplicas() {
		
		int formerSize = replSize.get();
		int numMembers = members.size();
		List<Future<Object>> futureResponses = new ArrayList<>();
		while (replSize.get() > formerSize * (numMembers  - 1) / numMembers) {
			
			//TODO ask what happens if there is no backups in this node (should not happen)
			BlockingQueue<SignalInfo> list;
			while (true) {
				Tuple<Address, Address> tuple2 = Utils.chooseRandomMember(members);
				list = replicas.get(tuple2.getFirst());
				if (list != null) {
					break;
				}
				list = replicas.get(tuple2.getSecond());
				if (list != null) {
					break;
				}
			}
			
			SignalInfo sInfo2 = list.poll();
			replSize.decrementAndGet();
			
			MyMessage<Signal> myMessage = new MyMessage<Signal>(sInfo2.getSignal(), Operation.MOVE, true, sInfo2.getCopyAddress());
			NotifyingFuture<Object> f = dispatcher.sendMessage(newMember, myMessage);
			futureResponses.add(f);
		}
		
		for (final Future<Object> future : futureResponses) {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void balancePrimaries() {
		
		int numMembers = members.size();
		int formerSize = mapSize.get();
		List<Future<Object>> futureResponses = new ArrayList<>();
		while (mapSize.get() > formerSize * (numMembers - 1) / numMembers) {

//			System.out.println("MAPSIZE: "+ mapSize.get() + " CUENTA: " + formerSize * (numMembers - 1) / numMembers);
			
			BlockingQueue<SignalInfo> list;
			SignalInfo sInfo;
			while(true) {
				 list = map.get(nextInLine.getAndDecrement() % THREADS);
				mapSize.decrementAndGet();
				sInfo = list.poll();
				if (sInfo != null) {
					break;
				}
			}		
						
			MyMessage<Signal> myMessage = new MyMessage<Signal>(sInfo.getSignal(), Operation.MOVE, false, sInfo.getCopyAddress());
			NotifyingFuture<Object> f = dispatcher.sendMessage(newMember, myMessage);
			futureResponses.add(f);
		}
		
		for (final Future<Object> future : futureResponses) {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
