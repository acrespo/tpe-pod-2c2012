package ar.edu.itba.pod.legajo50758.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.jgroups.Address;
import org.jgroups.util.NotifyingFuture;

import ar.edu.itba.pod.legajo50758.api.Signal;

public class NodeDownTask implements Runnable {

	private Queue<SignalInfo> lostReplicas;
	private BlockingQueue<SignalInfo> lostPrimaries;
	private MyMessageDispatcher dispatcher;
	private List<Address> members;
	private Address myAddress;

	public NodeDownTask(BlockingQueue<SignalInfo> lostPrimaries,
			Queue<SignalInfo> lostReplicas, MyMessageDispatcher dispatcher,
			List<Address> members, Address myAddress) {
		
		this.lostPrimaries = lostPrimaries;
		this.lostReplicas = lostReplicas;
		this.dispatcher = dispatcher;
		this.members = members;
		this.myAddress = myAddress;
	}

	@Override
	public void run() {
		
		if (lostPrimaries != null && !lostPrimaries.isEmpty()) {
			distributePrimaries();
		}
		if (lostReplicas != null && !lostReplicas.isEmpty()) {
			distributeReplicas();
		}

	}

	private void distributeReplicas() {
		
		SignalInfo sInfo;
		List<Future<Object>> futureResponses = new ArrayList<>();
		while((sInfo = lostReplicas.poll()) != null) {
			
//			System.out.println("distributing replica");
			
			Tuple<Address, Address> tuple2 = Utils.chooseRandomMember(members);
			Address chosen = tuple2.getFirst(); 
					
			MyMessage<Signal> myMessage = new MyMessage<Signal>(sInfo.getSignal(), Operation.MOVE, true, myAddress);
			NotifyingFuture<Object> f = dispatcher.sendMessage(chosen, myMessage);

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

	private void distributePrimaries() {
		
		SignalInfo sInfo;
		List<Future<Object>> futureResponses = new ArrayList<>();
		while((sInfo = lostPrimaries.poll()) != null) {
//			System.out.println("distributing primary");

			
			Tuple<Address, Address> tuple2 = Utils.chooseRandomMember(members);
			Address chosen = tuple2.getFirst(); 
					
			MyMessage<Signal> myMessage = new MyMessage<Signal>(sInfo.getSignal(), Operation.MOVE, false, myAddress);
			NotifyingFuture<Object> f = dispatcher.sendMessage(chosen, myMessage);

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
