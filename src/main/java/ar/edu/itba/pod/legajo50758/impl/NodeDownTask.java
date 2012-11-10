package ar.edu.itba.pod.legajo50758.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jgroups.Address;
import org.jgroups.util.NotifyingFuture;

import ar.edu.itba.pod.legajo50758.api.Signal;

public class NodeDownTask implements Runnable {

	private final Queue<SignalInfo> lostReplicas;
	private final BlockingQueue<SignalInfo> lostPrimaries;
	private final MyMessageDispatcher dispatcher;
	private final List<Address> members;
	private final Address myAddress;
	private final MyWorker worker;
	private final AtomicBoolean degradedMode;

	public NodeDownTask(BlockingQueue<SignalInfo> lostPrimaries,
			Queue<SignalInfo> lostReplicas, MyMessageDispatcher dispatcher,
			List<Address> members, Address myAddress, MyWorker worker, AtomicBoolean degradedMode) {
		
		this.lostPrimaries = lostPrimaries;
		this.lostReplicas = lostReplicas;
		this.dispatcher = dispatcher;
		this.members = members;
		this.myAddress = myAddress;
		this.worker = worker;
		this.degradedMode = degradedMode;
	}

	@Override
	public void run() {
		
		try { 
			if (lostPrimaries != null && !lostPrimaries.isEmpty()) {
				distributePrimaries();
			}
			if (lostReplicas != null && !lostReplicas.isEmpty()) {
				distributeReplicas();
			}
			worker.phaseEnd(members.size());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		degradedMode.set(false);
		//TODO resumir trabajos de procesamiento
	}

	private void distributeReplicas() throws Exception {
		
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
		
		worker.phaseEnd(members.size());
	}

	private void distributePrimaries() throws Exception {
		
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
		
		worker.phaseEnd(members.size());
	}
}
