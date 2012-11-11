package ar.edu.itba.pod.legajo50758.impl;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
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
				distributeSignals(lostPrimaries, false);
			}
			if (lostReplicas != null && !lostReplicas.isEmpty()) {
				distributeSignals(lostReplicas, true);
			}
			worker.phaseEnd(members.size());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		degradedMode.set(false);
		//TODO resumir trabajos de procesamiento
	}

	private void distributeSignals(Queue<SignalInfo> queue, boolean replica) throws Exception {
		
		SignalInfo sInfo;
		List<Future<Object>> futureResponses = new ArrayList<>();
		while((sInfo = queue.poll()) != null) {
						
			List<Address> allButMe = new LinkedList<>(members);
			allButMe.remove(myAddress);
			
			Tuple<Address, Address> tuple2 = Utils.chooseRandomMember(allButMe);
			Address chosen = tuple2.getFirst(); 
					
			MyMessage<Signal> myMessage = new MyMessage<Signal>(sInfo.getSignal(), Operation.MOVE, replica, myAddress);
			NotifyingFuture<Object> f = dispatcher.send(chosen, myMessage);

			futureResponses.add(f);
		}
		
		Utils.waitForResponses(futureResponses);
		worker.phaseEnd(members.size());
	}
}
