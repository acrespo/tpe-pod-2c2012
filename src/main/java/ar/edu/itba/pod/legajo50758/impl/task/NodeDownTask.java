package ar.edu.itba.pod.legajo50758.impl.task;

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
import ar.edu.itba.pod.legajo50758.impl.message.MyMessage;
import ar.edu.itba.pod.legajo50758.impl.message.MessageDispatcher;
import ar.edu.itba.pod.legajo50758.impl.message.Operation;
import ar.edu.itba.pod.legajo50758.impl.utils.SignalInfo;
import ar.edu.itba.pod.legajo50758.impl.utils.Synchronizer;
import ar.edu.itba.pod.legajo50758.impl.utils.Tuple;
import ar.edu.itba.pod.legajo50758.impl.utils.Utils;

public class NodeDownTask implements Runnable {

	private final Queue<SignalInfo> lostReplicas;
	private final BlockingQueue<SignalInfo> lostPrimaries;
	private final MessageDispatcher dispatcher;
	private final List<Address> members;
	private final Address myAddress;
	private final MessageConsumer worker;
	private final AtomicBoolean degradedMode;
	private Synchronizer waitForBalancing;

	public NodeDownTask(BlockingQueue<SignalInfo> lostPrimaries,
			Queue<SignalInfo> lostReplicas, MessageDispatcher dispatcher,
			List<Address> members, Address myAddress, MessageConsumer worker, 
			AtomicBoolean degradedMode, Synchronizer waitForBalancing) {
		
		this.lostPrimaries = lostPrimaries;
		this.lostReplicas = lostReplicas;
		this.dispatcher = dispatcher;
		this.members = members;
		this.myAddress = myAddress;
		this.worker = worker;
		this.degradedMode = degradedMode;
		this.waitForBalancing = waitForBalancing;
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
		waitForBalancing.release();
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
