package ar.edu.itba.pod.legajo50758.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.util.NotifyingFuture;

import ar.edu.itba.pod.legajo50758.api.Signal;

public class NewNodeTask implements Runnable {

	private final MySignalInfoMultimap<Integer> primaries;
	private final MySignalInfoMultimap<Address> replicas;
	private final AtomicInteger nextInLine;
	private final int THREADS;
	private final MyMessageDispatcher dispatcher;
	private final Address newMember;
	private final List<Address> members;
	private final MyWorker worker;
	private final AtomicBoolean degradedMode;

	public NewNodeTask(
			MySignalInfoMultimap<Integer> primaries, AtomicInteger nextInLine,
			MySignalInfoMultimap<Address> replicas, int THREADS,
			MyMessageDispatcher dispatcher,
			Tuple<Address, List<Address>> tuple, MyWorker worker, 
			AtomicBoolean degradedMode) {

		this.primaries = primaries;
		this.replicas = replicas;
		this.nextInLine = nextInLine;
		this.dispatcher = dispatcher;
		this.THREADS = THREADS;
		this.newMember = tuple.getFirst();
		this.members = tuple.getSecond();
		this.worker = worker;
		this.degradedMode = degradedMode;
	}

	@Override
	public void run() {

		try {
			balancePrimaries();
			balanceReplicas();
			worker.phaseEnd(members.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
		degradedMode.set(false);
		//TODO resumir trabajos de procesamiento
	}

	private void balanceReplicas() throws Exception {

		int formerSize = replicas.size();
		int numMembers = members.size();
		List<Future<Object>> futureResponses = new ArrayList<>();
		while (replicas.size() > (formerSize +1) * (numMembers - 1) / numMembers) {

//			 Utils.nodeSnapshot(myAddress, primaries, replicas);
//			System.out.println("CUENTA --- replSize.get(): " + replicas.size() + " formerSize * (numMembers - 1) / numMembers: " + (formerSize * (numMembers - 1) / numMembers));
			
			SignalInfo sInfo2;
			BlockingQueue<SignalInfo> list;
			Address chosen;
			while (true) {
				Tuple<Address, Address> tuple2 = Utils.chooseRandomMember(members);
				
				if (!tuple2.getFirst().equals(newMember)) {
					 list = replicas.get(tuple2.getFirst());
					if (list != null) {
						chosen = tuple2.getFirst();
						break;
					}
				}
				
				if (!tuple2.getSecond().equals(newMember)) {
					list = replicas.get(tuple2.getSecond());
					if (list != null) {
						chosen = tuple2.getSecond();
						break;
					}
				}
			}

			sInfo2 = replicas.pollList(chosen);
			if (sInfo2 == null) {
				continue;
			}

			MyMessage<Signal> myMessage = new MyMessage<Signal>(sInfo2.getSignal(), Operation.MOVE, true, sInfo2.getCopyAddress());
			NotifyingFuture<Object> f = dispatcher.send(newMember, myMessage);
			futureResponses.add(f);
		}

		Utils.waitForResponses(futureResponses);
		
		worker.phaseEnd(members.size());
	}


	private void balancePrimaries() throws Exception {

		int numMembers = members.size();
		int formerSize = primaries.size();
		List<Future<Object>> futureResponses = new ArrayList<>();
		while (primaries.size() > formerSize * (numMembers - 1) / numMembers) {

//			Utils.nodeSnapshot(myAddress, primaries, replicas);
			SignalInfo sInfo;
			while (true) {
				sInfo = primaries.pollList(nextInLine.getAndDecrement() % THREADS);
				if (sInfo != null) {
					break;
				}
			}

			MyMessage<Signal> myMessage = new MyMessage<Signal>(sInfo.getSignal(), Operation.MOVE, false, sInfo.getCopyAddress());
			NotifyingFuture<Object> f = dispatcher.send(newMember, myMessage);
			futureResponses.add(f);
		}

		Utils.waitForResponses(futureResponses);
		
		worker.phaseEnd(members.size());
	}
}
