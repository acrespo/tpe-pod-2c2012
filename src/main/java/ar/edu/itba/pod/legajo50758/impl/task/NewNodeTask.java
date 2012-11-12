package ar.edu.itba.pod.legajo50758.impl.task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.util.NotifyingFuture;

import ar.edu.itba.pod.legajo50758.api.Signal;
import ar.edu.itba.pod.legajo50758.impl.message.MyMessage;
import ar.edu.itba.pod.legajo50758.impl.message.MessageDispatcher;
import ar.edu.itba.pod.legajo50758.impl.message.Operation;
import ar.edu.itba.pod.legajo50758.impl.utils.SignalInfo;
import ar.edu.itba.pod.legajo50758.impl.utils.SignalInfoMultimap;
import ar.edu.itba.pod.legajo50758.impl.utils.Synchronizer;
import ar.edu.itba.pod.legajo50758.impl.utils.Tuple;
import ar.edu.itba.pod.legajo50758.impl.utils.Utils;

public class NewNodeTask implements Runnable {

	private final SignalInfoMultimap<Integer> primaries;
	private final SignalInfoMultimap<Address> replicas;
	private final AtomicInteger nextInLine;
	private final int THREADS;
	private final MessageDispatcher dispatcher;
	private final Address newMember;
	private final List<Address> members;
	private final MessageConsumer worker;
	private final AtomicBoolean degradedMode;
	private Synchronizer waitForBalancing;

	public NewNodeTask(
			SignalInfoMultimap<Integer> primaries, AtomicInteger nextInLine,
			SignalInfoMultimap<Address> replicas, int THREADS,
			MessageDispatcher dispatcher,
			Tuple<Address, List<Address>> tuple, MessageConsumer worker, 
			AtomicBoolean degradedMode, Synchronizer waitForBalancing) {

		this.primaries = primaries;
		this.replicas = replicas;
		this.nextInLine = nextInLine;
		this.dispatcher = dispatcher;
		this.THREADS = THREADS;
		this.newMember = tuple.getFirst();
		this.members = tuple.getSecond();
		this.worker = worker;
		this.degradedMode = degradedMode;
		this.waitForBalancing = waitForBalancing;
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
		waitForBalancing.release();
	}

	private void balanceReplicas() throws Exception {

		int formerSize = replicas.size();
		int numMembers = members.size();
		List<Future<Object>> futureResponses = new ArrayList<>();
		while (replicas.size() > (formerSize +1) * (numMembers - 1) / numMembers) {

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
