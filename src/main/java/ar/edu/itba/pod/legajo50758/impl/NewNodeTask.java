package ar.edu.itba.pod.legajo50758.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.util.NotifyingFuture;

import ar.edu.itba.pod.legajo50758.api.Signal;

public class NewNodeTask implements Runnable {

	private final ConcurrentHashMap<Integer, BlockingQueue<SignalInfo>> map;
	private final ConcurrentHashMap<Address, BlockingQueue<SignalInfo>> replicas;
	private final AtomicInteger nextInLine;
	private final AtomicInteger mapSize;
	private final int THREADS;
	private final AtomicInteger replSize;
	private final MyMessageDispatcher dispatcher;
	private final Address newMember;
	private final List<Address> members;
	private final Address myAddress;
	private final MyWorker worker;
	private final AtomicBoolean degradedMode;

	public NewNodeTask(
			ConcurrentHashMap<Integer, BlockingQueue<SignalInfo>> map,
			AtomicInteger mapSize, AtomicInteger nextInLine,
			ConcurrentHashMap<Address, BlockingQueue<SignalInfo>> replicas,
			AtomicInteger replSize, int THREADS,
			MyMessageDispatcher dispatcher,
			Tuple<Address, List<Address>> tuple, Address myAddress,
			MyWorker worker, AtomicBoolean degradedMode) {

		this.map = map;
		this.replicas = replicas;
		this.nextInLine = nextInLine;
		this.mapSize = mapSize;
		this.dispatcher = dispatcher;
		this.THREADS = THREADS;
		this.replSize = replSize;
		this.newMember = tuple.getFirst();
		this.members = tuple.getSecond();
		this.myAddress = myAddress;
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		degradedMode.set(false);
		//TODO resumir trabajos de procesamiento
	}

	private void balanceReplicas() throws Exception {

		int formerSize = replSize.get();
		int numMembers = members.size();
		List<Future<Object>> futureResponses = new ArrayList<>();
		while (replSize.get() > (formerSize +1) * (numMembers - 1) / numMembers) {

			// System.out.println("CHOOOSING RANDOM REPLICA TO BALANCE");
			// Utils.nodeSnapshot(myAddress, map, replicas);

			System.out.println("CUENTA --- replSize.get(): " + replSize.get() + " formerSize * (numMembers - 1) / numMembers: " + (formerSize * (numMembers - 1) / numMembers));
			
			// TODO ask what happens if there is no backups in this node (should
			// not happen)
			BlockingQueue<SignalInfo> list;
			while (true) {
				Tuple<Address, Address> tuple2 = Utils.chooseRandomMember(members);
				list = replicas.get(tuple2.getFirst());
				if (list != null && !tuple2.getFirst().equals(newMember)) {
					break;
				}
				list = replicas.get(tuple2.getSecond());
				if (list != null && !tuple2.getSecond().equals(newMember)) {
					break;
				}
			}

			SignalInfo sInfo2 = list.poll();
			System.out.println("sInfo2:" + sInfo2);
			if (sInfo2 == null) {
				replicas.remove(list);
				continue;
			}
			replSize.decrementAndGet();

			MyMessage<Signal> myMessage = new MyMessage<Signal>(sInfo2.getSignal(), Operation.MOVE, true,sInfo2.getCopyAddress());
			NotifyingFuture<Object> f = dispatcher.send(newMember, myMessage);
			futureResponses.add(f);
		}

		Utils.waitForResponses(futureResponses);
		
		worker.phaseEnd(members.size());
	}


	private void balancePrimaries() throws Exception {

		int numMembers = members.size();
		int formerSize = mapSize.get();
		List<Future<Object>> futureResponses = new ArrayList<>();
		while (mapSize.get() > formerSize * (numMembers - 1) / numMembers) {

			BlockingQueue<SignalInfo> list;
			SignalInfo sInfo;
			while (true) {
				list = map.get(nextInLine.getAndDecrement() % THREADS);
				mapSize.decrementAndGet();
				sInfo = list.poll();
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
