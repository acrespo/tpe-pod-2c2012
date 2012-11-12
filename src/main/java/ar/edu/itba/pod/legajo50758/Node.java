package ar.edu.itba.pod.legajo50758;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

import ar.edu.itba.pod.api.NodeStats;
import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.SPNode;
import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.api.SignalProcessor;
import ar.edu.itba.pod.api.Result.Item;
import ar.edu.itba.pod.legajo50758.message.MessageDispatcher;
import ar.edu.itba.pod.legajo50758.message.MyMessage;
import ar.edu.itba.pod.legajo50758.message.Operation;
import ar.edu.itba.pod.legajo50758.task.MessageConsumer;
import ar.edu.itba.pod.legajo50758.utils.DegradedModeException;
import ar.edu.itba.pod.legajo50758.utils.SignalInfo;
import ar.edu.itba.pod.legajo50758.utils.SignalInfoMultimap;
import ar.edu.itba.pod.legajo50758.utils.Synchronizer;
import ar.edu.itba.pod.legajo50758.utils.Tuple;
import ar.edu.itba.pod.legajo50758.utils.Utils;

public class Node implements SignalProcessor, SPNode {

	private final int THREADS;
	private final AtomicInteger nextInLine = new AtomicInteger(0);
	private final SignalInfoMultimap<Integer> primaries = new SignalInfoMultimap<>();
	private final SignalInfoMultimap<Address> replicas = new SignalInfoMultimap<>();

	private final JChannel channel; 
	private View currentView = null;
	
	private final AtomicInteger receivedSignals = new AtomicInteger(0);
	private String cluster = null;
	
	private final BlockingQueue<Message> msgQueue = new LinkedBlockingQueue<Message>();
	private final MessageConsumer worker;
	private Thread workerThread;
	private final AtomicBoolean degradedMode = new AtomicBoolean(true);
	private final Synchronizer waitForBalacing = new Synchronizer();
	
	private final MessageDispatcher dispatcher;
	private final MyReceiverAdapter receiver;
	
	public Node(int nThreads) throws Exception {
		
		THREADS = nThreads;
		channel = new JChannel("jgroups.xml");
		
		dispatcher = new MessageDispatcher(channel);
		worker = new MessageConsumer(msgQueue, channel, primaries, replicas, nextInLine, dispatcher, THREADS, degradedMode, waitForBalacing);
		receiver = new MyReceiverAdapter();
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
		
		return primaries.isEmpty();
	}

	@Override
	public void exit() throws RemoteException {
				
		clear();
		receivedSignals.set(0);
		cluster = null;
		
		if (channel.isConnected()) {
			
			channel.disconnect();
			workerThread.interrupt();
			workerThread = null;
			currentView = null;
			System.out.println("channel disconnected");
		}

		degradedMode.set(true);
		
	}

	private void clear() {
		primaries.clear();
		replicas.clear();
	}

	@Override
	public NodeStats getStats() throws RemoteException {
		return new NodeStats(
				cluster == null ? "standalone" : "cluster " + cluster, 
				receivedSignals.longValue(),
				primaries.size(), 
				replicas.size(), 
				degradedMode.get());
	}

	@Override
	public void add(Signal signal) throws RemoteException {
		
		if (channel.isConnected()) {
			List<Address> members = channel.getView().getMembers();
			Tuple<Address, Address> tuple = Utils.chooseRandomMember(members);
			Address primaryCopyAddress = tuple.getFirst();
			Address backupAddress = tuple.getSecond();
			
			LinkedList<Future<Object>> futures = new LinkedList<>();
			try {
				futures.add(dispatcher.send(primaryCopyAddress, new MyMessage<Signal>(signal, Operation.ADD, false, backupAddress)));
				futures.add(dispatcher.send(backupAddress, new MyMessage<Signal>(signal, Operation.ADD, true, primaryCopyAddress)));
			} catch (Exception e) {
				e.printStackTrace();
				throw new RemoteException();
			}
				
			Utils.waitForResponses(futures);
			
		} else {
			primaries.put(nextInLine.getAndIncrement() % THREADS, new SignalInfo(signal, null, true));
		}
	}

	@Override
	public Result findSimilarTo(Signal signal) throws RemoteException {
		
		boolean finished = false;
		while (!finished) {
			waitForBalacing.acquireAndRelease();
			receivedSignals.incrementAndGet();
					
			if (channel.isConnected()) {
				List<Future<Result>> futureResults = new ArrayList<>();
				List<Result> results = new LinkedList<>();
				for (final Address address : channel.getView().getMembers()) {
					futureResults.add(dispatcher.<Result>send(address, new MyMessage<Signal>(signal, Operation.QUERY)));
				}
							
				for (final Future<Result> future : futureResults) {
					try {
						results.add(future.get());
					} catch (DegradedModeException e) {
						// RETRY
						System.out.println("findSimilar: node down! Retrying...");
						continue;
					} catch (InterruptedException | ExecutionException e) {
						e.printStackTrace();
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
			} else {
				return worker.process(signal);
			}
		}
		return null;
	}
	
	private class MyReceiverAdapter extends ReceiverAdapter {

		@Override
		public void receive(Message message) {
			msgQueue.add(message);
		}
		
		@Override
		public void viewAccepted(final View newView) {
			
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					if (currentView == null) {
						waitForBalacing.drainPermits();
						currentView = newView;
						workerThread = new Thread(worker);
						workerThread.start();		
						
						if (newView.size() > 1) {
							
							try {
								worker.phaseEnd(newView.size());
								worker.phaseEnd(newView.size());
								worker.phaseEnd(newView.size());
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
						degradedMode.set(false);
						waitForBalacing.release();
						return;
					}
					
					for (Address member: newView.getMembers()) {
						if (!currentView.containsMember(member)) {
							// member is NEW NODE
							degradedMode.set(true);
							waitForBalacing.drainPermits();
							
							Tuple<Address, List<Address>> tuple = new Tuple<>(member, newView.getMembers());
							MyMessage<Tuple<Address, List<Address>>> myMsg = new MyMessage<>(tuple, Operation.NODEUP);
							msgQueue.add(new Message(channel.getAddress(), myMsg));
							currentView = newView;
							return;
						}
					}
					
					for (Address currMember: currentView.getMembers()) {
						if (!newView.containsMember(currMember)) {
							//currMember is DOWN
							degradedMode.set(true);
							waitForBalacing.drainPermits();
							dispatcher.nodeDisconnected(currMember);
							
							Tuple<Address, List<Address>> tuple = new Tuple<>(currMember, newView.getMembers());
							MyMessage<Tuple<Address, List<Address>>> myMsg = new MyMessage<>(tuple, Operation.NODEDOWN);
							msgQueue.add(new Message(channel.getAddress(), myMsg));
							currentView = newView;
							return;
						}
					}
				}
			}).start();
		}		
	}	
}
