package ar.edu.itba.pod.legajo50758.impl.task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.util.FutureListener;
import org.jgroups.util.NotifyingFuture;

import ar.edu.itba.pod.legajo50758.api.Result;
import ar.edu.itba.pod.legajo50758.api.Signal;
import ar.edu.itba.pod.legajo50758.impl.message.MyMessage;
import ar.edu.itba.pod.legajo50758.impl.message.MyMessageDispatcher;
import ar.edu.itba.pod.legajo50758.impl.message.Operation;
import ar.edu.itba.pod.legajo50758.impl.message.PhaseEnd;
import ar.edu.itba.pod.legajo50758.impl.message.RequestMessage;
import ar.edu.itba.pod.legajo50758.impl.message.ResponseMessage;
import ar.edu.itba.pod.legajo50758.impl.myUtils.MySignalInfoMultimap;
import ar.edu.itba.pod.legajo50758.impl.myUtils.SignalInfo;
import ar.edu.itba.pod.legajo50758.impl.myUtils.Tuple;
import ar.edu.itba.pod.legajo50758.impl.myUtils.Utils;

public class MyWorker implements Runnable {

	private final BlockingQueue<Message> msgQueue;
	private final JChannel channel;
	private final MySignalInfoMultimap<Integer> primaries;
	private final MySignalInfoMultimap<Address> replicas;
	private final AtomicInteger nextInLine;
	private final MyMessageDispatcher dispatcher;
	private final ExecutorService pool;
	private final int THREADS;
	private final Semaphore phaseCounter = new Semaphore(0);
	private final AtomicBoolean degradedMode;

	public MyWorker(BlockingQueue<Message> msqQueue, JChannel channel,
			MySignalInfoMultimap<Integer> primaries,
			MySignalInfoMultimap<Address> replicas, AtomicInteger nextInLine,
			MyMessageDispatcher dispatcher, final int THREADS, AtomicBoolean degradedMode) {
		
		this.msgQueue = msqQueue;
		this.channel = channel;
		this.primaries = primaries;
		this.replicas = replicas;
		this.nextInLine = nextInLine;
		this.dispatcher = dispatcher;
		this.THREADS = THREADS;
		pool = Executors.newFixedThreadPool(THREADS);
		this.degradedMode = degradedMode;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {

		System.out.println("Running worker thread");
		Message message = null;
		while (true) {

			try {
				message = msgQueue.take();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
//				e.printStackTrace();
				return;
			}

			if (message.getObject() instanceof RequestMessage) {
				handleRequestMessage(message);
			} else if (message.getObject() instanceof ResponseMessage) {
				dispatcher.processResponse(message.getSrc(), (ResponseMessage) message.getObject());
			} else if (message.getObject() instanceof MyMessage<?>) {

				// HANDLE EVENT
				MyMessage<?> myMessage = (MyMessage<?>) message.getObject();
				Object obj = myMessage.getContent();
				Operation op = myMessage.getOp();
	
				if (obj != null && obj instanceof Tuple<?, ?>) {
					
					if (op == Operation.NODEUP) {
						// member is NEW NODE
						Tuple<Address, List<Address>> tuple = (Tuple<Address, List<Address>>) obj;
						new Thread(new NewNodeTask(primaries, nextInLine, replicas, THREADS, dispatcher, tuple, this, degradedMode)).start();
					
					} else if (op == Operation.NODEDOWN) {
						
						System.out.println("NODE DOWN!!!!");
						Tuple<Address, List<Address>> tuple = (Tuple<Address, List<Address>>) obj;
						
						Address nodeDown = tuple.getFirst();
						List<Address> members = tuple.getSecond();
						
						BlockingQueue<SignalInfo> lostPrimaries = new LinkedBlockingQueue<>(replicas.get(nodeDown));
						Queue<SignalInfo> lostReplicas = new LinkedList<>(Utils.searchForSignal(nodeDown, primaries.values()));					

						
						System.out.println("NodeDOWN:" + nodeDown);
						
//						Utils.nodeSnapshot(channel.getAddress(), map, replicas);						
//						Utils.printSignals(lostPrimaries, "Lost Primaries: ");
//						Utils.printSignals(lostReplicas, "Lost Replicas: ");
//						System.out.println("Lost Primaries: " + lostPrimaries.size());
//						System.out.println("Lost Replicas: " + lostReplicas.size());
						
						new Thread(new NodeDownTask(lostPrimaries, lostReplicas, dispatcher, members, channel.getAddress(), this, degradedMode)).start();
					}
				}
			} else if (message.getObject() instanceof PhaseEnd) {
				phaseCounter.release();
			}
		}
	}

	private void handleRequestMessage(final Message message) {

		final RequestMessage reqMessage = (RequestMessage) message.getObject();
		final Object obj = reqMessage.getContent();

		if (obj instanceof MyMessage<?>) {

			MyMessage<?> myMessage = (MyMessage<?>) obj;
			if (myMessage.getContent() != null && myMessage.getContent() instanceof Signal) {

				Signal signal = (Signal) myMessage.getContent();
				
				if (myMessage.getOp() == Operation.ADD) {
					if (!myMessage.isReplica()) {
						primaries.put(nextInLine.getAndIncrement() % THREADS, new SignalInfo(signal, myMessage.getCopyAddress(), true));
						
					} else {
						replicas.put(myMessage.getCopyAddress(), new SignalInfo(signal, myMessage.getCopyAddress(), false));
					}
					
					respond(message.getSrc(), reqMessage.getId(), null);

				} else if (myMessage.getOp() == Operation.QUERY) {

					System.out.println("Processing Local signals");
					Result result = process(signal);
					
					System.out.println("Result " + result);
					
					respond(message.getSrc(), reqMessage.getId(), result);
					
				} else if (myMessage.getOp() == Operation.MOVE) {
					
					if (!myMessage.isReplica()) {
//						System.out.println("Moving primary");
						primaries.put(nextInLine.getAndIncrement() % THREADS, new SignalInfo(signal, myMessage.getCopyAddress(), true));
						
						NotifyingFuture<Object> future = dispatcher.send(myMessage.getCopyAddress(), new MyMessage<Signal>(signal, Operation.MOVED, false, channel.getAddress()));
						future.setListener(new FutureListener<Object>() {
							
							@Override
							public void futureDone(Future<Object> future) {
								respond(message.getSrc(), reqMessage.getId(), null);
							}
						});
						
					} else {
//						System.out.println("Moving replica");
						if (!myMessage.getCopyAddress().equals(channel.getAddress()) || channel.getView().getMembers().size() == 1) {
							
							replicas.put(myMessage.getCopyAddress(), new SignalInfo(signal, myMessage.getCopyAddress(), false));
							
							NotifyingFuture<Object> future = dispatcher.send(myMessage.getCopyAddress(), new MyMessage<Signal>(signal, Operation.MOVED, true, channel.getAddress()));
							future.setListener(new FutureListener<Object>() {
								
								@Override
								public void futureDone(Future<Object> future) {
									respond(message.getSrc(), reqMessage.getId(), null);
								}
							});
						} 
					}
				} else if (myMessage.getOp() == Operation.MOVED) {
					
					SignalInfo signalInfo;
//					Utils.nodeSnapshot(channel.getAddress(), map, replicas);
					
					if (!myMessage.isReplica()) {						
//						System.out.println("MOVED primary");
						signalInfo = replicas.remove(signal);
						replicas.put(myMessage.getCopyAddress(), signalInfo);
						
					} else {
//						System.out.println("MOVED replica");
						signalInfo = Utils.searchForSignal(signal, primaries.values());
					}
					
					signalInfo.setCopyAddress(myMessage.getCopyAddress());		
					respond(message.getSrc(), reqMessage.getId(), null);
				}
			}
		}
	}

	public Result process(Signal signal) {
		
		List<Future<Result>> futures = new ArrayList<>(THREADS);

		for (BlockingQueue<SignalInfo> signals : primaries.values()) {
			futures.add(pool.submit(new MyTask(signal, signals)));
		}

		List<Result> results = new ArrayList<>(THREADS);

		for (Future<Result> future : futures) {
			try {
				results.add(future.get());
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		Result result = new Result(signal);

		for (Result res : results) {
			for (Result.Item item : res.items()) {
				result = result.include(item);
			}
		}
		return result;
	}
	
	public void phaseEnd(int numMembers) throws Exception {
//		System.out.println("broadcasting phase end");
		channel.send(new Message(null, new PhaseEnd()));
		phaseCounter.acquire(numMembers);
	}
	
	private void respond(final Address address, final int id, Serializable content) {
		
		try {
			dispatcher.respond(address, id, content);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
