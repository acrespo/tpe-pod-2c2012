package ar.edu.itba.pod.legajo50758.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;

import ar.edu.itba.pod.legajo50758.api.Result;
import ar.edu.itba.pod.legajo50758.api.Signal;

public class MyWorker implements Runnable {

	private BlockingQueue<Message> msgQueue;
	private JChannel channel;
	private ConcurrentHashMap<Integer, BlockingQueue<SignalInfo>> map;
	private ConcurrentHashMap<Address, BlockingQueue<SignalInfo>> replicas;
	private AtomicInteger nextInLine;
	private AtomicInteger mapSize;
	private final MyMessageDispatcher dispatcher;
	private ExecutorService pool;
	private final int THREADS;
	private AtomicInteger replSize;

	public MyWorker(BlockingQueue<Message> msqQueue, JChannel channel,
			ConcurrentHashMap<Integer, BlockingQueue<SignalInfo>> map,
			ConcurrentHashMap<Address, BlockingQueue<SignalInfo>> replicas,
			AtomicInteger mapSize, AtomicInteger nextInLine,
			MyMessageDispatcher dispatcher, final int THREADS, AtomicInteger replSize) {
		
		this.msgQueue = msqQueue;
		this.channel = channel;
		this.map = map;
		this.replicas = replicas;
		this.nextInLine = nextInLine;
		this.mapSize = mapSize;
		this.dispatcher = dispatcher;
		this.THREADS = THREADS;
		pool = Executors.newFixedThreadPool(THREADS);
		this.replSize = replSize;
	}

	@Override
	public void run() {

		System.out.println("Running worker thread");
		Message message = null;
		while (true) {

			try {
				message = msgQueue.take();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
	
				if (obj != null && obj instanceof Signal) {
	
					if (op == Operation.ADD) {
//						System.out.println("RECEIVED ADD");
						if (!myMessage.isReplica()) {
							BlockingQueue<SignalInfo> list = map.get(nextInLine.getAndIncrement() % THREADS);
							list.add(new SignalInfo((Signal) obj, myMessage.getCopyAddress(), true));
							mapSize.incrementAndGet();
						} else {
//							System.out.println("RECEIVED ADD REPLICA");
							BlockingQueue<SignalInfo> list = replicas.get(myMessage.getCopyAddress());
							if (list == null) {
								list = new LinkedBlockingQueue<>();
								replicas.put(myMessage.getCopyAddress(), list);
							}
							list.add(new SignalInfo((Signal) obj, myMessage.getCopyAddress(), false));
							replSize.incrementAndGet();
						}
	
					} else if (op == Operation.MOVE) {
	
						if (!myMessage.isReplica()) {
							System.out.println("Moving primary");
							BlockingQueue<SignalInfo> list = map.get(nextInLine.getAndIncrement() % THREADS);
							list.add(new SignalInfo((Signal) obj, myMessage.getCopyAddress(), true));
							mapSize.incrementAndGet();
							try {
								send(new MyMessage<Signal>((Signal) obj, Operation.MOVED, false, channel.getAddress()), myMessage.getCopyAddress());
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						} else {
							System.out.println("Moving replica");
							BlockingQueue<SignalInfo> list = replicas.get(myMessage.getCopyAddress());
							if (list == null) {
								list = new LinkedBlockingQueue<>();
								replicas.put(myMessage.getCopyAddress(), list);
							}
							list.add(new SignalInfo((Signal) obj, myMessage.getCopyAddress(), false));
							replSize.incrementAndGet();
							try {
								send(new MyMessage<Signal>((Signal) obj, Operation.MOVED, true, channel.getAddress()), myMessage.getCopyAddress());
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					} else if (op == Operation.MOVED) {
						
						SignalInfo signalInfo;
						if (!myMessage.isReplica()) {
							signalInfo = searchForSignal((Signal) obj, replicas.values());
							System.out.println("MOVED primary");
						} else {
							System.out.println("MOVED replica");
							signalInfo = searchForSignal((Signal) obj, map.values());
						}
						signalInfo.setCopyAddress(message.getSrc());
					}
				} else if (obj != null && obj instanceof Tuple<?, ?>) {
					
					if (op == Operation.NODEUP) {
						// member is NEW NODE
						Tuple<Address, List<Address>> tuple = (Tuple<Address, List<Address>>) obj;
						Address member = tuple.getFirst();
						List<Address> members = tuple.getSecond();
						int numMembers = members.size();
						
						int formerSize = mapSize.get();
						while (mapSize.get() > formerSize * (numMembers - 1) / numMembers) {
		
//							System.out.println("MAPSIZE: "+ mapSize.get() + " CUENTA: " + formerSize * (numMembers - 1) / numMembers);
							
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
							
							//TODO ask what happens if there is no backups in this node (should not happen)
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
		
							System.out.println("EQUALS PRIMARY vs REPLICA :" + sInfo.getSignal().equals(sInfo2.getSignal()));
							try {
								send(new MyMessage<Signal>(sInfo.getSignal(), Operation.MOVE, false, sInfo.getCopyAddress()), member);
//								send(new MyMessage<Signal>(sInfo2.getSignal(), Operation.MOVE, true, sInfo2.getCopyAddress()), member);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}
			}
		}
	}

	private void handleRequestMessage(Message message) {

		final RequestMessage reqMessage = (RequestMessage) message.getObject();
		final Object obj = reqMessage.getContent();

		if (obj instanceof MyMessage<?>) {

			MyMessage<?> myMessage = (MyMessage<?>) obj;
			if (myMessage.getContent() != null && myMessage.getContent() instanceof Signal) {

				Signal signal = (Signal) myMessage.getContent();
				if (myMessage.getOp() == Operation.QUERY) {

					System.out.println("Processing Local signals");
					List<Future<Result>> futures = new ArrayList<>(THREADS);

					for (BlockingQueue<SignalInfo> signals : map.values()) {
						futures.add(pool.submit(new MyTask(signal, signals)));
					}

					List<Result> results = new ArrayList<>(THREADS);

					for (Future<Result> future : futures) {
						try {
							results.add(future.get());
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (ExecutionException e) {
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
					
					System.out.println("REsult " + result);
					try {
						dispatcher.respondTo(message.getSrc(), reqMessage.getId(), result);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}

	private SignalInfo searchForSignal(Signal obj, Collection<BlockingQueue<SignalInfo>> collection) {

		for (BlockingQueue<SignalInfo> list : collection) {
			for (SignalInfo sInfo : list) {
				if (sInfo.getSignal().equals(obj)) {
					return sInfo;
				}
			}
		}
		return null;
	}

	private <T> void send(MyMessage<T> myMessage, Address destAddress) throws Exception {
		channel.send(new Message(destAddress, null, myMessage));
	}
}
