package ar.edu.itba.pod.legajo50758.message;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.util.FutureListener;
import org.jgroups.util.NotifyingFuture;

import ar.edu.itba.pod.legajo50758.utils.DegradedModeException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

public class MessageDispatcher {

	private final JChannel channel;
	private final AtomicInteger idGenerator = new AtomicInteger(0);
	private final Multimap<Address, MyFutureImpl<?>> addressToFuture = Multimaps.synchronizedMultimap(HashMultimap.<Address, MyFutureImpl<?>>create());
	private final Map<Integer, MyFutureImpl<?>> idToFuture = new ConcurrentHashMap<>();
	
	public MessageDispatcher(JChannel channel) {
		
		this.channel = channel;
	}
	
	public <T> NotifyingFuture<T> send(Address address, Serializable content) {

		final int id = idGenerator.getAndIncrement();
		final MyFutureImpl<T> future = new MyFutureImpl<T>(id);

		addressToFuture.put(address, future);
		idToFuture.put(id, future);

		try {
			channel.send(address, new RequestMessage(id, content));
		} catch (final Exception e) {
			addressToFuture.remove(address, future);
			idToFuture.remove(id);
			future.nodeDisconnected();
		}
		return future;
	}
	
	public void processResponse(Address origin, ResponseMessage response) {

		final MyFutureImpl<?> future = idToFuture.remove(response.getId());
		if (future != null) {
			addressToFuture.remove(origin, future);
			future.setResponse(response.getContent());
		}
	}

	public void respond(Address address, int id, Serializable content) throws Exception {
		channel.send(address, new ResponseMessage(id, content));
	}
	
	public void nodeDisconnected(Address address) {
		final Collection<MyFutureImpl<?>> aborted = addressToFuture.removeAll(address);
		if (aborted != null) {
			for (final MyFutureImpl<?> future : aborted) {
				idToFuture.remove(future.getId());
				future.nodeDisconnected();
			}
		}
	}	
	
	private class MyFutureImpl<T> implements NotifyingFuture<T> {

		private T response;
		private final CountDownLatch isDone = new CountDownLatch(1);
		private boolean disconnected;
		private FutureListener<T> listener;
		private int id;
		private Object lock = new Object();
		
		public MyFutureImpl(int id) {
			this.id = id;
		}
		
		@SuppressWarnings("unchecked")
		public synchronized void setResponse(Serializable content) {
			
			if (isDone.getCount() == 0 || disconnected) {
				throw new IllegalStateException();
			}

			try {
				response = (T) content;
			} catch (final ClassCastException e) {
				
				response = null;
			}
			done();
			
		}

		public int getId() {
			return id;
		}
		
		private void done() {
			
			isDone.countDown();
			if (listener != null) {
				listener.futureDone(this);
			}
		}
		
		public void nodeDisconnected() {

			synchronized (lock) {
				
				if (isDone.getCount() != 0) {
					disconnected = true;
				}
				done();
			}
		}

		@Override
		public boolean cancel(boolean arg0) {
			return false;
		}

		@Override
		public T get() throws InterruptedException, DegradedModeException {
			
			isDone.await();
			synchronized (lock) {
				if (disconnected) {
					System.out.println("throwing exceptionnnn");
					throw new DegradedModeException("The recipient disconnected before answering");
				}
				return response;				
			}
		}

		@Override
		public T get(long timeout, TimeUnit timeUnit) throws InterruptedException, DegradedModeException, TimeoutException {
			
			if (isDone.await(timeout, timeUnit)) {
				return get();
			}
			throw new TimeoutException();
		}

		@Override
		public boolean isCancelled() {
			return false;
		}

		@Override
		public boolean isDone() {
			return isDone.getCount() == 0;
		}

		@Override
		public NotifyingFuture<T> setListener(FutureListener<T> listener) {
			
			synchronized (lock) {
				if (isDone() && listener != null) {
					listener.futureDone(this);
				}
				this.listener = listener;
				
				return this;				
			}
		}
	}
}
