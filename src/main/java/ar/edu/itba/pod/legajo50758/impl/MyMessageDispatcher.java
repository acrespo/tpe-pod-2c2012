package ar.edu.itba.pod.legajo50758.impl;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.util.FutureListener;
import org.jgroups.util.NotifyingFuture;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

public class MyMessageDispatcher {

	private final JChannel channel;
	private final AtomicInteger idGenerator = new AtomicInteger(0);
	private final Multimap<Address, MyFutureImpl<?>> addressToFuture = Multimaps.synchronizedMultimap(HashMultimap.<Address, MyFutureImpl<?>>create());
	private final Map<Integer, MyFutureImpl<?>> idToFuture = new ConcurrentHashMap<>();
	
	public MyMessageDispatcher(JChannel channel) {
		
		this.channel = channel;
	}
	
	public <T> NotifyingFuture<T> send(Address address, Serializable content) {

		final int id = idGenerator.getAndIncrement();
		final MyFutureImpl<T> future = new MyFutureImpl<T>();

		addressToFuture.put(address, future);
		idToFuture.put(id, future);

		try {
			channel.send(address, new RequestMessage(id, content));
		} catch (final Exception e) {
			addressToFuture.remove(address, future);
			idToFuture.remove(id);
			future.nodeDisconnected(e);
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
	
	
	private static class MyFutureImpl<T> implements NotifyingFuture<T> {

		private T response;
		private final CountDownLatch isDone = new CountDownLatch(1);
		private boolean disconnected;
		private Exception e;
		private FutureListener<T> listener;
		
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

		public synchronized void nodeDisconnected(Exception e) {
			
			if (isDone.getCount() != 0) {
				disconnected = true;
				this.e = e;
			}
			done();
		}

		private void done() {
			
			isDone.countDown();
			if (listener != null) {
				listener.futureDone(this);
			}
		}
		
		public synchronized void nodeDisconnected() {

			if (isDone.getCount() != 0) {
				disconnected = true;
			}
			done();
		}

		@Override
		public synchronized boolean cancel(boolean arg0) {
			return false;
		}

		@Override
		public T get() throws InterruptedException, ExecutionException {
			
			isDone.await();
			synchronized (this) {
				if (disconnected) {
					throw new ExecutionException("The reciepient disconnected before answering", e);
				}
				return response;				
			}
		}

		@Override
		public T get(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
			
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
		public synchronized NotifyingFuture<T> setListener(FutureListener<T> listener) {
			
			if (isDone() && listener != null) {
				listener.futureDone(this);
			}
			this.listener = listener;

			return this;
		}
	}
}
