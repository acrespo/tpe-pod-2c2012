package ar.edu.itba.pod.legajo50758.impl.utils;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.ThreadSafe;

import org.jgroups.annotations.GuardedBy;

import ar.edu.itba.pod.legajo50758.api.Signal;


/**
 * Ad-hoc thread-safe implementation of a multimap. Read documentation about {@link #values()} and {@link #get(Object)}
 * which rely on the user not modifying the collections returned by them. As long as the user does not this, this class 
 * will remain thread-safe.
 * 
 */
@ThreadSafe
public class SignalInfoMultimap<K> {

	@GuardedBy("lock")
	private final ConcurrentHashMap<K, BlockingQueue<SignalInfo>> map = new ConcurrentHashMap<>();
	@GuardedBy("lock")
	private final AtomicInteger size = new AtomicInteger(0);
	private final Object lock = new Object();

	public BlockingQueue<SignalInfo> put(K key, SignalInfo value) {

		synchronized (lock) {

			BlockingQueue<SignalInfo> prevList = map.get(key);
			BlockingQueue<SignalInfo> newList;
			if (prevList == null) {
				newList = new LinkedBlockingQueue<SignalInfo>();
				map.put(key, newList);
			} else {
				newList = prevList;
			}
			newList.add(value);
			size.incrementAndGet();
			return prevList;
		}
	}

	public int size() {
		return size.get();
	}

	/**
	 *	This method assumes that the queue returned won't be modified.  
	 *  Notice this could make the multimap's state inconsistent, because the size field would no longer reflect 
	 *  the real size. If needed this method could return a copy of the collection but this option was discarded for 
	 *  performance reasons. A more complete implementation would wrap the queue in another one which would throw an
	 *  exception when trying to modify it.
	 * 
	 * @return blockingQueue containing all the values associated to the key.
	 */
	public BlockingQueue<SignalInfo> get(K key) {
		synchronized (lock) {			
			return map.get(key);
		}
	}

	/**
	 *	This method assumes that the collection returned won't be modified.  
	 *  Notice this could make the multimap's state inconsistent, because the size field would no longer reflect 
	 *  the real size. If needed this method could return a copy of the collection but this option was discarded for 
	 *  performance reasons. A more complete implementation would wrap the collection in another one which would throw an
	 *  exception when trying to modify it.
	 * 
	 * @return collection of all values in the multimap.
	 */
	public Collection<BlockingQueue<SignalInfo>> values() {
		synchronized (lock) {
			return map.values();
		}
	}

	public SignalInfo pollList(K key) {

		synchronized (lock) {

			BlockingQueue<SignalInfo> list = map.get(key);
			if (list == null || list.isEmpty()) {
				return null;
			}
			size.decrementAndGet();
			return list.poll();
		}
	}

	public boolean isEmpty() {

		synchronized (lock) {

			for (BlockingQueue<SignalInfo> list : map.values()) {
				if (!list.isEmpty()) {
					return false;
				}
			}
			return true;
		}
	}

	public void clear() {

		synchronized (lock) {

			for (BlockingQueue<SignalInfo> list : map.values()) {
				list.clear();
			}
			map.clear();
			size.set(0);
		}
	}

	public SignalInfo remove(Signal value) {

		synchronized (lock) {

			for (BlockingQueue<SignalInfo> list : map.values()) {
				for (SignalInfo item : list) {
					if (item.getSignal().equals(value)) {
						size.decrementAndGet();
						list.remove(item);
						return item;
					}
				}
			}
			return null;
		}
	}
}
