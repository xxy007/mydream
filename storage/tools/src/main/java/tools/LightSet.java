package tools;

import org.apache.log4j.Logger;

@SuppressWarnings("unchecked")
public class LightSet<K, E extends K> implements XSet<K, E> {
	private LinkedElement[] entries;

	private int hashMask;

	private int size;

	private static Logger logger = Logger.getLogger(LightSet.class);

	public LightSet(int capacity) {
		// namenode可用内存的2%
		entries = new LinkedElement[capacity];
		hashMask = capacity - 1;
	}

	public int size() {
		return size;
	}

	public boolean contains(K key) {
		return (get(key) != null);
	}

	public E get(K key) {
		if (key == null) {
			throw new NullPointerException("key == null");
		}
		final int index = getIndex(key);
		for (LinkedElement e = entries[index]; e != null; e = e.getNext()) {
			if (e.equals(key)) {
				return (E) e;
			}
		}
		return null;
	}

	public E put(E element) {
		if (element == null || !(element instanceof LinkedElement)) {
			throw new NullPointerException("Null element is not supported or element is not instanceof LinkedElement.");
		}
		LinkedElement e = (LinkedElement) element;
		final int index = getIndex(element);

		final E existing = remove(index, element);

		e.setNext(entries[index]);
		entries[index] = e;

		size++;

		return existing;
	}

	public E remove(K key) {
		if (key == null) {
			throw new NullPointerException("key == null");
		}
		final int index = getIndex(key);
		for (LinkedElement e = entries[index]; e != null; e = e.getNext()) {
			if (e.equals(key)) {
				return (E) e;
			}
		}
		return null;
	}

	private E remove(int index, K key) {
		if (entries[index] == null) {
			return null;
		} else if (entries[index].equals(key)) {
			size--;
			final LinkedElement e = entries[index];
			entries[index] = e.getNext();
			e.setNext(null);
			return (E) e;
		} else {
			LinkedElement prev = entries[index];
			for (LinkedElement curr = prev.getNext(); curr != null;) {
				if (curr.equals(key)) {
					size--;
					prev.setNext(curr.getNext());
					curr.setNext(null);
					return (E) curr;
				} else {
					prev = curr;
					curr = curr.getNext();
				}
			}
			return null;
		}
	}

	public void clear() {

	}

	private int getIndex(K key) {
		return key.hashCode() & hashMask;
	}

	/**
	 * 根据最大可用内存，计算容器大小，即根据内存量(maxMem*percentage)来计算数组中最多存储对象个数N，将N作为容器大小
	 * @param percentage
	 * @param unit
	 * @return
	 */
	public static int computeCapacity(double percentage) {
		long maxMemory = Runtime.getRuntime().maxMemory();
		if (percentage > 100.0 || percentage < 0.0) {
			throw new IllegalArgumentException("Percentage " + percentage + " must be greater than or equal to 0 "
					+ " and less than or equal to 100");
		}
		if (maxMemory < 0) {
			throw new IllegalArgumentException("Memory " + maxMemory + " must be greater than or equal to 0");
		}
		if (percentage == 0.0 || maxMemory == 0) {
			return 0;
		}
		//获取是32还是64系统
		final String vmBit = System.getProperty("sun.arch.data.model");

		// Percentage of max memory
		final double percentDivisor = 100.0 / percentage;
		final double percentMemory = maxMemory / percentDivisor;

		// 取percentMemory得自然对数。0.5为了四舍五入
		final int e1 = (int) (Math.log(percentMemory) / Math.log(2.0) + 0.5);
		//32和64位系统，对象占用内存不同，32位4字节，64为8字节，所以64需要多除一个2
		final int e2 = e1 - ("32".equals(vmBit) ? 2 : 3);
		final int exponent = e2 < 0 ? 0 : e2 > 30 ? 30 : e2;	//最多1G，2的30次方为1G
		final int c = 1 << exponent;
		return c;
	}
}
