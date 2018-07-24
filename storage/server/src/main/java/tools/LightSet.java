package tools;

public class LightSet<K, E extends K> implements XSet<K, E> {
	private LinkedElement[] entries;

	private int hashMask;

	private int size;

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
	
	public static int computeCapacity(int percentage) {
		long totalMem = Runtime.getRuntime().totalMemory();
		return (int) ((totalMem*percentage)/100);
	}
	
}
