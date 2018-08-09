package tools;

public interface XSet<K, E extends K> {
	int size();

	boolean contains(K key);

	E get(K key);

	E put(E element);

	E remove(K key);

	void clear();
}
