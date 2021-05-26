package com.feeyo.raft.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * 封装n个 object array 避免 ArrayList grow & copy
 * 
 * @author zhuam
 *
 * @param <E>
 */ 
public class CompositeList<E> implements List<E> {

	private transient Node<E> first = null; 	// 头节点
	private transient Node<E> last = null; 		// 尾节点
	private transient int nodeSz = 0; 			// 链表大小
	//
	private transient int size = 0;				// 大小
	
	@Override
	public boolean add(E e) {
		if( e == null ) 
			return false;
		//
        Node<E> l = last;
        if (l == null) {
        	Node<E> node = new Node<>(l, null);
        	node.add(e);
        	//
        	last = first = node;
        	nodeSz++;
        } else {
        	if ( !l.ensureCapacity() ) {
        		Node<E> node = new Node<>(l, null);
        		node.add(e);
        		l.next = node;
        		//
        		last = node;
        		nodeSz++;
        	} else  {
        		l.add(e);
        	}
        }
        size++;
		return true;
	}
	

	@Override
	public boolean addAll(Collection<? extends E> c) {
		for(E e: c)
			add( e );
		return true;
	}
	
	/**
	 * 获取结点
	 * @param index 节点索引
	 * @return 返回节点
	 */
	private Node<E> getNode(int index) {
		// 如果是大小的一半之前,正序查找,否则倒序查找,提高效率
		if (index < (nodeSz >> 1)) { // 除2
			Node<E> f = first;
			for (int i = 0; i < index; i++)
				f = f.next;
			return f;
		} else {
			// TODO: 长度size 从 1开始， index 从 0开始，所以此处倒序查找需要 再减1
			Node<E> l = last;
			for (int i = nodeSz - 1; i > index; i--)
				l = l.prev;
			return l;
		}
	}
	
	@Override
	public E get(int index) {
		int nIndex = index / Node.initialCapacity;
		Node<E> node = getNode( nIndex );
		if ( node == null )
			return null;
		//
		int relativeIndex = index - (nIndex * Node.initialCapacity);
		return node.get( relativeIndex );
	}

	@Override
	public void clear() {

        for (Node<E> x = first; x != null; ) {
            Node<E> next = x.next;
            x.clear();
            x.next = null;
            x.prev = null;
            x = next;
        }
        //
        first = last = null;
        nodeSz = 0;
        size = 0;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public boolean isEmpty() {
		return size == 0;
	}

	@Override
	public boolean contains(Object o) {
		throw new UnsupportedOperationException("contains(o) is not supported");
	}

	@Override
	public Iterator<E> iterator() {
		throw new UnsupportedOperationException("iterator is not supported");
	}

	@Override
	public Object[] toArray() {
		throw new UnsupportedOperationException("toArray is not supported");
	}

	@SuppressWarnings("hiding")
	@Override
	public <E> E[] toArray(E[] a) {
		throw new UnsupportedOperationException("toArray(a) is not supported");
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException("remove(o) is not supported");
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException("containsAll(c) is not supported");
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		throw new UnsupportedOperationException("addAll(index, c) is not supported");
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("removeAll(c) is not supported");
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("retainAll(c) is not supported");
	}

	@Override
	public E set(int index, E element) {
		throw new UnsupportedOperationException("set(index, element) is not supported");
	}

	@Override
	public void add(int index, E element) {
		throw new UnsupportedOperationException("add(index, element) is not supported");
	}

	@Override
	public E remove(int index) {
		throw new UnsupportedOperationException("remove(index) is not supported");
	}

	@Override
	public int indexOf(Object o) {
		throw new UnsupportedOperationException("indexOf(o) is not supported");
	}

	@Override
	public int lastIndexOf(Object o) {
		throw new UnsupportedOperationException("lastIndexOf(o) is not supported");
	}

	@Override
	public ListIterator<E> listIterator() {
		throw new UnsupportedOperationException("listIterator is not supported");
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		throw new UnsupportedOperationException("listIterator(index) is not supported");
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException("subList(fromIndex, toIndex) is not supported");
	}
	
	//
	// 节点
	static class Node<E> {
		
		public static final int initialCapacity = 1024;
		//
		private Node<E> prev = null;
		private Node<E> next = null;
		//
		private transient Object[] elementData;
		private int wroteIdx;
		private int size;
		//
		public Node(Node<E> prev, Node<E> next) {
			this.prev = prev;
			this.next = next;
			//
			this.elementData = new Object[ initialCapacity ];
			this.wroteIdx = 0;
			this.size = initialCapacity;
		}
		
		public void add(E e) {
			elementData[ wroteIdx++ ] = e;
		}
		
		@SuppressWarnings("unchecked")
		public E get(int index) {
			return (E) elementData[index];
		}
		
		public boolean ensureCapacity() {
			if ( wroteIdx < size )
				return true;
			return false;
		}
		
		public void clear() {
			 // clear to let GC do its work
	        for (int i = 0; i < size; i++)
	            elementData[i] = null;
	        //
	        elementData = null;
	        wroteIdx = 0;
	        size = 0;
		}
	}
}