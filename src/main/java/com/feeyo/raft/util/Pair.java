package com.feeyo.raft.util;

import com.google.common.base.Objects;

/**
 * Container to ease passing around a tuple of two objects. This object provides
 * a sensible implementation of equals(), returning true if equals() is true on
 * each of the contained objects.
 */
public class Pair<F, S> {
	
	public final F first;
	public final S second;

	public Pair(F first, S second) {
		this.first = first;
		this.second = second;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Pair)) {
			return false;
		}
		Pair<?, ?> p = (Pair<?, ?>) o;
		return Objects.equal(p.first, first) && Objects.equal(p.second, second);
	}


	@Override
	public int hashCode() {
		return (first == null ? 0 : first.hashCode()) ^ (second == null ? 0 : second.hashCode());
	}

	//
	public static <F, S> Pair<F, S> create(F first, S second) {
		return new Pair<F, S>(first, second);
	}

}
