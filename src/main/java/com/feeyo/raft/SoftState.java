package com.feeyo.raft;

/**
 * 软状态易变且不需要保存在WAL日志中的状态数据, 包括(集群leader、节点的当前状态)
 */
public class SoftState {
	private final long id;
	private final StateType state;
	
	public SoftState(long id, StateType state) {
		this.id = id;
		this.state = state;
	}

	public long getId() {
		return id;
	}

	public StateType getState() {
		return state;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (!(obj instanceof SoftState))
			return false;

		SoftState other = (SoftState) obj;
		return this.id == other.id && this.state == other.state;
	}
}
