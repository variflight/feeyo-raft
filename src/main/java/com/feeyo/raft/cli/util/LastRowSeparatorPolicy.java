package com.feeyo.raft.cli.util;

public class LastRowSeparatorPolicy implements ISeparatorPolicy {

	private int rowCount;

	public LastRowSeparatorPolicy(int rowCount) {
		this.rowCount = rowCount;
	}

	@Override
	public boolean hasSeparatorAt(int row) {
		if (row == rowCount - 1)
			return true;
		return false;
	}
}