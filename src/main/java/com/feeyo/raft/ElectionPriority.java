package com.feeyo.raft;

public class ElectionPriority {
	//
	public static final int Disabled = -1; // priority -1 表示此节点禁用了优先级选择功能
	public static final int NotElected = 0; // priority 0 节点永远不会参与选举
	public static final int MinValue = 1; // priority 1 优先权选择的最小值
}
