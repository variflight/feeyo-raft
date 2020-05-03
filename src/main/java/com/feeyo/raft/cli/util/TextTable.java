package com.feeyo.raft.cli.util;

/**
 * 
 * @author zhuam
 *
 */
public class TextTable {
	
	protected ISeparatorPolicy separatorPolicy;
	
	private String[] columnNames;
	private Object[][] data;
	
	public TextTable(String[] columnNames, Object[][] data) {
		this.columnNames = columnNames;
		this.data = data;
	}

	public String[] getColumnNames() {
		return columnNames;
	}

	public Object[][] getData() {
		return data;
	}
	
	
	public int getRowCount() {
		if ( data != null )
			return data.length;
		return 0;
	}
	
	public int getColumnCount() {
		if ( columnNames != null )
			return columnNames.length;
		return 0;
	}
	
	public String getColumnName(int i) {
		if ( columnNames != null && i >= 0 && i < columnNames.length)
			return columnNames[i];
		return "";
	}
	
	public Object getValueAt(int row, int column) {
		if ( data != null )
			return data[row][column];
		return null;
	}
	
	public void setSeparatorPolicy(ISeparatorPolicy separatorPolicy) {
		this.separatorPolicy = separatorPolicy;
	}
	
	protected boolean hasSeparatorAt(int row) {
		if( separatorPolicy != null )
			return separatorPolicy.hasSeparatorAt(row);
		
		return false;
	}

}
