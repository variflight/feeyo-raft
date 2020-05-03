package com.feeyo.raft.cli.util;

public class TextUtil {
	
	
	public static String toText(String[] columnNames, Object[][] data) {
		
		TextTable tt = new TextTable(columnNames, data);
		tt.setSeparatorPolicy(new LastRowSeparatorPolicy(data.length));

		TextTableRenderer ttRenderer = new TextTableRenderer(tt);
		return ttRenderer.render(2);
	}
	
	
	
	public static void main(String[] args) {
		
		String[] columnNames = { "X1", "X2", "X3", "X4", "X###" };
		
		Object[][] data = {
			    {"yyy1", "xx", "xx", 5, false},
			    {"yyy12", "xx", "xx", 44, true},
			    {"yyy123", "xx", "xx", 333, true},
			    {"yyy1234", "xx", "xx", 2222, true},
			    {"yyy12345", "xx", "xxx", 11111, false}
			};
		
		String txt = toText(columnNames, data);
		System.out.println( txt );
		System.out.println("\n");
		System.out.println("\n");
		
	}
	

}
