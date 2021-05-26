package com.feeyo.raft.cli;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class ScreenPrinter {
	
	private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
	
	//
	public static void main(String[] args) {
		//
		List<List<String>> lists = new ArrayList<>();
		List<Integer> maxSizeList = new ArrayList<>();
		//
		int columnCount = 3;
		for(int i = 0; i < columnCount; i++) {
			//
			List<String> column_list = new ArrayList<>();
			for(int j = 0; j < 10; j++) {
				String str = "iiiii-" + i + "-jjjjj-" + j;
				column_list.add(str);
			}
			//
			lists.add( column_list );
			maxSizeList.add(20);
		}
		//
		output(lists, maxSizeList);
	}

	//
	public static void output(List<List<String>> lists, List<Integer> maxSizeList) {
		printBlockLine(maxSizeList);
		printRow(lists, 0, maxSizeList);
		printBlockLine(maxSizeList);
		for (int i = 1; i < lists.get(0).size(); i++) {
			printRow(lists, i, maxSizeList);
		}
		printBlockLine(maxSizeList);
	}

	private static void printBlockLine(List<Integer> maxSizeList) {
		StringBuilder blockLine = new StringBuilder();
		for (Integer integer : maxSizeList) {
			blockLine.append("+").append(StringUtils.repeat("-", integer));
		}
		blockLine.append("+");
		println(blockLine.toString());
	}

	private static void printRow(List<List<String>> lists, int i, List<Integer> maxSizeList) {
		printf("|");
		for (int j = 0; j < maxSizeList.size(); j++) {
			printf("%" + maxSizeList.get(j) + "s|", lists.get(j).get(i));
		}
		println();
	}

	public static void printf(String format, Object... args) {
		SCREEN_PRINTER.printf(format, args);
	}
	
	public static void print(String msg) {
		SCREEN_PRINTER.print(msg);
	}

	public static void println() {
		SCREEN_PRINTER.println();
	}

	public static void println(String msg) {
		SCREEN_PRINTER.println(msg);
	}
}