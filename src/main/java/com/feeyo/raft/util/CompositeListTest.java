package com.feeyo.raft.util;


public class CompositeListTest {
	
	public static void main(String[] args) {
		//
		CompositeList<String> clist = new CompositeList<>();
		long t1 = System.currentTimeMillis();
		for(int i = 0; i < 3000000; i++) {
			clist.add("xx-" + i);
		}
		long t2 = System.currentTimeMillis();
		
		System.out.println( clist.get(0) );
		System.out.println( clist.get(700000) );
		
		//
		//
		for(int i = 0; i < 1000000; i++) {
			int idx = 2928399;
			clist.get(idx);
		}
		long t3 = System.currentTimeMillis();
		long diff1 = t2 -t1;
		long diff2 = t3 -t2;
		System.out.println(clist.size()  + ", diff1=" + diff1 + ", diff2=" + diff2 );
		
		clist.clear();
		System.out.println(clist.size() );
	}

}
