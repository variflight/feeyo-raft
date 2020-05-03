package com.feeyo.raft.cli.util;

import org.apache.commons.lang3.StringUtils;


/**
 * 
 * @author zhuam
 *
 */
public class TextTableRenderer {
	
	protected String[] formats;
	protected int[] lengths;
	
	protected TextTable textTable;
	
	public TextTableRenderer(TextTable textTable) {
		this.textTable = textTable;
	}

	//
	public String render(int padding) {
		
		StringBuffer stringBuffer = new StringBuffer();
	
		// 解析列长度, 查找每列中字符串的最大长度
		lengths = new int[textTable.getColumnCount()];
		for (int col = 0; col < textTable.getColumnCount(); col++) {
			for (int row = 0; row < textTable.getRowCount(); row++) {
				Object val = textTable.getValueAt(row, col);
				String valStr = String.valueOf(val);
				if ( val == null) {
					valStr = "";
				}
				int maxColumnLength =  Math.max( valStr.length(), lengths[col] );
				lengths[col] = maxColumnLength;
			}
		}
		
		// row分割线
		StringBuilder sepSb = new StringBuilder();
		for (int j = 0; j < textTable.getColumnCount(); j++) {
			if (j == 0) {
				sepSb.append("|");
			}
			lengths[j] = Math.max( textTable.getColumnName(j).length(), lengths[j]);
			sepSb.append(StringUtils.repeat("-", lengths[j] + 1));
			sepSb.append("|");
		}
		String separator = sepSb.toString();


		// 为每列生成 format的字符串 & 计算totalLength
		int totLength = 0;
		formats = new String[lengths.length];
		for (int i = 0; i < lengths.length; i++) {
			StringBuilder sb = new StringBuilder();
			if (i == 0) {
				sb.append("|");
			}
			sb.append(" %1$-");
			sb.append(lengths[i]);
			sb.append("s|");
			sb.append(i + 1 == lengths.length ? "\n" : "");
			formats[i] = sb.toString();
			totLength += lengths[i];
		}

		//
		String paddingStr = StringUtils.repeat(" ", padding);
		String headerStartSep = StringUtils.repeat("_", totLength + textTable.getColumnCount() * 2);
		stringBuffer.append( paddingStr );
		stringBuffer.append( headerStartSep ).append("\r\n");
		
		stringBuffer.append( paddingStr );
		for (int j = 0; j < textTable.getColumnCount(); j++) {
			stringBuffer.append( String.format(formats[j], textTable.getColumnName(j) ) );
		}

		String headerSep = StringUtils.repeat("=", totLength + textTable.getColumnCount() * 2 - 1);
		stringBuffer.append( paddingStr );
		stringBuffer.append( "|" );
		stringBuffer.append( headerSep );
		stringBuffer.append( "|" );
		stringBuffer.append("\r\n");

		//
		for (int i = 0; i < textTable.getRowCount(); i++) {
			
			stringBuffer.append(paddingStr);
			
			// 输出 value
			for (int j = 0; j < textTable.getColumnCount(); j++) {
				Object val = textTable.getValueAt(i, j);
				if (val == null) {
					val = "";
				}
				stringBuffer.append( String.format(formats[j], val ) );
			}
			
			//
			if ( textTable.hasSeparatorAt(i) ) {
				stringBuffer.append(paddingStr);
				stringBuffer.append(separator);
				stringBuffer.append("\r\n");
			}
		}
		return stringBuffer.toString();
	}

}
