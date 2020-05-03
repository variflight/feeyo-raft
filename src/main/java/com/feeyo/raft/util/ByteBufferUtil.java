package com.feeyo.raft.util;

import java.nio.ByteBuffer;

public class ByteBufferUtil {
    private static final byte NEGATIVE = '-';
    private static final byte ZERO = '0';

    // 结果类似于 11 -> "11".getBytes(), 不产生任何其他对象
    public static void writeIntAsBytes(ByteBuffer byteBuffer, int number, int numberByteCount) {
        if (number == 0) {
            byteBuffer.put(ZERO);
        } else if (number > 0) {
            for (int i = 0; i < numberByteCount; i++) {
                int perNumber = number / (int) Math.pow(10, numberByteCount - i - 1);
                number = number % (int) Math.pow(10, numberByteCount - i - 1);
                byteBuffer.put((byte) ((perNumber + 48) & 0xFF));
            }
        } else {
            byteBuffer.put(NEGATIVE);
            number= - number;
            for (int i = 0; i < numberByteCount - 1; i++) {
                int perNumber = number / (int) Math.pow(10, numberByteCount - 2 - i);
                number = number % (int) Math.pow(10, numberByteCount - 2  - i);
                byteBuffer.put((byte) ((perNumber + 48) & 0xFF));
            }
        }
    }

    public static int numberByteCount(int number) {
        if (number == 0) {
            return 1;
        } else if (number > 0) {
            return (int) Math.log10(number) + 1;
        } else {
            return (int) Math.log10(-number) + 2;
        }
    }

    // private static void printIntAsBytes(int number, int numberByteCount) {
    //     char[] chars;
    //     if (number == 0) {
    //         chars = new char[] {'0'};
    //     } else if (number > 0) {
    //         chars = new char[numberByteCount];
    //         for (int i = 0; i < numberByteCount; i++) {
    //             int perNumber = number / (int) Math.pow(10, numberByteCount - i - 1);
    //             number = number % (int) Math.pow(10, numberByteCount - i - 1);
    //             chars[i] = (char) (perNumber + 48);
    //         }
    //     } else {
    //         chars = new char[numberByteCount];
    //         chars[0] = NEGATIVE;
    //         number= - number;
    //         for (int i = 0; i < numberByteCount - 1; i++) {
    //             int perNumber = number / (int) Math.pow(10, numberByteCount - 2 - i);
    //             number = number % (int) Math.pow(10, numberByteCount - 2  - i);
    //             chars[i + 1] = (char) (perNumber + 48);
    //         }
    //     }
    //     System.out.println(Arrays.toString(chars));
    // }
    //
    // public static void main(String[] args) {
    //     test(885);
    //     test(-4);
    //     test(0);
    //     test(3);
    //     test(10);
    //     test(-57773);
    //     test(1000);
    //     test(8844445);
    // }
    //
    // private static void test(int test) {
    //     int count = numberByteCount(test);
    //     System.out.print("count is " + count + "\t");
    //     printIntAsBytes(test, count);
    // }
}