package com.feeyo.raft.cli;

public class CliException extends Exception {

	private static final long serialVersionUID = 1L;

    public CliException(String message) {
        super(message);
    }
    
    public CliException(Throwable cause) {
        super(cause);
    }

    public CliException(String message, Throwable cause) {
        super(message, cause);
    }
}