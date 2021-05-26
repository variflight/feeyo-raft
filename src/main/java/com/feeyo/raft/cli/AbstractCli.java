package com.feeyo.raft.cli;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.ArrayUtils;

import com.feeyo.raft.util.Versions;

public abstract class AbstractCli {
	
	protected static final String RAFT_CLI_PREFIX = "Raft";
	
	//
	protected static final String HOST_ARGS = "h";
	protected static final String HOST_NAME = "host";

	protected static final String HELP_ARGS = "help";

	protected static final String PORT_ARGS = "p";
	protected static final String PORT_NAME = "port";
	
	
	protected static String host = "127.0.0.1";
	protected static String port = "8000";

	
	protected static final String QUIT_COMMAND = "quit";
	protected static final String EXIT_COMMAND = "exit";
	//
	protected static final String HELP_COMMAND = "help";
	
	//
	protected static final String ADD_NODE_COMMAND = "addNode";
	protected static final String REMOVE_NODE_COMMAND  = "removeNode";
	protected static final String TRANSFER_LEADER_COMMAND = "transferLeader";
	
	protected static final String GET_NODES_COMMAND = "getNodes";
	protected static final String GET_NODE_PRS_COMMAND = "getNodePrs";
	
	//
	protected static final String VERSION_COMMAND = "version";
	
	//
	protected static final int MAX_HELP_CONSOLE_WIDTH = 88;
	protected static final String SCRIPT_HINT = "./raftCli.sh";
	
	//
	protected static Set<String> keywordSet = new HashSet<>();
	
	protected static void init(){
		keywordSet.add("-"+HOST_ARGS);
		keywordSet.add("-"+HELP_ARGS);
		keywordSet.add("-"+PORT_ARGS);
	}
	
	
	protected static Options createOptions() {
		Options options = new Options();
		Option help = new Option(HELP_ARGS, false, "Display help information(optional)");
		help.setRequired(false);
		options.addOption(help);

		Option host = Option.builder(HOST_ARGS).argName(HOST_NAME).hasArg().desc("Host Name (optional, default 127.0.0.1)").build();
		options.addOption(host);

		Option port = Option.builder(PORT_ARGS).argName(PORT_NAME).hasArg().desc("Port (optional, default 8000)").build();
		options.addOption(port);

		return options;
	}
	
	protected static String[] removePortArgs(String[] args) {
		int index = -1;
		for(int i = 0; i < args.length; i++){
			if(args[i].equals("-"+PORT_ARGS)){
				index = i;
				break;
			}
		}
		if(index >= 0){
			if((index+1 >= args.length) || (index+1 < args.length && keywordSet.contains(args[index+1]))){
				return ArrayUtils.remove(args, index);
			}
		}
		return args;
	}
	
	protected static String checkRequiredArg(String arg, String name, CommandLine commandLine, boolean isRequired, String defaultValue) 
			throws CliException {
		
		String str = commandLine.getOptionValue(arg);
		if (str == null) {
			if(isRequired) {
				String msg = String.format("%s: Required values for option '%s' not provided", RAFT_CLI_PREFIX, name);
				ScreenPrinter.println(msg);
				ScreenPrinter.println("Use -help for more information");
				throw new CliException(msg);
			} else if (defaultValue == null){
				String msg = String.format("%s: Required values for option '%s' is null.", RAFT_CLI_PREFIX, name);
				throw new CliException(msg);
			} else {
				return defaultValue;
			}
		}
		return str;
	}
	
	protected static void displayLogo(){
		ScreenPrinter.println("RAFT 1.0.20200826 beta");
	}
	
	//
	@SuppressWarnings("unchecked")
	protected static OPERATION_RESULT handleInputInputCmd(String cmd, List<String> cmdArgs){
		//
		String specialCmd = cmd.toLowerCase().trim();
		if (specialCmd.equals(QUIT_COMMAND) || specialCmd.equals(EXIT_COMMAND)) {
			ScreenPrinter.println(specialCmd + " normally");
			return OPERATION_RESULT.RETURN_OPER;
		}
		
		if(specialCmd.equals(HELP_COMMAND) ) {
			ScreenPrinter.println("Require more params input, eg. ./raftCli.sh -h xxx.xxx.xxx.xxx -p xxxx");
			ScreenPrinter.println("For more information, please check the following hint.");
			ScreenPrinter.println("addNode [id][ip][port][learner]" );
			ScreenPrinter.println("removeNode [id]" );
			ScreenPrinter.println("transferLeader [id]" );
			ScreenPrinter.println("getNodes" );
			ScreenPrinter.println("getNodePrs" );

			return OPERATION_RESULT.CONTINUE_OPER;
		}
		
		///
		//
		if (specialCmd.startsWith( ADD_NODE_COMMAND.toLowerCase() )) {
			if ( cmdArgs.size() < 5 ) {
				ScreenPrinter.println("args error " );
				return OPERATION_RESULT.CONTINUE_OPER;
			}
			
			try {
				String id = cmdArgs.get(1);
				String ip = cmdArgs.get(2);
				String port = cmdArgs.get(3);
				String isLearner = cmdArgs.get(4);
				String hostAndPort = host + ":" + port;
				//
				Object[] data = CliRpc.addNode(hostAndPort, id, ip, port, isLearner);
				ScreenPrinter.output((List<List<String>>)data[0], (List<Integer>)data[1]);
				//
			} catch (CliException e) {
				ScreenPrinter.println("Failed to add node because: " + e.getMessage());
			}
			return OPERATION_RESULT.CONTINUE_OPER;
		}

		///
		//
		if(specialCmd.startsWith( REMOVE_NODE_COMMAND.toLowerCase() )){
			if ( cmdArgs.size() < 2 ) {
				ScreenPrinter.println("args error " );
				return OPERATION_RESULT.CONTINUE_OPER;
			}
			
			try {
				String id = cmdArgs.get(1);
				String hostAndPort = host + ":" + port;
				//
				Object[] data = CliRpc.removeNode(hostAndPort, id);
				ScreenPrinter.output((List<List<String>>)data[0], (List<Integer>)data[1]);
				//
			} catch (CliException e) {
				ScreenPrinter.println("Failed to remove node because: " + e.getMessage());
			}
			return OPERATION_RESULT.CONTINUE_OPER;
		}

		///
		//
		if(specialCmd.startsWith( TRANSFER_LEADER_COMMAND.toLowerCase() )){
			if ( cmdArgs.size() < 2 ) {
				ScreenPrinter.println("args error " );
				return OPERATION_RESULT.CONTINUE_OPER;
			}
			
			try {
				String id = cmdArgs.get(1);
				String hostAndPort = host + ":" + port;
				//
				Object[] data = CliRpc.transferLeader(hostAndPort, id);
				ScreenPrinter.output((List<List<String>>)data[0], (List<Integer>)data[1]);
				//
			} catch (CliException e) {
				ScreenPrinter.println("Failed to transfer leader because: " + e.getMessage());
			}
			return OPERATION_RESULT.CONTINUE_OPER;
			
		}
		
		///
		//
		if(specialCmd.startsWith( GET_NODES_COMMAND.toLowerCase() )){
			try {
				String hostAndPort = host + ":" + port;
				//
				Object[] data = CliRpc.getNodes(hostAndPort);
				ScreenPrinter.output((List<List<String>>)data[0], (List<Integer>)data[1]);
				//
			} catch (CliException e) {
				ScreenPrinter.println("Failed to get nodes because: " + e.getMessage());
			}
			return OPERATION_RESULT.CONTINUE_OPER;
		}
		
		///
		//
		if(specialCmd.startsWith( GET_NODE_PRS_COMMAND.toLowerCase() )) {
			try {
				String hostAndPort = host + ":" + port;
				//
				Object[] data = CliRpc.getNodePrs(hostAndPort);
				ScreenPrinter.output((List<List<String>>)data[0], (List<Integer>)data[1]);
				//
			} catch (CliException e) {
				ScreenPrinter.println("Failed to get node prs because: " + e.getMessage());
			}
			return OPERATION_RESULT.CONTINUE_OPER;
		}
		
		if( specialCmd.startsWith( VERSION_COMMAND.toLowerCase()) ) {
			ScreenPrinter.println( Versions.RAFT_VERSION );
			return OPERATION_RESULT.CONTINUE_OPER;
		}
		return OPERATION_RESULT.NO_OPER;
	}

	enum OPERATION_RESULT{
		RETURN_OPER, CONTINUE_OPER, NO_OPER
	}
	
}