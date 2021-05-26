package com.feeyo.raft.cli;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import jline.console.ConsoleReader;

//
public class CliMain extends AbstractCli {
	
	public static final Pattern ARGS_PATTERN = Pattern.compile("\\s*([^\"\']\\S*|\"[^\"]*\"|'[^']*')\\s*");
	public static final Pattern QUOTED_PATTERN = Pattern.compile("^([\'\"])(.*)(\\1)$");

	//
	public static void main(String[] args)  {
		
		Options options = createOptions();
		HelpFormatter hf = new HelpFormatter();
		hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
		
		//
		CommandLine commandLine = null;
		CommandLineParser parser = new DefaultParser();
		//
		init();
		args = removePortArgs(args);
		//
		try {
			//
			commandLine = parser.parse(options, args);
		
		} catch (ParseException e) {
			System.out.println("Require more params input, eg. ./raftCli.sh -h xxx.xxx.xxx.xxx -p xxxx");
			System.out.println("For more information, please check the following hint.");
			hf.printHelp(RAFT_CLI_PREFIX, options, true);
			return;
		}
		
		ConsoleReader reader = null;
		try {
			reader = new ConsoleReader();
			reader.setExpandEvents(false);
			
			String s;
			try {
				host = checkRequiredArg(HOST_ARGS, HOST_NAME, commandLine, false, host);
				port = checkRequiredArg(PORT_ARGS, PORT_NAME, commandLine, false, port);
			} catch (CliException e) {
				return;
			}
			
			displayLogo();
			System.out.println(RAFT_CLI_PREFIX + "> login successfully");
			
			while (true) {
				s = reader.readLine(RAFT_CLI_PREFIX + "> ", null);
				if (s == null) {
					continue;
				} else {
					
					Matcher matcher = ARGS_PATTERN.matcher(s);
					List<String> cmdArgs = new LinkedList<String>();
					while (matcher.find()) {
						String value = matcher.group(1);
						if (QUOTED_PATTERN.matcher(value).matches()) {
							// Strip off the surrounding quotes
							value = value.substring(1, value.length() - 1);
						}
						cmdArgs.add(value);
					}
					
					//
					if (cmdArgs.isEmpty()) {
						continue;
					}
					
					String cmd = cmdArgs.get(0);
					if (cmd != null && !cmd.trim().equals("")) {
						OPERATION_RESULT result = handleInputInputCmd( cmd, cmdArgs );
						switch (result) {
						case RETURN_OPER:
							return;
						case CONTINUE_OPER:
							continue;
						default:
							break;
						}
					}
				}
			}
			
		} catch (Exception e) {
			System.out.println(RAFT_CLI_PREFIX + "> exit client with error " + e.getMessage());
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}
}
