package com.snowflake.s3compatapitestsuite.options;

import org.apache.commons.cli.*;

/**
 * Parser to parse CLI arguments.
 */
public class CliParser {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    Options options;
    HelpFormatter formatter = new HelpFormatter();
    public CliParser(Options options) {
        this.options = options;
    }
    public CommandLine parse(String[] args) {
        try {
            cmd = parser.parse(options, args, /*stopAtNonOption=*/ true);
        } catch (ParseException e) {
            if (e instanceof UnrecognizedOptionException) {

            }
            formatter.printHelp(" optional arguments: ", options);
            throw new IllegalArgumentException(e);
        }
        return cmd;
    }
}
