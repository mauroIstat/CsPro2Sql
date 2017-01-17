
package cspro2sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.reader.DictionaryReader;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Main class to run the project:
 * 
 * CsPro2Sql -e schema -d DICTIONARY_FILE -s SCHEMA_NAME [options]
 * CsPro2Sql -e loader [-p PROPERTIES_FILE]
 * CsPro2Sql -e monitor -s SCHEMA_NAME [-p PROPERTIES_FILE] [-o OUTPUT_FILE]
 * 
 *  -a,--all                   transers all the records (modified time not considered)
 *  -d,--dictionary <arg>      path to the dictionary file
 *  -e,--engine <arg>          select engine: [loader|schema|monitor]
 *  -h,--help                  display help
 *  -o,--output <arg>          name of the output file containing the script
 *  -p,--properties <arg>      database.properties file (default resources/database.properties)
 *  -s,--schema <arg>          name of database schema
 *  -tp,--table-prefix <arg>   prefix of table names
 * 
 * @author Istat Cooperation Unit
 */
public class Main {

    public static void main(String[] args) {
        //Get command line options
        CsPro2SqlOptions opts = getCommandLineOptions(args);
        boolean error = false;

        if (opts.schemaEngine) {
            //Parse the dictionary
            Dictionary dictionary = null;
            try {
                dictionary = DictionaryReader.read(opts.dictFile, opts.tablePrefix);
            } catch (Exception ex) {
                opts.ps.close();
                opts.printHelp("Impossible to read dictionary file ("+ex.getMessage()+")");
            }
            //Execute the schema engine
            try {
                error = !SchemaEngine.execute(dictionary, opts.schema, opts.ps);
            } catch (Exception ex) {
                opts.printHelp("Impossible to create the datatabse schema ("+ex.getMessage()+")");
            } finally {
                opts.ps.close();
            }
        } else if (!opts.monitorEngine) {
            //Execute the loader engine
            error = !LoaderEngine.execute(opts.propertiesFile, opts.allRecords);
        } else if (opts.monitorEngine) {
            error = !MonitorEngine.execute(opts.propertiesFile, opts.schema, opts.ps);
        }
        if (error) System.exit(1);
    }

    private static CsPro2SqlOptions getCommandLineOptions(String[] args) {
        Options options = new Options();
        options.addOption("d", "dictionary", true, "path to the dictionary file");
        options.addOption("o", "output", true, "name of the output file containing the script");
        options.addOption("tp", "table-prefix", true, "prefix of table names");
        options.addOption("h", "help", false, "display help");
        options.addOption("s", "schema", true, "name of database schema");
        options.addOption("e", "engine", true, "select engine: [loader|schema|monitor]");
        options.addOption("p", "properties", true, "database.properties file (default resources/database.properties)");
        options.addOption("a", "all", false, "transers all the records (modified time not considered)");

        CsPro2SqlOptions opts = new CsPro2SqlOptions();
        opts.options = options;
        //Start parsing command line
        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("h") || args.length == 0) { //help option or empty option
                opts.printHelp();
            }

            if (!cmd.hasOption("e")) { //Loader engine option provided
                opts.printHelp("The engine type is mandatory!");
            }
            
            String engine = cmd.getOptionValue("e");
            if ("schema".equals(engine)) {
                opts.schemaEngine = true;
            } else if ("loader".equals(engine)) {
                opts.loaderEngine = true;
            } else if ("monitor".equals(engine)) {
                opts.monitorEngine = true;
            } else {
                opts.printHelp("Wrong engine type!");
            }

            if (opts.schemaEngine) { //Parse command line to get schema engine properties
                if (!cmd.hasOption("d")) {
                    opts.printHelp("The input dictionary file is mandatory!");
                } else if (!cmd.hasOption("s")) {
                    opts.printHelp("The database schema is mandatory!");
                }
                
                opts.dictFile = cmd.getOptionValue("d");
                opts.schema = cmd.getOptionValue("s");

                if (cmd.hasOption("tp")) { //Table name prefix provided
                    opts.tablePrefix = cmd.getOptionValue("tp");
                }

                if (cmd.hasOption("o")) { //Output file name provided
                    opts.ps = new PrintStream(cmd.getOptionValue("o"),"UTF-8");
                }
            } else if (opts.loaderEngine) {
                if (cmd.hasOption("p")) {
                    opts.propertiesFile = cmd.getOptionValue("p");
                }
                if (cmd.hasOption("a")) {
                    opts.allRecords = true;
                }
            } else if (opts.monitorEngine) {
                if (!cmd.hasOption("s")) {
                    opts.printHelp("The database schema is mandatory!");
                }

                opts.schema = cmd.getOptionValue("s");

                if (cmd.hasOption("p")) {
                    opts.propertiesFile = cmd.getOptionValue("p");
                }
                if (cmd.hasOption("o")) { //Output file name provided
                    opts.ps = new PrintStream(cmd.getOptionValue("o"),"UTF-8");
                }
            }
        } catch (ParseException | FileNotFoundException | UnsupportedEncodingException e) {
            opts.printHelp();
        }
        //End parsing command line

        return opts;
    }

    private static class CsPro2SqlOptions {
        boolean schemaEngine = false; //used to switch between Loader and Schema engines
        boolean loaderEngine = false;
        boolean monitorEngine = false;
        String dictFile = "";
        String schema = "";
        String tablePrefix = "";
        String propertiesFile = "resources/database.properties";
        PrintStream ps = System.out;
        Options options;
        boolean allRecords = false;
        
        void printHelp() {
            printHelp(null);
        }
        
        void printHelp(String errMessage) {
            HelpFormatter formatter = new HelpFormatter();
            if (errMessage != null) {
                System.err.println(errMessage);
            }
            formatter.printHelp("\n\n"
                    + "CsPro2Sql -e schema -d DICTIONARY_FILE -s SCHEMA_NAME [options]\n"
                    + "CsPro2Sql -e loader [-p PROPERTIES_FILE]\n"
                    + "CsPro2Sql -e monitor -s SCHEMA_NAME [-p PROPERTIES_FILE] [-o OUTPUT_FILE]\n"
                    + "\n", options);
            if (errMessage != null) {
                System.exit(1);
            } else {
                System.exit(0);
            }
        }
    }

}
