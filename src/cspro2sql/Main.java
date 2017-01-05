
package cspro2sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.reader.DictionaryReader;
import java.io.FileNotFoundException;
import java.io.PrintStream;
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
 * 
 *  -d,--dictionary <arg>      path to the dictionary file
 *  -e,--engine <arg>          select engine: [loader|schema]
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
                SchemaEngine.execute(dictionary, opts.schema, opts.ps);
            } catch (Exception ex) {
                opts.printHelp("Impossible to create the datatabse schema ("+ex.getMessage()+")");
            } finally {
                opts.ps.close();
            }
        } else {
            //Execute the loader engine
            System.out.println("Starting data transfer from CsPro to MySql...");
            LoaderEngine.execute(opts.propertiesFile);
            System.out.println("Data transfer completed!");
        }
    }

    private static CsPro2SqlOptions getCommandLineOptions(String[] args) {
        Options options = new Options();
        options.addOption("d", "dictionary", true, "path to the dictionary file");
        options.addOption("o", "output", true, "name of the output file containing the script");
        options.addOption("tp", "table-prefix", true, "prefix of table names");
        options.addOption("h", "help", false, "display help");
        options.addOption("s", "schema", true, "name of database schema");
        options.addOption("e", "engine", true, "select engine: [loader|schema]");
        options.addOption("p", "properties", true, "database.properties file (default resources/database.properties)");

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
                opts.schemaEngine = false;
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
                    opts.ps = new PrintStream(cmd.getOptionValue("o"));
                }
            } else {
                if (cmd.hasOption("p")) { //Output file name provided
                    opts.propertiesFile = cmd.getOptionValue("p");
                }
            }
        } catch (ParseException | FileNotFoundException e) {
            opts.printHelp();
        }
        //End parsing command line

        return opts;
    }

    private static class CsPro2SqlOptions {
        boolean schemaEngine; //used to switch between Loader and Schema engines
        String dictFile = "";
        String schema = "";
        String tablePrefix = "";
        String propertiesFile = "resources/database.properties";
        PrintStream ps = System.out;
        Options options;
        
        void printHelp() {
            printHelp(null);
        }
        
        void printHelp(String errMessage) {
            HelpFormatter formatter = new HelpFormatter();
            if (errMessage != null) {
                System.err.println(errMessage);
            }
            formatter.printHelp("\n\nCsPro2Sql -e schema -d DICTIONARY_FILE -s SCHEMA_NAME [options]\nCsPro2Sql -e loader [-p PROPERTIES_FILE]\n\n", options);
            if (errMessage != null) {
                System.exit(1);
            } else {
                System.exit(0);
            }
        }
    }

}
