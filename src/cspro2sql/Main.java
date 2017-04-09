package cspro2sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.reader.DictionaryReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Copyright 2017 ISTAT
 *
 * Licensed under the EUPL, Version 1.1 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence. You may
 * obtain a copy of the Licence at:
 *
 * http://ec.europa.eu/idabc/eupl5
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * Licence for the specific language governing permissions and limitations under
 * the Licence.
 *
 * @author Guido Drovandi <drovandi @ istat.it>
 * @author Mauro Bruno <mbruno @ istat.it>
 * @version 0.9.1
 */
public class Main {

    private static final String VERSION = "0.9.4";

    public static void main(String[] args) {
        //Get command line options
        CsPro2SqlOptions opts = getCommandLineOptions(args);
        boolean error = false;

        //Parse the dictionary
        Dictionary dictionary = null;
        if (opts.dictFile != null && !opts.dictFile.isEmpty()) {
            try {
                dictionary = DictionaryReader.read(opts.dictFile, opts.tablePrefix);
            } catch (IOException ex) {
                opts.ps.close();
                opts.printHelp("Impossible to read dictionary file (" + ex.getMessage() + ")");
            }
        } else {
            try {
                Class.forName("com.mysql.jdbc.Driver").newInstance();
                String srcSchema = opts.prop.getProperty("db.source.schema");
                String srcDataTable = opts.prop.getProperty("db.source.data.table");
                try (Connection connSrc = DriverManager.getConnection(
                        opts.prop.getProperty("db.source.uri") + "/" + srcSchema + "?autoReconnect=true&useSSL=false",
                        opts.prop.getProperty("db.source.username"),
                        opts.prop.getProperty("db.source.password"))) {
                    connSrc.setReadOnly(true);
                    try (Statement stmt = connSrc.createStatement()) {
                        try (ResultSet r = stmt.executeQuery("select dictionary_full_content from " + srcSchema + ".cspro_dictionaries where dictionary_name = '" + srcDataTable + "'")) {
                            r.next();
                            dictionary = DictionaryReader.readFromString(r.getString(1), opts.tablePrefix);
                        }
                    }
                }
            } catch (ClassNotFoundException | SQLException | IOException | InstantiationException | IllegalAccessException ex) {
                opts.ps.close();
                System.err.println("Impossibile to read dictionary from database (" + ex.getMessage() + ")");
            }
        }

        if (opts.schemaEngine) {
            error = !SchemaEngine.execute(dictionary, opts.prop, opts.foreignKeys, opts.ps);
        } else if (opts.loaderEngine) {
            error = !LoaderEngine.execute(dictionary, opts.prop, opts.allRecords, opts.checkConstraints, opts.checkOnly, opts.force, opts.recovery, opts.ps);
        } else if (opts.monitorEngine) {
            error = !MonitorEngine.execute(dictionary, opts.prop, opts.ps);
        } else if (opts.updateEngine) {
            error = !UpdateEngine.execute(dictionary, opts.prop);
        } else if (opts.statusEngine) {
            error = !StatusEngine.execute(opts.prop);
        }
        if (opts.ps != null) {
            opts.ps.close();
        }

        if (error) {
            System.exit(1);
        }
    }

    private static CsPro2SqlOptions getCommandLineOptions(String[] args) {
        Options options = new Options();
        options.addOption("a", "all", false, "transfer all the questionnaires");
        options.addOption("cc", "check-constraints", false, "perform constraints check");
        options.addOption("co", "check-only", false, "perform only constraints check (no data transfer)");
        options.addOption("e", "engine", true, "select engine: [loader|schema|monitor|update|status]");
        options.addOption("f", "force", false, "skip check of loader multiple running instances");
        options.addOption("fk", "foreign-keys", false, "create foreign keys to value sets");
        options.addOption("h", "help", false, "display this help");
        options.addOption("o", "output", true, "name of the output file");
        options.addOption("p", "properties", true, "properties file");
        options.addOption("r", "recovery", false, "recover a broken session of the loader");
        options.addOption("v", "version", false, "print the version of the programm");

        CsPro2SqlOptions opts = new CsPro2SqlOptions(options);
        //Start parsing command line
        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("h") || args.length == 0) { //help option or empty option
                opts.printHelp();
            }
            if (cmd.hasOption("v")) {
                opts.printVersion();
            }

            if (!cmd.hasOption("e")) { //Loader engine option provided
                opts.printHelp("The engine type is mandatory!");
            }
            if (!cmd.hasOption("p")) {
                opts.printHelp("The properties file is mandatory!");
            }

            opts.propertiesFile = cmd.getOptionValue("p");
            String engine = cmd.getOptionValue("e");
            switch (engine) {
                case "schema":
                    opts.schemaEngine = true;
                    opts.foreignKeys = cmd.hasOption("fk");
                    if (cmd.hasOption("o")) {
                        opts.ps = new PrintStream(cmd.getOptionValue("o"), "UTF-8");
                    } else {
                        opts.ps = System.out;
                    }   break;
                case "loader":
                    opts.loaderEngine = true;
                    opts.checkConstraints = cmd.hasOption("cc");
                    opts.checkOnly = cmd.hasOption("co");
                    opts.allRecords = cmd.hasOption("a");
                    opts.force = cmd.hasOption("f");
                    opts.recovery = cmd.hasOption("r");
                    if (cmd.hasOption("o")) {
                        opts.ps = new PrintStream(cmd.getOptionValue("o"), "UTF-8");
                    }   break;
                case "monitor":
                    opts.monitorEngine = true;
                    if (cmd.hasOption("o")) {
                        opts.ps = new PrintStream(cmd.getOptionValue("o"), "UTF-8");
                    } else {
                        opts.ps = System.out;
                    }   break;
                case "update":
                    opts.updateEngine = true;
                    break;
                case "status":
                    opts.statusEngine = true;
                    break;
                default:
                    opts.printHelp("Wrong engine type!");
                    break;
            }
        } catch (ParseException | FileNotFoundException | UnsupportedEncodingException e) {
            opts.printHelp();
        }
        //End parsing command line

        //Load property file
        Properties prop = new Properties();
        try (InputStream in = new FileInputStream(opts.propertiesFile)) {
            prop.load(in);
        } catch (IOException ex) {
            System.out.println("Cannot read properties file '" + opts.propertiesFile + "'");
            opts.printHelp();
        }

        opts.prop = prop;
        opts.dictFile = prop.getProperty("dictionary.filename");
        opts.schema = prop.getProperty("db.dest.schema");
        if (opts.schema == null || opts.schema.isEmpty()) {
            opts.printHelp("The database schema is mandatory!\nPlease set 'db.dest.schema' into the properties file");
        }
        opts.tablePrefix = prop.getProperty("db.dest.table.prefix", "");

        return opts;
    }

    private static class CsPro2SqlOptions {

        boolean schemaEngine;
        boolean loaderEngine;
        boolean monitorEngine;
        boolean updateEngine;
        boolean statusEngine;
        boolean allRecords;
        boolean foreignKeys;
        boolean checkConstraints;
        boolean checkOnly;
        boolean force;
        boolean recovery;
        String dictFile;
        String schema;
        String tablePrefix;
        String propertiesFile;
        PrintStream ps = null;
        Properties prop;
        private final Options options;

        CsPro2SqlOptions(Options options) {
            this.options = options;
        }

        void printHelp() {
            printHelp(null);
        }

        void printHelp(String errMessage) {
            HelpFormatter formatter = new HelpFormatter();
            if (errMessage != null) {
                System.err.println(errMessage);
            }
            System.out.println("CsPro2Sql - version " + VERSION + "\n");
            formatter.printHelp("\n\n"
                    + "CsPro2Sql -e schema  -p PROPERTIES_FILE [-fk] [-o OUTPUT_FILE]\n"
                    + "CsPro2Sql -e loader  -p PROPERTIES_FILE [-a] [-cc] [-co] [-f|-r] [-o OUTPUT_FILE]\n"
                    + "CsPro2Sql -e monitor -p PROPERTIES_FILE [-o OUTPUT_FILE]\n"
                    + "CsPro2Sql -e update  -p PROPERTIES_FILE\n"
                    + "CsPro2Sql -e status  -p PROPERTIES_FILE\n"
                    + "CsPro2Sql -v\n"
                    + "\n", options);
            if (errMessage == null) {
                System.exit(0);
            } else {
                System.exit(1);
            }
        }

        void printVersion() {
            System.out.println("CsPro2Sql - version " + VERSION);
            System.exit(0);
        }
    }

}
