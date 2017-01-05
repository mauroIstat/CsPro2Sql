
package cspro2sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.reader.DictionaryReader;
import cspro2sql.writer.SchemaWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Use this class to create the schema script for the database
 * 
 * @author Istat Cooperation Unit
 */
public class SchemaEngine {

    private static final Logger LOGGER = Logger.getLogger(SchemaEngine.class.getName());

    public static void main(String[] args) throws Exception {
    	Properties prop = new Properties();
        
        //Load property file
        try (InputStream in = SchemaEngine.class.getResourceAsStream("/database.properties")) {
            prop.load(in);
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Cannot read properties file", ex);
            return;
        }
        
        //Parse dictionary file
        try {
            Dictionary dictionary = DictionaryReader.read(
                    prop.getProperty("dictionary.filename"),
                    prop.getProperty("db.dest.table.prefix"));
            execute(dictionary, prop.getProperty("db.dest.schema"), System.out);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Impossible to create the database schema", ex);
            System.exit(1);
        }
        
    }
    
    static void execute(Dictionary dictionary, String schema, PrintStream out) {
        SchemaWriter.write(schema, dictionary, out);
    }

}
