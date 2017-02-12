package cspro2sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.reader.DictionaryReader;
import cspro2sql.sql.TemplateManager;
import cspro2sql.writer.MonitorWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;

/**
 *
 * @author Istat Cooperation Unit
 */
public class MonitorEngine {

    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        try (InputStream in = MonitorEngine.class.getResourceAsStream("/database.properties")) {
            prop.load(in);
        } catch (IOException ex) {
            return;
        }
        try {
            Dictionary dictionary = DictionaryReader.read(
                    prop.getProperty("dictionary.filename"),
                    prop.getProperty("db.dest.table.prefix"));
            execute(dictionary, prop, System.out);
        } catch (Exception ex) {
            System.exit(1);
        }
    }

    static boolean execute(Dictionary dictionary, Properties prop, PrintStream out) {
        String schema = prop.getProperty("db.dest.schema");
        TemplateManager tm = new TemplateManager(dictionary, prop);
        return MonitorWriter.write(schema, tm, out);
    }

}
