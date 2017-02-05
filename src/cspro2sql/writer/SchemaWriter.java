package cspro2sql.writer;

import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Item;
import cspro2sql.bean.Record;
import cspro2sql.bean.ValueSet;
import cspro2sql.sql.TemplateManager;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * This class writes the SQL scripts to create a MySQL DB schema parsing the
 * data structure representing the CSPro Dictionary
 *
 * @author Istat Cooperation Unit
 */
public class SchemaWriter {

    public static void write(Dictionary dictionary, Properties prop, boolean foreignKeys, PrintStream ps) {
        String schema = prop.getProperty("db.dest.schema");
        String sourceDataTable = prop.getProperty("db.source.data.table");

        ps.println("CREATE SCHEMA IF NOT EXISTS " + schema + ";");
        ps.println();
        ps.println("USE " + schema + ";");
        ps.println();

        try {
            Map<String, String> params = new HashMap<>();
            params.put("@SCHEMA", schema);
            params.put("@SOURCE_DATA_TABLE", sourceDataTable);
            TemplateManager.printTemplate("cspro2sql_dictionary", params, ps);
            TemplateManager.printTemplate("cspro2sql_error", params, ps);
        } catch (IOException ex) {
            return;
        }

        for (Record record : dictionary.getRecords()) {
            for (Item item : record.getItems()) {
                printValueSet(schema, item, ps);
            }
        }

        for (Record record : dictionary.getRecords()) {
            ps.println("CREATE TABLE " + schema + "." + record.getTableName() + " (");
            ps.println("    ID INT(9) UNSIGNED AUTO_INCREMENT,");
            if (!record.isMainRecord()) {
                ps.println("    " + record.getMainRecord().getName() + " INT(9) UNSIGNED NOT NULL,");
                ps.println("    COUNTER INT(9) UNSIGNED NOT NULL,");
            }
            for (Item item : record.getItems()) {
                printItem(schema, foreignKeys, item, ps);
            }
            if (!record.isMainRecord()) {
                ps.println("    INDEX (" + record.getMainRecord().getName() + "),");
                ps.println("    FOREIGN KEY (" + record.getMainRecord().getName() + ") REFERENCES " + schema + "." + record.getMainRecord().getTableName() + "(id),");
            }
            ps.println("    PRIMARY KEY (ID)");
            ps.println(") ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;");
            ps.println();
        }
    }

    private static void printItem(String schema, boolean foreignKeys, Item item, PrintStream ps) {
        String name = item.getName();
        int length = item.getLength();
        switch (item.getDataType()) {
            case Dictionary.ITEM_ALPHA:
                ps.println("    " + name + " CHAR(" + length + "),");
                break;
            case Dictionary.ITEM_DECIMAL:
                ps.println("    " + name + " INT(" + length + "),");
                break;
            default:
        }
        if (foreignKeys && item.hasValueSets()) {
            ps.println("    FOREIGN KEY (" + name + ") REFERENCES " + schema + "." + item.getValueSetName() + "(ID),");
        }
        for (Item subItem : item.getSubItems()) {
            printItem(schema, foreignKeys, subItem, ps);
        }
    }

    private static void printValueSet(String schema, Item item, PrintStream ps) {
        if (item.hasValueSets() && item.getValueSets().get(0).isNotCreated()) {
            ps.println("CREATE TABLE " + schema + "." + item.getValueSetName() + " (");
            switch (item.getDataType()) {
                case Dictionary.ITEM_ALPHA:
                    ps.println("    ID CHAR(" + item.getLength() + "),");
                    break;
                case Dictionary.ITEM_DECIMAL:
                    ps.println("    ID INT(" + item.getLength() + "),");
                    break;
                default:
            }
            ps.println("    VALUE CHAR(" + item.getValueSetsValueLength() + "),");
            ps.println("    PRIMARY KEY (ID)");
            ps.println(") ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;");
            ps.println();
            boolean first = true;
            Set<String> keys = new HashSet<>();
            ps.println("INSERT INTO " + schema + "." + item.getValueSetName() + "(ID,VALUE) VALUES ");
            for (int i = 0; i < item.getValueSets().size(); i++) {
                ValueSet valueSet = item.getValueSets().get(i);
                valueSet.setCreated();
                for (Map.Entry<String, String> e : valueSet.getValues().entrySet()) {
                    if (keys.contains(e.getKey())) {
                        continue;
                    }
                    if (!first) {
                        ps.println(",");
                    }
                    ps.print("    (\"" + e.getKey() + "\",\"" + e.getValue() + "\")");
                    keys.add(e.getKey());
                    first = false;
                }
            }
            ps.println(";");
            ps.println();
        }
        for (Item subItem : item.getSubItems()) {
            printValueSet(schema, subItem, ps);
        }
    }

}
