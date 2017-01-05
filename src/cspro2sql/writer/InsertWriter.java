
package cspro2sql.writer;

import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Item;
import cspro2sql.bean.Record;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * This class executes SQL inserts to transfer questionnaire from a CsPro db to a MySQL schema
 *
 * @author Istat Cooperation Unit
 */
public class InsertWriter {

    public static void create(String schema, Dictionary dictionary, Map<Record, List<List<String>>> descr, Statement stmt) throws SQLException {
        int id = 0;
        boolean exists = false;
        for (Map.Entry<Record, List<List<String>>> e : descr.entrySet()) {
            Record record = e.getKey();
            String selectSql = "select ID from " + schema + "." + record.getTableName() + " where ";
            String sql = "insert into " + schema + "." + record.getTableName() + " (";
            boolean first = true;
            if (!record.isMainRecord()) {
                first = false;
                sql += dictionary.getMainRecord().getName() + ",COUNTER";
            }
            for (Item item : record.getItems()) {
                if (first) first = false;
                else sql += ",";
                sql += item.getName();
            }
            sql += ") values ";
            for (int i=0; i<e.getValue().size(); i++) {
                List<String> values = e.getValue().get(i);
                if (i>0) sql += ",";
                sql += "\n\t\t(";
                first = true;
                if (!record.isMainRecord()) {
                    first = false;
                    sql += id + "," + i;
                }
                for (String v : values) {
                    if (first) first = false;
                    else sql += ",";
                    sql += v;
                }
                sql += ")";
            }
            if (record.isMainRecord()) {
                int i=0;
                first = true;
                for (Item item : record.getItems()) {
                    String value = e.getValue().get(0).get(i++);
                    if (first) first = false;
                    else selectSql += " AND ";
                    if (value==null)
                        selectSql += item.getName() + " is null";
                    else
                        selectSql += item.getName() + "='" + value+ "'";
                }
                ResultSet executeQuery = stmt.executeQuery(selectSql);
                exists = executeQuery.next();
                if (exists) {
                    id = executeQuery.getInt(1);
                    continue;
                }
            }
            /*
            if (exists && !record.isMainRecord()) {
                System.out.println("delete from " + schema + "." + record.getTableName() +
                        " where " + record.getMainRecord().getName() + "=" + id);
            }
            System.out.println(sql);
            if (record.isMainRecord()) {
                System.out.println("select last_insert_id()");
            }
            */
            
            if (exists && !record.isMainRecord()) {
                stmt.executeUpdate("delete from " + schema + "." + record.getTableName() +
                        " where " + record.getMainRecord().getName() + "=" + id);
            }
            stmt.executeUpdate(sql);
            if (record.isMainRecord()) {
                ResultSet lastInsertId = stmt.executeQuery("select last_insert_id()");
                lastInsertId.next();
                id = lastInsertId.getInt(1);
            }
        }
    }

}
