package cspro2sql.sql;

import cspro2sql.bean.Answer;
import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Item;
import cspro2sql.bean.Record;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PreparedStatementManager {

    private static final Map<String, PreparedStatement> RECORDS_LIST = new HashMap<>();

    public static void execute(Record record) throws SQLException {
        String key = record.getName();
        PreparedStatement insertPS = RECORDS_LIST.get(key);
        insertPS.executeBatch();
        //insertPS.clearParameters();
    }

    public static void populateInsertPreparedStatement(Record record, int id, int index, List<Answer> values, String schema, Connection conn) throws SQLException {
        PreparedStatement insertPS = getInsertStmt(record, schema, conn);
        int field = 1;
        if (!record.isMainRecord()) {
            insertPS.setInt(field++, id);
            insertPS.setInt(field++, index);
        }
        for (Answer a : values) {
            Item item = a.getItem();
            String v = a.getValue();
            switch (item.getDataType()) {
                case Dictionary.ITEM_DECIMAL:
                    if (v == null) {
                        insertPS.setNull(field++, Types.INTEGER);
                    } else if (v.contains(".")) {
                        insertPS.setDouble(field++, Double.parseDouble(v.trim()));
                    } else {
                        insertPS.setInt(field++, Integer.parseInt(v.trim()));
                    }
                    break;
                case Dictionary.ITEM_ALPHA:
                    if (v == null) {
                        insertPS.setNull(field++, Types.VARCHAR);
                    } else {
                        insertPS.setString(field++, v);
                    }
                    break;
            }
        }
        insertPS.addBatch();
    }

    private static PreparedStatement getInsertStmt(Record record, String schema, Connection conn) {
        String key = record.getName();
        if (!RECORDS_LIST.containsKey(key)) {
            RECORDS_LIST.put(key, createInsertPreparedStatement(record, schema, conn));
        }
        return RECORDS_LIST.get(key);
    }

    private static PreparedStatement createInsertPreparedStatement(Record record, String schema, Connection conn) {
        String sql = "insert into " + schema + "." + record.getTableName() + " (";
        String values = "";
        boolean first = true;
        if (!record.isMainRecord()) {
            first = false;
            sql += record.getMainRecord().getName() + ",COUNTER";
            values += "?,?";
        }
        for (Item item : record.getItems()) {
            if (first) {
                first = false;
            } else {
                sql += ",";
                values += ",";
            }
            sql += item.getName();
            values += "?";
            for (Item subitem : item.getSubItems()) {
                sql += ",";
                sql += subitem.getName();
                values += ",?";
            }
        }
        sql += ") values (";
        sql += values;
        sql += ")";
        try {
            return conn.prepareStatement(sql);
        } catch (SQLException ex) {
            System.out.println("Impossibile to create prepared statement (" + ex.getMessage() + ")");
            System.exit(1);
        }
        return null;
    }

    public static String getSqlCode(Record record, int id, List<List<Answer>> vvalues, String schema) {
        String sql = "insert into " + schema + "." + record.getTableName() + " (";
        boolean first = true;
        if (!record.isMainRecord()) {
            first = false;
            sql += record.getMainRecord().getName() + ",COUNTER";
        }
        for (Item item : record.getItems()) {
            if (first) {
                first = false;
            } else {
                sql += ",";
            }
            sql += item.getName();
            for (Item subitem : item.getSubItems()) {
                sql += ",";
                sql += subitem.getName();
            }
        }
        sql += ") values ";
        for (int i = 0; i < vvalues.size(); i++) {
            List<Answer> values = vvalues.get(i);
            if (i > 0) {
                sql += ",";
            }
            sql += "\n\t\t(";
            first = true;
            if (!record.isMainRecord()) {
                first = false;
                sql += id + "," + i;
            }
            for (Answer a : values) {
                if (first) {
                    first = false;
                } else {
                    sql += ",";
                }
                sql += a.getValue();
            }
            sql += ")";
        }
        return sql;
    }

}
