package cspro2sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.reader.DictionaryReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

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
public class UpdateEngine {

    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        try (InputStream in = UpdateEngine.class.getResourceAsStream("/database.properties")) {
            prop.load(in);
        } catch (IOException ex) {
            return;
        }
        try {
            Dictionary dictionary = DictionaryReader.read(
                    prop.getProperty("dictionary.filename"),
                    prop.getProperty("db.dest.table.prefix"));
            execute(dictionary, prop);
        } catch (Exception ex) {
            System.exit(1);
        }
    }

    static boolean execute(Dictionary dictionary, Properties prop) {
        String schema = prop.getProperty("db.dest.schema");
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();

            //Connect to the destination database
            try (Connection connDst = DriverManager.getConnection(
                    prop.getProperty("db.dest.uri") + "/" + prop.getProperty("db.dest.schema") + "?autoReconnect=true&useSSL=false",
                    prop.getProperty("db.dest.username"),
                    prop.getProperty("db.dest.password"))) {
                connDst.setAutoCommit(false);

                try (Statement readDst = connDst.createStatement()) {
                    try (Statement writeDst = connDst.createStatement()) {
                        try (ResultSet rs = readDst.executeQuery("SELECT * FROM " + schema + ".cspro2sql_report")) {
                            while (rs.next()) {
                                String template = rs.getString(1);
                                System.out.print("Updating " + template + "... ");
                                writeDst.executeUpdate("TRUNCATE " + schema + ".m" + template);
                                writeDst.executeQuery("SELECT @ID := 0");
                                writeDst.executeUpdate("INSERT INTO " + schema + ".m" + template + " SELECT @ID := @ID + 1 ID, " + template + ".* FROM " + schema + "." + template);
                                connDst.commit();
                                System.out.println("done");
                            }
                        }
                    }
                }
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            System.out.println("Database exception (" + ex.getMessage() + ")");
            return false;
        }
        return true;
    }

}
