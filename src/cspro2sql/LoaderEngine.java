
package cspro2sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Record;
import cspro2sql.reader.DictionaryReader;
import cspro2sql.reader.QuestionnaireReader;
import cspro2sql.writer.InsertWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Use this class to transfer date from a CsPro db to a MySql schema
 * 
 * @author Istat Cooperation Unit
 */
public class LoaderEngine {
    
    private static final Logger LOGGER = Logger.getLogger(LoaderEngine.class.getName());
    private static final int RECORDS_LIMIT = 100;
    private static final String ERROR_STMT = "insert into CSPRO2SQL_ERRORS (ERROR, DATE, CSPRO_GUID, QUESTIONNAIRE) values (?,?,?,?)";
    private static final String LAST_UPDATE_INSERT_STMT = "update CSPRO2SQL_LASTUPDATE set LAST_UPDATE = ?";
    private static final String LAST_UPDATE_SELECT_STMT = "select LAST_UPDATE from CSPRO2SQL_LASTUPDATE";
    
    public static void main(String[] args) {
        execute("/database.properties");
    }
    
    static void execute(String propertiesFile) {
    	Dictionary dictionary;
        Properties prop = new Properties();
        boolean isLocalFile = new File(propertiesFile).exists();
        //Load property file
        try (InputStream in =
            (isLocalFile?
                new FileInputStream(propertiesFile):
                LoaderEngine.class.getResourceAsStream(propertiesFile))) {
            prop.load(in);
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Cannot read properties file", ex);
            return;
        }
        
        //Parse dictionary file
        try {
            dictionary = DictionaryReader.read(
                    prop.getProperty("dictionary.filename"),
                    prop.getProperty("db.dest.table.prefix"));
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Impossible to read dictionary file", ex);
            return;
        }
        
        Connection connSrc = null;
        Connection connDst = null;
        
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            String srcSchema = prop.getProperty("db.source.schema");
            String srcDataTable = prop.getProperty("db.source.data.table");
            
            //Connect to the source database
            connSrc = DriverManager.getConnection(
                    prop.getProperty("db.source.uri")+"/"+srcSchema+"?autoReconnect=true&useSSL=false",
                    prop.getProperty("db.source.username"),
                    prop.getProperty("db.source.password"));
            connSrc.setReadOnly(true);
            
            //Connect to the destination database
            connDst = DriverManager.getConnection(
                    prop.getProperty("db.dest.uri")+"/"+prop.getProperty("db.dest.schema")+"?autoReconnect=true&useSSL=false",
                    prop.getProperty("db.dest.username"),
                    prop.getProperty("db.dest.password"));
            connDst.setAutoCommit(false);
            
            Statement stmtDst = connDst.createStatement();
            PreparedStatement errorStmt = connDst.prepareStatement(ERROR_STMT);
            PreparedStatement lastUpdateStmt = connDst.prepareStatement(LAST_UPDATE_INSERT_STMT);
            PreparedStatement selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid from "+srcSchema+"."+srcDataTable+" where modified_time >= ?  limit "+RECORDS_LIMIT);
            
            Timestamp now = new Timestamp(System.currentTimeMillis());
            ResultSet result = stmtDst.executeQuery(LAST_UPDATE_SELECT_STMT);
            result.next();
            Timestamp dTimestamp = result.getTimestamp(1);

            //Get questionnaires from source database (CSPro plain text files)
            selectQuestionnaire.setTimestamp(1, dTimestamp);
            result = selectQuestionnaire.executeQuery();
            while (result.next()) {
                String questionnaire = result.getString(1);
                InputStream binaryStream = result.getBinaryStream(2);
                //Get the microdata parsing CSPro plain text files according to its dictionary
                try {
                    Map<Record, List<List<String>>> microdata = QuestionnaireReader.parse(dictionary, questionnaire);
                    //Generate the insert statements (to store microdata into the destination database)
                    InsertWriter.create(prop.getProperty("db.dest.schema"), dictionary, microdata, stmtDst);
                    connDst.commit();
                } catch (Exception e) {
                    connDst.rollback();
                    String msg = "Impossible to transfer questionnaire - "+e.getMessage();
                    if (msg.length()>2048) msg = msg.substring(0, 2048);
                    errorStmt.setString(1, msg);
                    errorStmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
                    errorStmt.setBinaryStream(3, binaryStream);
                    errorStmt.setString(4, questionnaire);
                    errorStmt.executeUpdate();
                    connDst.commit();
                }
            }
            
            lastUpdateStmt.setTimestamp(1, now);
            lastUpdateStmt.executeUpdate();
            connDst.commit();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            try {
                if (connDst!=null) connDst.rollback();
            } catch (SQLException ex1) {
                LOGGER.log(Level.SEVERE, "Rollback failure", ex1);
            }
            LOGGER.log(Level.SEVERE, "Database exception", ex);
        } finally {
            try {
                if (connSrc!=null) connSrc.close();
            } catch (SQLException ex) {
                LOGGER.log(Level.WARNING, "Impossible to close the db conenction", ex);
            }
            try {
                if (connDst!=null) connDst.close();
            } catch (SQLException ex) {
                LOGGER.log(Level.WARNING, "Impossible to close the db conenction", ex);
            }
        }
    }
    
}
