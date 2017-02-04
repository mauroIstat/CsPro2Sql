package cspro2sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Record;
import cspro2sql.reader.DictionaryReader;
import cspro2sql.writer.MonitorWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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
        Record mainRecord = dictionary.getMainRecord();

        String schema = prop.getProperty("db.dest.schema");
        String[] ea = prop.getProperty("column.questionnaire.ea").split(",");
        String[] eaName = prop.getProperty("column.questionnaire.ea.name").split(",");
        String[] eaDescription = prop.getProperty("column.questionnaire.ea.description").split(",");
        eaName = Arrays.copyOf(eaName, ea.length);
        eaDescription = Arrays.copyOf(eaDescription, ea.length);
        for (int i = 0; i < eaDescription.length; i++) {
            if (eaDescription[i] != null && !eaDescription[i].isEmpty()) {
                eaDescription[i] = mainRecord.getValueSetPrefix() + eaDescription[i];
            }
        }
        String[] ageRangeS = prop.getProperty("range.individual.age").split(",");
        int[] ageRange = new int[ageRangeS.length];
        for (int i = 0; i < ageRangeS.length; i++) {
            ageRange[i] = Integer.parseInt(ageRangeS[i]);
        }

        Map<String, String> params = new HashMap<>();
        params.put("@SCHEMA", schema);
        params.put("@QUESTIONNAIRE_TABLE", mainRecord.getTableName());
        params.put("@QUESTIONNAIRE_COLUMN_BASE", mainRecord.getName());
        params.put("@QUESTIONNAIRE_COLUMN_REGION", ea[0]);
        params.put("@INDIVIDUAL_TABLE", mainRecord.getTablePrefix() + prop.getProperty("table.individual"));
        params.put("@INDIVIDUAL_COLUMN_SEX", prop.getProperty("column.individual.sex"));
        params.put("@INDIVIDUAL_COLUMN_AGE", prop.getProperty("column.individual.age"));
        params.put("@INDIVIDUAL_COLUMN_RELIGION", prop.getProperty("column.individual.religion"));
        params.put("@INDIVIDUAL_VALUE_SEX_MALE", prop.getProperty("column.individual.sex.value.male"));
        params.put("@INDIVIDUAL_VALUE_SEX_FEMALE", prop.getProperty("column.individual.sex.value.female"));
        params.put("@VALUESET_REGION", eaDescription[0]);
        params.put("@VALUESET_SEX", mainRecord.getValueSetPrefix() + prop.getProperty("column.individual.sex"));
        params.put("@VALUESET_RELIGION", mainRecord.getValueSetPrefix() + prop.getProperty("column.individual.religion"));

        return MonitorWriter.write(schema, ea, eaName, eaDescription, ageRange, params, out);
    }

}
