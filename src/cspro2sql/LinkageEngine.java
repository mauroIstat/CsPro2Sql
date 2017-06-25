package cspro2sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.reader.DictionaryReader;
import cspro2sql.sql.TemplateManager;
import cspro2sql.writer.LinkageWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
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
 * @author Paolo Giacomi <giacomi @ istat.it>
 * @version 0.9.12
 * @since 0.9.9
 */
public class LinkageEngine {
    
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        try (InputStream in = LinkageWriter.class.getResourceAsStream("/database.properties")) {
            prop.load(in);
        } catch (IOException ex) {
            return;
        }
        try {
            List<Dictionary> dictionaries = DictionaryReader.parseDictionaries(
                    prop.getProperty("db.dest.schema"),
                    prop.getProperty("dictionary"),
                    prop.getProperty("dictionary.prefix"));
            execute(dictionaries.get(0), dictionaries.get(1), prop, System.out);
        } catch (Exception ex) {
            System.exit(1);
        }
    }
    
    static boolean execute(Dictionary censusDictionary, Dictionary pesDictionary, Properties prop, PrintStream out) {
        String schema = prop.getProperty("db.dest.schema");
        TemplateManager tm = new TemplateManager(censusDictionary);
        return LinkageWriter.write(schema, censusDictionary, pesDictionary, tm, out);
    }
    
}
