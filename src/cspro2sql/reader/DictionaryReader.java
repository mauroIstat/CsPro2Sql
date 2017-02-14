package cspro2sql.reader;

import cspro2sql.bean.BeanFactory;
import cspro2sql.bean.Dictionary;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

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
 * @version 0.9
 */
public class DictionaryReader {

    public static Dictionary read(String fileName, String tablePrefix) throws IOException {
        Dictionary dictionary = new Dictionary();
        boolean isLocalFile = new File(fileName).exists();
        try (InputStream in
                = (isLocalFile
                        ? new FileInputStream(fileName)
                        : DictionaryReader.class.getResourceAsStream("/" + fileName))) {
            try (InputStreamReader fr = new InputStreamReader(in, "UTF-8")) {
                try (BufferedReader br = new BufferedReader(fr)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        switch (line) {
                            case Dictionary.DICT_LEVEL:
                            case Dictionary.DICT_RECORD:
                                dictionary.addRecord(BeanFactory.createRecord(br, tablePrefix));
                                break;
                            case Dictionary.DICT_ITEM:
                                dictionary.addItem(BeanFactory.createItem(br));
                                break;
                            case Dictionary.DICT_VALUESET:
                                dictionary.addValueSet(BeanFactory.createValueSet(br));
                                break;
                            default:
                        }
                    }
                }
            }
        }
        return dictionary;
    }

    public static Dictionary readFromString(String dictionaryString, String tablePrefix) throws IOException {
        Dictionary dictionary = new Dictionary();
        try (Reader reader = new StringReader(dictionaryString)) {
            try (BufferedReader br = new BufferedReader(reader)) {
                String line;
                while ((line = br.readLine()) != null) {
                    switch (line) {
                        case Dictionary.DICT_LEVEL:
                        case Dictionary.DICT_RECORD:
                            dictionary.addRecord(BeanFactory.createRecord(br, tablePrefix));
                            break;
                        case Dictionary.DICT_ITEM:
                            dictionary.addItem(BeanFactory.createItem(br));
                            break;
                        case Dictionary.DICT_VALUESET:
                            dictionary.addValueSet(BeanFactory.createValueSet(br));
                            break;
                        default:
                    }
                }
            }
        }
        return dictionary;
    }

}
