package cspro2sql.reader;

import cspro2sql.bean.BeanFactory;
import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Item;
import cspro2sql.bean.ValueSet;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Set;

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
 * @version 0.9.6
 */
public class DictionaryReader {

    public static Dictionary read(String fileName, String tablePrefix, Set<String> multipleAnswers, Set<String> ignoreItems) throws IOException {
        Dictionary dictionary = new Dictionary();
        boolean isLocalFile = new File(fileName).exists();
        try (InputStream in
                = (isLocalFile
                        ? new FileInputStream(fileName)
                        : DictionaryReader.class.getResourceAsStream("/" + fileName))) {
            try (InputStreamReader fr = new InputStreamReader(in, "UTF-8")) {
                try (BufferedReader br = new BufferedReader(fr)) {
                    read(dictionary, tablePrefix, multipleAnswers, ignoreItems, br);
                }
            }
        }
        return dictionary;
    }

    public static Dictionary readFromString(String dictionaryString, String tablePrefix, Set<String> multipleAnswers, Set<String> ignoreItems) throws IOException {
        Dictionary dictionary = new Dictionary();
        try (Reader reader = new StringReader(dictionaryString)) {
            try (BufferedReader br = new BufferedReader(reader)) {
                read(dictionary, tablePrefix, multipleAnswers, ignoreItems, br);
            }
        }
        return dictionary;
    }

    private static void read(Dictionary dictionary, String tablePrefix, Set<String> multipleAnswers, Set<String> ignoreItems, BufferedReader br) throws IOException {
        String line;
        boolean skipValueSet = false;
        while ((line = br.readLine()) != null) {
            switch (line) {
                case Dictionary.DICT_LEVEL:
                case Dictionary.DICT_RECORD:
                    dictionary.addRecord(BeanFactory.createRecord(br, tablePrefix));
                    skipValueSet = false;
                    break;
                case Dictionary.DICT_ITEM:
                    Item item = BeanFactory.createItem(br, multipleAnswers);
                    if (ignoreItems.contains(item.getName())) {
                        skipValueSet = true;
                    } else {
                        dictionary.addItem(item);
                        skipValueSet = false;
                    }
                    break;
                case Dictionary.DICT_VALUESET:
                    ValueSet vs = BeanFactory.createValueSet(br);
                    if (!skipValueSet) {
                        dictionary.addValueSet(vs);
                    }
                    break;
                default:
            }
        }
    }

}
