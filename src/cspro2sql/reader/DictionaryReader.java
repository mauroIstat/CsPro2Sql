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
 * This class reads the CSPro Dictionary creating a data structure that
 * represents the dictionary
 *
 * @author Istat Cooperation Unit
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
