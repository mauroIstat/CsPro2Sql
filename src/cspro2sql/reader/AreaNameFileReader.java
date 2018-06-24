
package cspro2sql.reader;

import cspro2sql.bean.AreaNameFile;
import cspro2sql.bean.NamedArea;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * @author Josh Handley <joshua.e.handley@census.gov>
 */
public class AreaNameFileReader {
    private static final String AREA_NAMES = "[Area Names]";
    private static final String LEVELS = "[Levels]";
    private static final String AREAS = "[Areas]";
    private static final Pattern VERSION_LINE = Pattern.compile("Version\\s*=\\s*CSPro ([0-9]+\\.[0-9]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern LEVEL_NAME = Pattern.compile("Name\\s*=\\s*(.+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern AREA_LINE = Pattern.compile("((?:(?:X|[0-9]+)\\s*,?\\s*)+)=\\s*(.+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern AREA_CODE = Pattern.compile("(X|[0-9]+)\\s*,?\\s*", Pattern.CASE_INSENSITIVE);
    
    public static AreaNameFile parseAreaNamesFile(String filename) throws Exception
    {
        try {
            return read(filename);
        } catch (IOException ex) {
            throw new Exception("Impossible to read dictionary " + filename + " (" + ex.getMessage() + ")", ex);
        }
    }
    
    private static AreaNameFile read(String fileName) throws IOException
    {
        try (InputStream in = new FileInputStream(fileName)) {
            try (InputStreamReader fr = new InputStreamReader(in, "UTF-8")) {
                try (BufferedReader br = new BufferedReader(fr)) {
                    return read(br);
                }
            }
        }
    }
    
    private static AreaNameFile read(BufferedReader br) throws IOException {
        
        br.mark(4);
        if ('\ufeff' != br.read()) 
            br.reset();
        readThroughSectionHeader(br, AREA_NAMES);
        String version = readVersion(br);
        if (version == null)
            throw new IOException("Missing version");
        String[] levels = readLevels(br);
        NamedArea[] areas = readAreas(br);
        return new AreaNameFile(levels, areas);
    }
    
    private static void readThroughSectionHeader(BufferedReader br, String header) throws IOException
    {
        String line;
        while ((line = br.readLine().trim()) != null) {
            if (line.equalsIgnoreCase(header))
                return;
            else if (!line.isEmpty() )
                throw new IOException("Expected " + header + " got " + line );
        }
    }
    
    private static String readVersion(BufferedReader br) throws IOException {
        String line;
        String version = null;
        Matcher matcher;
        while ((line = br.readLine().trim()) != null) {
            if (line.equalsIgnoreCase(LEVELS))
            {
                break;
            }
            else if ((matcher = VERSION_LINE.matcher(line)).matches())
            {
                version = matcher.group(1);
            } else if (!line.isEmpty())
            {
                throw new IOException("Expected 'name=level' got " + line );
            }
        }
        return version;
    }

    private static String[] readLevels(BufferedReader br) throws IOException {
        ArrayList<String> levels = new ArrayList<>();
        String line;
        Matcher matcher;
        while ((line = br.readLine().trim()) != null) {
            if (line.equalsIgnoreCase(AREAS)) {
                break;
            } else if ((matcher = LEVEL_NAME.matcher(line)).matches())
            {
                levels.add(matcher.group(1));
            } else if (!line.isEmpty())
            {
                throw new IOException("Expected 'name=level' got " + line );
            }
        }
        return levels.toArray(new String[0]);
    }

    private static NamedArea[] readAreas(BufferedReader br) throws IOException {
        ArrayList<NamedArea> areas = new ArrayList<>();
        String line;
        Matcher lineMatcher;
        while ((line = br.readLine()) != null) {
            if ((lineMatcher = AREA_LINE.matcher(line)).matches())
            {
                String name = lineMatcher.group(2);
                String codesString = lineMatcher.group(1);
                ArrayList<Integer> codes = new ArrayList<>();
                Matcher codeMatcher = AREA_CODE.matcher(codesString);
                while (codeMatcher.find()) {
                    String code = codeMatcher.group(1);
                    if (code.equals("X"))
                        break;
                    else
                        codes.add(Integer.parseInt(code));
                }
                areas.add(new NamedArea(name, codes));
            } else if (!line.isEmpty()) {
                throw new IOException("Expected 'name=level' got " + line );
            }
        }
        return areas.toArray(new NamedArea[0]);
    }
}
