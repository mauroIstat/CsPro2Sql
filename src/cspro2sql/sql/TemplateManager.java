package cspro2sql.sql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Map;

public class TemplateManager {

    public static void printTemplate(String template, Map<String, String> params, PrintStream ps) throws IOException {
        try (InputStream in = TemplateManager.class.getResourceAsStream("/cspro2sql/sql/" + template + ".sql")) {
            try (InputStreamReader isr = new InputStreamReader(in, "UTF-8")) {
                try (BufferedReader br = new BufferedReader(isr)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        for (Map.Entry<String, String> e : params.entrySet()) {
                            line = line.replace(e.getKey(), e.getValue());
                        }
                        ps.println(line);
                    }
                }
            }
        }
    }

}
