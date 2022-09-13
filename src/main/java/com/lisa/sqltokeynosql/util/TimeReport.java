package com.lisa.sqltokeynosql.util;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TimeReport
{
    //
    public static double TotalSegundos;

    private static Map<String, ArrayList<Double>> TemposNeo4jSegundos = new HashMap<>();
    private static Map<String, ArrayList<Double>> TemposConnectorSegundos = new HashMap<>();

    public static void putTimeNeo4j(String key, Double time)
    {
        if(!TemposNeo4jSegundos.containsKey(key))
            TemposNeo4jSegundos.put(key, new ArrayList<>());
        TemposNeo4jSegundos.get(key).add(time);
    }

    public static void putTimeConnector(String key, Double time)
    {
        if(!TemposConnectorSegundos.containsKey(key))
            TemposConnectorSegundos.put(key, new ArrayList<>());
        TemposConnectorSegundos.get(key).add(time);
    }

    private static String printNumber(Double value)
    {
        return Double.toString(value).replace(".", ",");
    }
    public static void GeneratCsvRepost() throws IOException {
        var append = new StringBuilder();
        append.append("COMANDO;MINUTOS;SEGUNDOS" + "\n");
        var totalSecondsConnector = SumGenerateReport(append, TemposConnectorSegundos);
        var totalSecondsNeo4j = SumGenerateReport(append, TemposNeo4jSegundos);

        var report = new StringBuilder();
        report.append("TOTAL;MINUTOS;SEGUNDOS;Overhead" + "\n");
        report.append("GERAL;" + printNumber(TotalSegundos/ 60) + ";" + printNumber(TotalSegundos ) + ";" +  printNumber(TotalSegundos/ totalSecondsConnector -1) + "\n");
        report.append("CONNECTOR" + ";" + printNumber(totalSecondsConnector/ 60) + ";" + printNumber(totalSecondsConnector) + ";" +  printNumber(totalSecondsConnector / totalSecondsNeo4j -1) + "\n");
        report.append("NEO4J" + ";" + printNumber(totalSecondsNeo4j/ 60) + ";" + printNumber(totalSecondsNeo4j) + "\n");

        report.append("\n");
        report.append(append);

        String formattedDate = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyy-MM-dd HH-mm-ss"));

        var fileName = "report-" +formattedDate + ".csv";
        var folder = Paths.get("reports");
        if(!Files.exists(folder))
            Files.createDirectory(folder);

        var filePath = Paths.get(folder.toString(), fileName);
        FileWriter myWriter = new FileWriter(filePath.toString(), StandardCharsets.ISO_8859_1);
        myWriter.write(report.toString());
        myWriter.close();

        // Clear Reports
        TotalSegundos = 0l;
        TemposNeo4jSegundos = new HashMap<>();
        TemposConnectorSegundos = new HashMap<>();

    }

    private static Double SumGenerateReport(StringBuilder linhas, Map<String, ArrayList<Double>> dados)
    {
        Double totalSeconds = 0d;
        for (var stringLongEntry : dados.entrySet())
        {
            var comando = stringLongEntry.getKey();
            var times = stringLongEntry.getValue();
            for (Double time : times)
            {
                totalSeconds += time;
                var segundos = printNumber(time);
                var minutos = printNumber(time / 60);
                linhas.append(comando + ";" + minutos + ";" + segundos + "\n");
            }
        }

        return totalSeconds;
    }

}
