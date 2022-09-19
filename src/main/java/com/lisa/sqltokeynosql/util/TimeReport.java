package com.lisa.sqltokeynosql.util;

import org.springframework.util.StopWatch;

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

    public static void putTimeNeo4j(String key1, StopWatch stopWatch)
    {
        stopWatch.stop();
        var time = stopWatch.getTotalTimeSeconds();

        var key = key1 + "-NEO4j";
        if(!TemposNeo4jSegundos.containsKey(key))
            TemposNeo4jSegundos.put(key, new ArrayList<>());
        TemposNeo4jSegundos.get(key).add(time);
    }

    public static void putTimeConnector(String key1, StopWatch stopWatch)
    {
        stopWatch.stop();
        var time = stopWatch.getTotalTimeSeconds();
        var key = key1 + "-CONNECTOR";
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
        report.append("TOTAL;MINUTOS;SEGUNDOS;OVERHEAD(%);OVERHEAD(Segundos)" + "\n");
        report.append("GERAL;" + PrintInMinuts(TotalSegundos) + ";" + printNumber(TotalSegundos) + ";" + PrintOverHead(TotalSegundos, totalSecondsConnector) + "\n");
        report.append("CONNECTOR" + ";" + PrintInMinuts(totalSecondsConnector) + ";" + printNumber(totalSecondsConnector) + ";" +  PrintOverHead(totalSecondsConnector , totalSecondsNeo4j) + "\n");
        report.append("NEO4J" + ";" + PrintInMinuts(totalSecondsNeo4j) + ";" + printNumber(totalSecondsNeo4j) + "\n");

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

        System.out.println("EXECUÇÃO FINALIZADA: " + filePath);
        // Clear Reports
        TotalSegundos = 0l;
        TemposNeo4jSegundos = new HashMap<>();
        TemposConnectorSegundos = new HashMap<>();

    }
    private static String PrintInMinuts(double value)
    {
        return printNumber(value/ 60);
    }

    private static String PrintOverHead(double total, double subTotal)
    {
        var percentual =  printNumber(total/ subTotal  -1);
        var diferenca =  printNumber(total- subTotal);

        return percentual + ";" + diferenca;
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

    public static StopWatch CreateAndStartStopwatch()
    {
        var stopwatch = new org.springframework.util.StopWatch();
        stopwatch.start();
        return stopwatch;
    }
}
