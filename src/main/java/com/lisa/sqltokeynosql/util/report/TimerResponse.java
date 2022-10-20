package com.lisa.sqltokeynosql.util.report;

public class TimerResponse
{
    public TimerResponse(Double tempoCamada, Double tempoConector, Double tempoNeo4j)
    {
        TempoCamada = tempoCamada;
        TempoConector = tempoConector;
        TempoNeo4j = tempoNeo4j;
    }

    public final Double TempoCamada;
    public final Double TempoConector;
    public final Double TempoNeo4j;
}
