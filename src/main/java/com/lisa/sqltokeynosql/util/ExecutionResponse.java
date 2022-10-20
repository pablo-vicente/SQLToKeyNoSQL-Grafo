package com.lisa.sqltokeynosql.util;

import com.lisa.sqltokeynosql.util.report.TimerResponse;

import java.util.ArrayList;

public class ExecutionResponse
{
    public ExecutionResponse(ArrayList<DataSet> dataSets, com.lisa.sqltokeynosql.util.report.TimerResponse timerResponse)
    {
        DataSets = dataSets;
        TimerResponse = timerResponse;
    }
    public ArrayList<DataSet> DataSets;
    public TimerResponse TimerResponse;


}
