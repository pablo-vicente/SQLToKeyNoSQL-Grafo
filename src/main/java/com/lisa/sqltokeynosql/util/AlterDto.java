package com.lisa.sqltokeynosql.util;

import net.sf.jsqlparser.statement.alter.AlterOperation;

public class AlterDto
{
    public AlterDto(AlterOperation alterOperation, String colunaExistente, String colunaNova)
    {
        AlterOperation = alterOperation;
        ColunaExistente = colunaExistente;
        ColunaNova = colunaNova;
    }

    public final AlterOperation AlterOperation;
    public final String ColunaExistente;
    public final String ColunaNova;
}
