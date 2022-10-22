using System.Diagnostics;
using GeradorSQL.Enums;
using GeradorSQL.Services;
using GeradorSQL.Utils;

while (true)
{
    Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
    Console.WriteLine("GERADOR SQL BANCO DB_MATCONSTRU");
    Console.WriteLine("-----------------------------------------------------------------------------------------------------------");

    var linhas = ConsoleUtils.LerQuantidadeConsultas("CONSULTAS");
    
    Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
    Console.WriteLine("Tipo de Consulta");
    foreach (var consulta in Enum.GetValues<Consulta>())
        Console.WriteLine($"[{(int)consulta}] {consulta.GetDisplayName()}");
    Console.WriteLine("-----------------------------------------------------------------------------------------------------------");

    Consulta tipoConsulta;
    var digitadoTipo = Console.ReadLine()?.Replace("_", "");
    while (!Enum.TryParse(digitadoTipo, out tipoConsulta))
    {
        Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
        Console.WriteLine("HUMMMM, parece que você não digitou um número. Vamos tentar de novo. A gente aceita esse formato 1_000_000");
        Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
        Console.WriteLine("Quantidade CONSULTAS?");
        digitadoTipo = Console.ReadLine()!.Replace("_", "");
    }

    
    
    Console.WriteLine("Gerando consultas...");
    var stop = new Stopwatch();
    stop.Start();
    
    
    var file = await ConsultasService.GerarAsync(tipoConsulta, linhas);

    
    Console.WriteLine();
    Console.WriteLine(stop.Elapsed.ToString());
    Console.WriteLine($"ARQUIVO: {file.FullName}");
    
    Process.Start("explorer.exe", file.FullName);
}

