using System.Diagnostics;
using GeradorSQL.Enums;
using GeradorSQL.Seeds;

while (true)
{
    Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
    Console.WriteLine("GERADOR SQL BANCO DB_MATCONSTRU");
    Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
    Console.WriteLine("Quantidade CONSULTAS?");
    var digitado = Console.ReadLine().Replace("_", "");

    int linhas;
    Consulta tipoConsulta;

    while (!int.TryParse(digitado, out linhas))
    {
        Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
        Console.WriteLine("HUMMMM, parece que você não digitou um número. Vamos tentar de novo. A gente aceita esse formato 1_000_000");
        Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
        Console.WriteLine("Quantidade CONSULTAS?");
        digitado = Console.ReadLine().Replace("_", "");
    }
    Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
    Console.WriteLine("Tipo de Consulta");
    foreach (var consulta in Enum.GetValues<Consulta>())
        Console.WriteLine($"[{(int)consulta}] {consulta.GetDisplayName()}");
    Console.WriteLine("-----------------------------------------------------------------------------------------------------------");

    var digitadoTipo = Console.ReadLine()?.Replace("_", "");
    while (!Enum.TryParse(digitadoTipo, out tipoConsulta))
    {
        Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
        Console.WriteLine("HUMMMM, parece que você não digitou um número. Vamos tentar de novo. A gente aceita esse formato 1_000_000");
        Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
        Console.WriteLine("Quantidade CONSULTAS?");
        digitadoTipo = Console.ReadLine().Replace("_", "");
    }

    Console.WriteLine("Gerando consultas...");

    var sufixo = tipoConsulta + "_" + linhas * Seeds.Count(tipoConsulta);

    var basePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Consultas");
    if (!Directory.Exists(basePath))
        Directory.CreateDirectory(basePath);
    
    var file = new FileInfo(Path.Combine(basePath, $"{sufixo}.sql"));
    var stop = new Stopwatch();
    stop.Start();
    await using var streamWriter = new StreamWriter(file.FullName);

    await Seeds
        .GenerateAsync(tipoConsulta, linhas, streamWriter)
        .ConfigureAwait(false);

    await streamWriter.FlushAsync();
    Console.WriteLine();
    Console.WriteLine(stop.Elapsed.ToString());
    Console.WriteLine($"ARQUIVO: {file.FullName}");
    
    Process.Start("explorer.exe", file.FullName);

}

