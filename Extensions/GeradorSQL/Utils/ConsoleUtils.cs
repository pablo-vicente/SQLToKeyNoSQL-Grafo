namespace GeradorSQL.Utils;

public static class ConsoleUtils
{
    public static int LerQuantidadeConsultas(string nome)
    {
        Console.WriteLine($"Quantidade {nome}?");
        var digitado = Console.ReadLine()!.Replace("_", "");
        int linhas;
        while (!int.TryParse(digitado, out linhas))
        {
            Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
            Console.WriteLine("HUMMMM, parece que você não digitou um número. Vamos tentar de novo. A gente aceita esse formato 1_000_000");
            Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
            Console.WriteLine($"Quantidade {nome}?");
            digitado = Console.ReadLine()!.Replace("_", "");
        }

        return linhas;
    }
}