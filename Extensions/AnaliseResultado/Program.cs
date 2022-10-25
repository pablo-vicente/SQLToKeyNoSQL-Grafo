using System.Diagnostics;
using AnaliseResultado;
using GeradorSQL.Enums;
using GeradorSQL.Seeds;
using GeradorSQL.Services;
using GeradorSQL.Utils;

#region Casos de Testes

// CREATE TABLE
// ALTER TABLE
// DROP TABLE:

// INSERT
// UPDATE
// DELETE
// SELECT

// CENARIOS
// Instruções com 5 mil itens (85 mil registros)
// Instruções com 25 mil itens (425 mil registros)
// Instruções com 50 mil itens (850 mil registros)
// Instruções com 500 mil itens (8.5 milhões de registros)
// -----------

//      CREATE TABLE
//      INSERT(N)
//      SELECT
//      UPDATE
//      DELETE
//      INSERT
//      ALTER TABLE
//      DROP TABLE

#endregion

Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
Console.WriteLine("ANALISADOR CONVERSOR NEO4J BANCO DB_MATCONSTRU");
Console.WriteLine("-----------------------------------------------------------------------------------------------------------");

var linhas = ConsoleUtils.LerQuantidadeConsultas("CONSULTAS");
var interacoes = ConsoleUtils.LerQuantidadeConsultas("INTERAÇÕES");

var baseProject = AppDomain.CurrentDomain.BaseDirectory;
var baseDirect = Path.Combine(baseProject, "SCRIPTS");

(FileInfo arquivo, string  nome) drop = (new FileInfo(Path.Combine(baseDirect, "bd_matConstru_DDL_DROP.sql")), "DROP");
var create = (new FileInfo(Path.Combine(baseDirect, "bd_matConstru_DDL_CREATE.sql")), "CREATE");
var alter = (new FileInfo(Path.Combine(baseDirect, "bd_matConstru_DDL_ALTER.sql")), "ALTER");
var delete = (new FileInfo(Path.Combine(baseDirect, "bd_matConstru_DML_DELETE.sql")), "DELETE");
var update = (new FileInfo(Path.Combine(baseDirect, "bd_matConstru_DML_UPDATE.sql")), "UPDATE");
var select = (new FileInfo(Path.Combine(baseDirect, "bd_matConstru_DML_SELECT.sql")), "SELECT");

var httpCliente = new HttpClient
{
    BaseAddress = new Uri("http://localhost:8080/"),
    Timeout = TimeSpan.FromHours(1)
};

Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
Console.WriteLine("LIMPANDO A BASE DE DADOS");
await ResultadosService.LimparBaseAsync(httpCliente, drop.arquivo);

var cenario = $"{linhas:G}({linhas * SeedsFactory.Count(Consulta.Insert):G})";
var relatorioName = new FileInfo(Path.Combine(baseProject, "RELATORIOS", $"CENARIO_{cenario}_{DateTime.Now:yyyy-MM-dd HH-mm-ss}.csv"));
var relatorio = await ResultadosService.CriarRelatorio(relatorioName);

Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
Console.WriteLine("GERANDO CONSULTAS NECESSÁRIAS");

var insert = (await ConsultasService.GerarAsync(Consulta.Insert, linhas), "INSERT");
var insertN = (await ConsultasService.GerarAsync(Consulta.InsertN, linhas), "INSERTN");


for (var execucao = 1; execucao <= interacoes; execucao++)
{
    Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
    // Console.Write("\r{0:P2}   ", $"EXECUÇÃO: {execucao:G}|{interacoes:G}");
    Console.WriteLine($"EXECUÇÃO: {execucao:G}/{interacoes:G}");
    await ResultadosService.RodarScripSalvartAsync(httpCliente, create, relatorio, cenario, execucao);
    await ResultadosService.RodarScripSalvartAsync(httpCliente, insert, relatorio, cenario, execucao);
    await ResultadosService.RodarScripSalvartAsync(httpCliente, select, relatorio, cenario, execucao);
    await ResultadosService.RodarScripSalvartAsync(httpCliente, update, relatorio, cenario, execucao);
    await ResultadosService.RodarScripSalvartAsync(httpCliente, delete, relatorio, cenario, execucao);
    await ResultadosService.RodarScripSalvartAsync(httpCliente, insertN, relatorio, cenario, execucao);
    await ResultadosService.RodarScripSalvartAsync(httpCliente, alter, relatorio, cenario, execucao);
    await ResultadosService.RodarScripSalvartAsync(httpCliente, drop, relatorio, cenario, execucao);
}

await relatorio.FlushAsync();
Process.Start("explorer.exe", relatorioName.FullName);