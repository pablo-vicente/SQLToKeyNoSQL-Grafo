using System.Diagnostics;
using System.Globalization;
using System.Net.Http.Headers;
using System.Text.Json;
using AnaliseResultado;

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

//      CREATE
//      INSERT
// SELECT
//      UPDATE
//      ALTER TABLE
//      DELETE
//      INSERT
//      DROP TABLE:

#endregion

var interacoes = 100;
var cultura = CultureInfo.CreateSpecificCulture("pt-BR");

var baseProject = AppDomain.CurrentDomain.BaseDirectory;
var baseDirect = Path.Combine(baseProject, "SCRIPTS");
var relatorioName = Path.Combine(baseProject, $"RESULTADOS-{DateTime.Now:yyyy-MM-DD HH-mm-ss}.csv");
var relatorio = new StreamWriter(relatorioName);

var drop = new FileInfo(Path.Combine(baseDirect, "bd_matConstru_DDL_DROP.sql"));
var create = new FileInfo(Path.Combine(baseDirect, "bd_matConstru_DDL_CREATE.sql"));
var alter = new FileInfo(Path.Combine(baseDirect, "bd_matConstru_DDL_ALTER.sql"));



var httpCliente = new HttpClient
{
    BaseAddress = new Uri("http://localhost:8080/"),
};

await LimparBaseAsync(httpCliente, drop);

for (var execucao = 1; execucao <= interacoes; execucao++)
{
    var timeResponse = await RodarScriptAsync(httpCliente, create);
    await SalvarNoRelatorioAsync(timeResponse, "CREATE", string.Empty, execucao); 
    await RodarScriptAsync(httpCliente, drop);
}

await relatorio.FlushAsync();
Process.Start("explorer.exe", relatorioName);

async Task<TimeResponse> RodarScriptAsync(HttpClient httpClient, FileSystemInfo file)
{
    using var multipartFormContent = new MultipartFormDataContent();
    var fileStreamContent = new StreamContent(new FileStream(file.FullName, FileMode.Open));
    fileStreamContent.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
    multipartFormContent.Add(fileStreamContent, name: "file", fileName: file.Name);
    var response = await httpClient.PostAsync("/query-file-sql-script", multipartFormContent);
    
    response.EnsureSuccessStatusCode();
    var readAsStringAsync = await response.Content.ReadAsStringAsync();

    var options = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true
    };
    
    return JsonSerializer.Deserialize<Report>(readAsStringAsync, options).TimerResponse;
}

async Task SalvarNoRelatorioAsync(TimeResponse response, string comando, string cenario, int execucao)
{
    await relatorio
        .WriteLineAsync("COMANDO;" +
                        "CENARIO;" +
                        "EXECUCAO;" +
                        $"{nameof(TimeResponse.TempoCamada)};" +
                        $"{nameof(TimeResponse.TempoConector)};" +
                        $"{nameof(TimeResponse.TempoNeo4j)}");
    
    await relatorio
        .WriteLineAsync(
            $"{comando};" +
            $"{cenario};" +
            $"{execucao};" +
            $"{response.TempoCamada.ToString(cultura)};" +
            $"{response.TempoConector.ToString(cultura)};" +
            $"{response.TempoNeo4j.ToString(cultura)}");
}

async Task LimparBaseAsync(HttpClient httpCliente1, FileInfo fileInfo)
{
    try
    {
        await RodarScriptAsync(httpCliente1, fileInfo);
    }
    catch (Exception ex)
    {
        // LIMPEZA DA BASE
        Console.WriteLine(ex);
    }
}