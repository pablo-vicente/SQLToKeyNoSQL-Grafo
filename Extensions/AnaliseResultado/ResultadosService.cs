using System.Globalization;
using System.Net.Http.Headers;
using System.Text.Json;

namespace AnaliseResultado;

public static class ResultadosService
{
    public static readonly CultureInfo Cultura = CultureInfo.CreateSpecificCulture("pt-BR");

    public static async Task RodarScripSalvartAsync(
        HttpClient httpClient,
        (FileSystemInfo Arquivo, string Comando) instrucao,
        TextWriter relatorio,
        string cenario,
        int execucao)
    {
        var timerResponse = await RodarScriptAsync(httpClient, instrucao.Arquivo);
        await SalvarNoRelatorioAsync(relatorio, timerResponse, instrucao.Comando, cenario, execucao);
    }

    private static async Task<TimeResponse> RodarScriptAsync(
        HttpClient httpClient, FileSystemInfo arquivo)
    {
        using var multipartFormContent = new MultipartFormDataContent();
        var fileStreamContent = new StreamContent(new FileStream(arquivo.FullName, FileMode.Open));
        fileStreamContent.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
        multipartFormContent.Add(fileStreamContent, name: "file", fileName: arquivo.Name);
        var response = await httpClient.PostAsync("/query-file-sql-script", multipartFormContent);
        
        response.EnsureSuccessStatusCode();
        var readAsStringAsync = await response.Content.ReadAsStringAsync();
    
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };
        
        return JsonSerializer.Deserialize<Report>(readAsStringAsync, options).TimerResponse;
    }

    public static async Task<StreamWriter> CriarRelatorio(FileInfo relatorioName)
    {
        if(!relatorioName.Directory!.Exists)
            relatorioName.Directory.Create();
        
        var relatorio = new StreamWriter(relatorioName.FullName);
        await relatorio
            .WriteLineAsync("COMANDO;" +
                            "CENARIO;" +
                            "EXECUCAO;" +
                            $"{nameof(TimeResponse.TempoCamada)};" +
                            $"{nameof(TimeResponse.TempoConector)};" +
                            $"{nameof(TimeResponse.TempoNeo4j)}");

        return relatorio;
    }

    private static async Task SalvarNoRelatorioAsync(
        TextWriter relatorio, 
        TimeResponse response, 
        string comando, 
        string cenario, 
        int execucao)
    {
        await relatorio
            .WriteLineAsync(
                $"{comando};" +
                $"{cenario};" +
                $"{execucao};" +
                $"{response.TempoCamada.ToString(Cultura)};" +
                $"{response.TempoConector.ToString(Cultura)};" +
                $"{response.TempoNeo4j.ToString(Cultura)}");
    }
    
    public static async Task LimparBaseAsync(HttpClient httpCliente1, FileSystemInfo fileInfo)
    {
        try
        {
            await RodarScriptAsync(httpCliente1, fileInfo);
        }
        catch (Exception)
        {
            // ignored
        }
    }

    
}