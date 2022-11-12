var dataSets = [];
var tabelaResultados;

events();
getConnectores();
getDatabases();


// GET  /connectors OK
// GET  /current-database OK
// POST /current-database OK
// GET  /databases OK
// POST /no-sql-target
// GET  /no-sql-targets
// POST /query OK
// POST /query-file-sql-script OK

function events()
{
    document
        .querySelector('#select-nome-db-existente')
        .addEventListener('change', async (e) => {
            e.preventDefault();
            const select = document.getElementById("select-nome-db-existente");
            const database = select.options[select.selectedIndex].value;

            await postCurrentDatabase(database);
        });

    document
        .querySelector('#btn-executar-query')
        .addEventListener('click', async (e) =>
        {
            e.preventDefault();
            document.getElementById('loading').style.display = '';
            document.getElementById('select-tabelas').style.display = 'none';
            document.getElementById('sem-dados').style.display = 'none';
            document.getElementById('div-tabela').style.display = 'none';
            document.getElementById('timer').value = '';
            await runQuery();
        });

    document
        .querySelector('#select-resultado-tabelas')
        .addEventListener('change', async (e) =>
        {
            e.preventDefault();
            const select = document.getElementById("select-resultado-tabelas");
            const tableIndex = select.options[select.selectedIndex].value;
            document.getElementById('div-tabela').style.display = 'none';
            document.getElementById('loading').style.display = '';
            renderTable(tableIndex)
        });
}

async function runQuery()
{
    function putTimer(time)
    {
        const timeSeconds = time.toLocaleString('pt-BR',
            {
                minimumFractionDigits: 4,
                maximumFractionDigits: 4
            })

        const timeMinutes = (time / 60).toLocaleString('pt-BR',
            {
                minimumFractionDigits: 4,
                maximumFractionDigits: 4
            })

        const timeHors = (time / 60 / 60).toLocaleString('pt-BR',
            {
                minimumFractionDigits: 4,
                maximumFractionDigits: 4
            })

        document.getElementById('timer').value = `${timeSeconds}s | ${timeMinutes}m | ${timeHors}h`
    }

    let file = document.getElementById("formFile").files[0];
    const textArea = document.getElementById('sql-query-text');

    if(file === undefined || file === null || file === '')
    {
        const queryText = textArea.value;
        const blobObject = new Blob([queryText], {type: 'text/plain'});
        file = new File([blobObject], 'query.sql', {type: 'text/plain'});
    }
    else
        textArea.value = '';

    const formData = new FormData();
    formData.append('file', file);

    await fetch('/query-file-sql-script', {
        method: 'POST',
        body: formData
    }).then(async res =>
    {
        document.getElementById('formFile').value = "";
        if(!res.ok)
        {
            document.getElementById('loading').style.display = 'none'
            return await handleErro(res);
        }

        const result = await res.json();
        putTimer(result.TimerResponse.TempoCamada);
        dataSets = result.DataSets;

        const select = document.querySelector("#select-resultado-tabelas");
        select.options.length = 0;

        for (let i = 0; i < result.DataSets.length; i++)
        {
            const table = result.DataSets[i];

            const opt = document.createElement('option');
            opt.value = i;
            opt.text = table.tableName.toUpperCase();
            select.appendChild(opt);
        }
        renderTable(0);
    });
}

function renderTable(indexTabela)
{
    document.getElementById('sem-dados').style.display = 'none';
    document.getElementById('div-tabela').style.display = 'none';
    document.getElementById('loading').style.display = '';
    if(dataSets.length === 0)
    {
        document.getElementById('sem-dados').style.display = '';
        document.getElementById('select-tabelas').style.display = 'none';
        document.getElementById('div-tabela').style.display = 'none';
        document.getElementById('loading').style.display = 'none';
        return;
    }

    const data = dataSets[indexTabela];
    const columns = data.columns.map(function (column) {return {title: column.toUpperCase()}});
    const lines = data.data;

    tabelaResultados = $('#table-resultados').DataTable({
        language: {
            url: 'pt-BR.json'
        },
        fixedColumns: true,
        deferRender:    true,
        scrollY:        '60vh',
        scrollCollapse: true,
        scroller:       true,
        scrollX:        true,
        stateSave:      true,
        select:         true,
        data: lines,
        columns: columns,
        columnDefs: [{
            targets: '_all',
            className: 'dt-head-center dt-body-center'
        }]
    });

    new $.fn.dataTable.FixedColumns(tabelaResultados);

    setTimeout(() =>
    {
        document.getElementById('sem-dados').style.display = 'none';
        document.getElementById('div-tabela').style.display = '';
        document.getElementById('select-tabelas').style.display = '';
        document.getElementById('loading').style.display = 'none';
        tabelaResultados.draw();
    }, 5);

}

async function postCurrentDatabase(database)
{
    await fetch("/current-database",
        {
            method: 'POST',
            body: JSON.stringify(
                {
                    "name": database
                }),
            headers: new Headers(
                {
                    'Content-Type': 'application/json'
                })
        })
        .then(async res =>
        {
            if(!res.ok)
                await handleErro(res)
        })
}

async function getDatabases()
{
    return await fetch("/databases",
        {
            method: 'GET',
        })
        .then(async res =>
        {
            if (!res.ok)
                return await handleErro(res)

            const databases = await res.json();
            const select = document.querySelector("#select-nome-db-existente");
            select.options.length = 0;

            databases.forEach(database =>
            {
                const opt = document.createElement('option');
                opt.value = database.name;
                opt.text = database.name
                opt.selected = false;
                select.appendChild(opt)
            });
        })
        .then(async () => await getCurrenteDatabase())
}

async function getCurrenteDatabase()
{
    return await fetch("/current-database",
        {
            method: 'GET',
        })
        .then(async res =>
        {
            if (!res.ok)
                return await handleErro(res)

            const currentDatabase = await res.json();

            const select = document.querySelector("#select-nome-db-existente");
            select.value = currentDatabase.name
        })
}

async function getConnectores()
{
    return await fetch("/connectors",
        {
            method: 'GET',
        })
        .then(async res =>
        {
            if (!res.ok)
                return await handleErro(res)

            const connectors = await res.json();
            const select = document.querySelector("#select-target-nosql");
            select.options.length = 0;

            connectors.forEach(connector =>
            {
                const opt = document.createElement('option');
                opt.value = connector;
                opt.text = connector
                opt.selected = false;
                select.appendChild(opt)
            });

        })
}

async function handleErro(res) {
    let response = await res.json();
    console.log(response)
    document.getElementById('code').innerText = response;
    const myModalEl = document.querySelector('#model-alert');
    const modal = bootstrap.Modal.getOrCreateInstance(myModalEl);
    modal.show();
}