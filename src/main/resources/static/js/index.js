var dataSets = [];
var tabelaResultados;
var noSqlTargets = [];

events();
getConnectores();
getDatabases();

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

            insertLoadingButton('btn-executar-query');
            await runQuery();
            document.getElementById('loading').style.display = 'none'
            removeLoadingButton('btn-executar-query');
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

    document
        .querySelector('#form-target-nosql')
        .addEventListener('submit', async (e) =>
        {
            e.preventDefault();

            insertLoadingButton('salvar-target');
            const target = await createUpdateNoSqlTarget();
            const database = await createDatabase();
            const currente = await getDatabases();

            Promise
                .all([target, database, currente])
                .then(() =>
                {
                    removeLoadingButton('salvar-target');
                })
        });

    document
        .querySelector('#select-target-nosql')
        .addEventListener('change', async (e) =>
        {
            e.preventDefault();
            const connector = document.querySelector('#select-target-nosql').value;
            const noSqlTarget = noSqlTargets.find(x=> x.connector === connector);

            if(noSqlTarget === null || noSqlTarget === undefined)
                setCredencialTargetNoSql('', '', '')
            else
                setCredencialTargetNoSql(noSqlTarget.user, noSqlTarget.password, noSqlTarget.url)
        });

    document
        .querySelector('#editar-target')
        .addEventListener('click', async (e) =>
        {
            e.preventDefault();
            enableDisableTargetInputs(false);
            e.target.style.display = 'none';
        });
}

async function createDatabase()
{
    const database = document.querySelector('#nome-db-novo').value;

    if(database === undefined || database === null || database === "")
        return;

    return await fetch("/database",
        {
            method: 'POST',
            body: JSON.stringify(
                {
                    "name": database,
                    "connector": document.querySelector('#select-target-nosql').value
                }),
            headers: new Headers(
                {
                    'Content-Type': 'application/json'
                })
        })
        .then(async res =>
        {
            if(!res.ok)
                showModal(await res.json());

            document.querySelector('#nome-db-novo').value = '';
        })
}

async function createUpdateNoSqlTarget()
{
    const user = document.querySelector("#target-nosql-usuario");
    const password = document.querySelector("#target-nosql-senha");
    const url = document.querySelector("#target-nosql-url");

    if (user.disabled && password.disabled && url.disabled)
        return;

    const obj = {
        "connector": document.querySelector("#select-target-nosql").value,
        "name": document.querySelector("#select-target-nosql").value,
        "user": user.value,
        "password": password.value,
        "url": url.value,
    };

    return await fetch("/no-sql-target",
        {
            method: 'POST',
            body: JSON.stringify(obj),
            headers: new Headers(
                {
                    'Content-Type': 'application/json'
                })
        })
        .then(async res =>
        {
            if(!res.ok)
                return showModal(await res.json());
        })
}

async function getNosqlTargets()
{
    return await fetch("/no-sql-targets")
        .then(async res =>
        {
            if(!res.ok)
                return showModal(await res.json());

            return await res.json();
        })
}

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

async function runQuery()
{
    debugger

    if(document.querySelector('#select-nome-db-existente').options.length === 0 )
    {
        showModal("Não foi definido um banco de dados!");
        return;
    }

    let file = document.getElementById("formFile").files[0];
    const textArea = document.getElementById('sql-query-text');

    if(file === undefined || file === null || file === '')
    {
        const queryText = textArea.value;

        if(queryText.trim() === '')
        {
            showModal("Inclua um arquivo ou escreva uma expressão SQL");
            return;
        }

        const blobObject = new Blob([queryText], {type: 'text/plain'});
        file = new File([blobObject], 'query.sql', {type: 'text/plain'});
    }
    else
        textArea.value = '';

    const formData = new FormData();
    formData.append('file', file);

    return await fetch('/query-file-sql-script', {
        method: 'POST',
        body: formData
    }).then(async res =>
    {
        document.getElementById('formFile').value = "";
        if(!res.ok)
            return showModal(await res.json());

        const result = await res.json();
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
        putTimer(result.TimerResponse.TempoCamada);
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

    if (tabelaResultados !== null && tabelaResultados !== undefined)
    {
        tabelaResultados.clear();
        tabelaResultados.destroy();
    }

    const tableId = 'table-resultados-' + indexTabela;

    document.querySelector('#div-tabela')
        .innerHTML =    `<table 
                            id="${tableId}" 
                            class="table table-striped table-bordered nowrap" 
                            style="width: 100%">
                        </table>`;


    tabelaResultados = $('#' + tableId).DataTable({
        language: {
            url: 'pt-BR.json'
        },
        destroy: true,
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
                showModal(await res.json());
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
                return showModal(await res.json());

            const databases = await res.json();
            const select = document.querySelector("#select-nome-db-existente");
            select.options.length = 0;

            databases.forEach(database =>
            {
                const opt = document.createElement('option');
                opt.value = database.name;
                opt.text = `${database.name} (${database.targetDB.connector})`;
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
                return showModal(await res.json());

            const currentDatabase = await res.json();

            if(currentDatabase === null || currentDatabase === undefined)
                return ;
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
                return showModal(await res.json());

            const connectors = await res.json();
            const select = document.querySelector("#select-target-nosql");
            select.options.length = 0;

            connectors.forEach(connector =>
            {
                const opt = document.createElement('option');
                opt.value = connector;
                opt.text = connector
                opt.selected = connector === 'NEO4J';
                opt.disabled = connector !== 'NEO4J' && connector !== "MONGO";
                select.appendChild(opt)
            });

            noSqlTargets = await getNosqlTargets();

            if(noSqlTargets.length === 0)
            {
                setCredencialTargetNoSql('', '', '')
                return;
            }

            const noSqlTarget = noSqlTargets[0];
            select.value = noSqlTarget.connector;
            setCredencialTargetNoSql(noSqlTarget.user, noSqlTarget.password, noSqlTarget.url)
        })
}

function enableDisableTargetInputs(disabled)
{
    document.getElementById('target-nosql-usuario').disabled = disabled;
    document.getElementById('target-nosql-senha').disabled = disabled;
    document.getElementById('target-nosql-url').disabled = disabled;
}

function setCredencialTargetNoSql(user, password, url)
{
    document.querySelector("#target-nosql-usuario").value = user;
    document.querySelector("#target-nosql-senha").value = password;
    document.querySelector("#target-nosql-url").value = url;
    if(user === '' && password === '' && url === '')
    {
        enableDisableTargetInputs(false)
        document.getElementById('editar-target').style.display = 'none';
    }
    else
    {
        enableDisableTargetInputs(true)
        document.getElementById('editar-target').style.display = '';
    }
}

function showModal(message) {
    console.log(message)
    document.getElementById('code').innerText = message;
    const myModalEl = document.querySelector('#model-alert');
    const modal = bootstrap.Modal.getOrCreateInstance(myModalEl);
    modal.show();
}

function insertLoadingButton(id)
{
    const button = document.getElementById(id);
    button.disabled = true;
    const value = button.innerText;
    button.innerHTML = `<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>&nbsp;${value}`;
}

function removeLoadingButton(id)
{
    const button = document.getElementById(id);
    button.disabled = false;
    document.querySelector('#' + id + ' span').remove();
    button.innerText = button.innerText.trim();
}