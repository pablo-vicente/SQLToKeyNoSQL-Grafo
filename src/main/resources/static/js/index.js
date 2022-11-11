var dataSets = []

function modal() {
    var myModalEl = document.querySelector('#model-alert')
    var modal = bootstrap.Modal.getOrCreateInstance(myModalEl) // Returns a Bootstrap modal instance
    modal.show();
}

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

            document.getElementById('resultados').style.display = 'none';
            document.getElementById('sem-dados').style.display = 'none';
            document.getElementById('div-tabela-resultados').style.display = 'none';


            await fetch('/query-file-sql-script', {
                method: 'POST',
                body: formData
            }).then(async res =>
            {
                document.getElementById('formFile').value = "";
                if(!res.ok)
                    await handleErro(res);

                const result = await res.json();
                putTimer(result.TimerResponse.TempoCamada);
                dataSets = result.DataSets;

                const select = document.querySelector("#select-resultado-tabelas");
                select.options.length = 0;

                for (let i = 0; i < result.DataSets.length; i++)
                {
                    const table = result.DataSets[0];

                    const opt = document.createElement('option');
                    opt.value = i;
                    opt.text = table.tableName;
                    select.appendChild(opt);
                }
                createTable(0);
            })

        });
}

function createTable(indexTabela)
{
    document.getElementById('resultados').style.display = 'block';
    if(dataSets.length === 0)
    {
        document.getElementById('sem-dados').style.display = 'block';
        return;
    }

    const data = dataSets[indexTabela];
    document.getElementById('table-resultados').innerHTML = '';
    document.getElementById('table-resultados').innerHTML = `<thead>
                        <tr>
                            ${data.columns.map(x => `<th scope="col">${x}</th>`).join("")}
                        </tr>
                    </thead>
                    <tbody>
                        ${data.data.map(row => `<tr>${row.map(column => `<td>${column}</td>`).join("")}</tr>`).join("")}
                    </tbody>`;

    document.getElementById('div-tabela-resultados').style.display = 'block';
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