await createConnectionSgbdAsync();
await createEDefineDatabaseAsync();
await runQuery();

async function createConnectionSgbdAsync()
{
    await fetch("/no-sql-targets")
        .then(res => res.json())
        .then(async targets =>
        {

            const neo4j = {
                "connector": "NEO4J",
                "name": "neo4j",
                "password": "pAsSw0rD",
                "url": "bolt://localhost:7687",
                "user": "neo4j"
            };
            let createConnection = true;
            if(targets !== [] && targets !== null && targets !== undefined && targets.length !== 0)
                createConnection = targets.find(x=> x.connector === neo4j.connector) !== undefined;

            if(createConnection)
            {
                const request = new Request("/no-sql-target",
                    {
                        method: 'POST',
                        body: JSON.stringify(neo4j),
                        headers: new Headers(
                            {
                                'Content-Type': 'application/json'
                            })
                    });

                await fetch(request)
                    .catch(error =>
                    {
                        alert("HTTP-Error: " + error.status);
                    });
            }

        }).catch(error =>
        {
            alert("HTTP-Error: " + error.status);
        });
}

async function createEDefineDatabaseAsync()
{
    const request = new Request("/current-database",
        {
            method: 'POST',
            body: JSON.stringify({
                "name": "bd_matConstru"
            }),
            headers: new Headers(
                {
                    'Content-Type': 'application/json'
                })
        });

    await fetch(request)
        .catch(error =>
        {
            alert("HTTP-Error: " + error.status);
        });
}

async function runQuery()
{
    const request = new Request("/query",
        {
            method: 'POST',
            body: JSON.stringify({
                "value": "create table funcao(id_funcao int not null primary key auto_increment,desc_funcao varchar(50),salario double,carga_horaria varchar(30));"
            }),
            headers: new Headers(
                {
                    'Content-Type': 'application/json'
                })
        });

    await fetch(request)
        .catch(error =>
        {
            alert("HTTP-Error: " + error.status);
        });
}