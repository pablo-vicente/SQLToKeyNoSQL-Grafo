await createConnectionSgbdAsync();
await createEDefineDatabaseAsync();

async function createConnectionSgbdAsync()
{
    await fetch("/no-sql-targets")
        .then(async res =>
        {
            if(!res.ok)
            {
                await handleErro(res)
                return
            };

            const targets = await res.json();

            const neo4j = {
                "connector": "NEO4J",
                "name": "neo4j",
                "password": "pAsSw0rD",
                "url": "bolt://localhost:7687",
                "user": "neo4j"
            };
            let createConnection = true;
            if(targets !== [] && targets !== null && targets !== undefined && targets.length !== 0)
                createConnection = targets.find(x=> x.connector === neo4j.connector) === undefined;

            if(!createConnection)
            {
                console.log("Conexão SGBD Já existe");
                return
            }

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
                .then(async res =>
                {
                    if(!res.ok)
                    {
                        await handleErro(res)
                        return
                    }
                    else
                        console.log("Conexão SGBD Executada com Sucesso");
                })
        })
}

async function handleErro(res)
{
    let response = await res.json();
    console.log(response)
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
        .then(async res =>
        {
            if(!res.ok)
                await handleErro(res)
            else
                console.log("Banco de dados Definido com sucesso");
        })
}

