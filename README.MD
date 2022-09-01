# Rodando Programa

- Baixar [DOCKER](https://www.docker.com/)
- Rodar Arquivo ```docker-compose.yml```, instâncias dos bancos de dados utlizados (Mongo e Neo4j)
- MONGO: acessível através do [localhost:8081](http://localhost:8081) (utilizado para armazenar dicionario)
  - User: ``root``
  - PWS: ``root``
- NEO4J(Enterprise): acessível através da [localhost:7474](http://localhost:7474)
    - User: ``neo4j``
    - PWS: ``pAsSw0rD``
- Documentação da API SQLTONOKEYNOSQL: [SWAGER](http://localhost:8080/swagger)
- Recomendação: utilizar o [POSTAM](https://www.postman.com/) para os testes da API

## PASSOS

# 1 Criar Conexão com SGBD através da rota ```POST/no-sql-target```
> Dados de conexão Neo4j
```JSON
{
  "connector": "NEO4J",
  "name": "neo4j",
  "password": "pAsSw0rD",
  "url": "bolt://localhost:7687",
  "user": "neo4j"
}
```
> É possivel verificar criaçao através ```GET/no-sql-targets```

# 2 Definir o banco de dados através ```POST/current-database```
- Se o banco não existir será criado, se existir será apenas definido
```JSON
{
  "name": "bd_matConstru"
}
```
> É possivel verificar criaçao através ```GET/databases```
> È possível verificar se está definido através ```GET/current-database```

# 3 Definir o banco de dados através ```POST/current-database```






















