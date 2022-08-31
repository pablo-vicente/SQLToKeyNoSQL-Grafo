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

*  1 Criar Conexão com SGBD através da rota ```/noSqlTarget```
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
  





















