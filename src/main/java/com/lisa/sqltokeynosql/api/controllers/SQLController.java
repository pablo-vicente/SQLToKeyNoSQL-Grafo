package com.lisa.sqltokeynosql.api.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lisa.sqltokeynosql.api.dto.CurrentDataBaseRequestDTO;
import com.lisa.sqltokeynosql.api.dto.NoSqlTargetDTO;
import com.lisa.sqltokeynosql.api.dto.SQLDTO;
import com.lisa.sqltokeynosql.api.enums.Connector;
import com.lisa.sqltokeynosql.architecture.Parser;
import com.lisa.sqltokeynosql.util.NoSQL;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
public class SQLController {

    @Autowired
    private Parser parser;

    @Autowired
    private ObjectMapper mapper;

    @PostMapping(value = "/query", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Run SQL Queries, only one for request.")
    public ResponseEntity<String> query(@RequestBody SQLDTO query) {
        return ResponseEntity.ok(parser.run(query.getValue()).map(this::getAsString)
                .orElse("No value found."));
    }

    @PostMapping(value = "/currentDataBase", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Set the current database.")
    public ResponseEntity<String> choseCurrentDataBase(@RequestBody CurrentDataBaseRequestDTO currentDataBase) {
        parser.changeCurrentDB(currentDataBase.getName());
        return ResponseEntity.ok(currentDataBase.getName());
    }

    @GetMapping(value = "/currentDataBase", produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Get the current database.")
    public ResponseEntity<String> getCurrentDataBase() throws IOException {
        return ResponseEntity.ok(mapper.writeValueAsString(parser.getCurrentDataBase()));
    }

    @GetMapping(value = "/listDbs", produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Get all database previously registered.")
    public ResponseEntity<String> listDbs() throws IOException {
        return ResponseEntity.ok(mapper.writeValueAsString(parser.getRdbms()));
    }

    @PostMapping(value = "/noSqlTarget", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Create the SGDB Target.")
    public ResponseEntity<String> createNoSqlTarget(@RequestBody NoSqlTargetDTO noSqlTargetDTO) throws IOException {
        NoSQL noSQL = new NoSQL(noSqlTargetDTO.getName(),
                            noSqlTargetDTO.getUser(),
                        noSqlTargetDTO.getPassword(),
                            noSqlTargetDTO.getUrl(),
                noSqlTargetDTO.getConnector());
        parser.addNoSqlTarget(noSQL);
        return ResponseEntity.ok(mapper.writeValueAsString(noSQL));
    }

    @GetMapping(value = "/noSqlTargets", produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Get all SGBDs previously registered.")
    public ResponseEntity<String> listNoSqlTargets() throws IOException {
        return ResponseEntity.ok(
                mapper.writeValueAsString(parser.getNoSqlTargets())
        );
    }

    @GetMapping(value = "/connectors", produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Get all SGBDs supported.")
    public ResponseEntity<String> listConnectors() throws IOException
    {
        var connectors = new ArrayList<>(Arrays.asList(Connector.values()));
        return ResponseEntity.ok(mapper.writeValueAsString(connectors));
    }

    private String getAsString(Object object) {
        try {
            return mapper.writeValueAsString(object);
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}