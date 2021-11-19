package com.lisa.sqltokeynosql.api.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lisa.sqltokeynosql.api.dto.CurrentDataBaseRequestDTO;
import com.lisa.sqltokeynosql.api.dto.NoSqlTargetDTO;
import com.lisa.sqltokeynosql.api.dto.SQLDTO;
import com.lisa.sqltokeynosql.architecture.Parser;
import com.lisa.sqltokeynosql.util.DataSet;
import com.lisa.sqltokeynosql.util.NoSQL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
public class SQLController {

    @Autowired
    private Parser parser;

    @Autowired
    private ObjectMapper mapper;

    @PostMapping(value = "/query", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> query(@RequestBody SQLDTO query) {
        return ResponseEntity.ok(parser.run(query.getValue()).map(this::getAsString)
                .orElse("No value found."));
    }



    @PostMapping(value = "/currentDataBase", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> choseCurrentDataBase(@RequestBody CurrentDataBaseRequestDTO currentDataBase) {
        parser.changeCurrentDB(currentDataBase.getName());
        return ResponseEntity.ok(currentDataBase.getName());
    }

    @GetMapping(value = "/currentDataBase", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getCurrentDataBase() throws IOException {
        return ResponseEntity.ok(mapper.writeValueAsString(parser.getCurrentDataBase()));
    }

    @GetMapping(value = "/listDbs", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> listDbs() throws IOException {
        return ResponseEntity.ok(mapper.writeValueAsString(parser.getRdbms()));
    }

    @PostMapping(value = "/noSqlTarget", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> createNoSqlTarget(@RequestBody NoSqlTargetDTO noSqlTargetDTO) throws IOException {
        NoSQL noSQL = new NoSQL(noSqlTargetDTO.getName(),
                            noSqlTargetDTO.getUser(),
                        noSqlTargetDTO.getPassword(),
                            noSqlTargetDTO.getUrl(),
                        noSqlTargetDTO.getConnector().getValue());
        parser.addNoSqlTarget(noSQL);
        return ResponseEntity.ok(mapper.writeValueAsString(noSQL));
    }

    @GetMapping(value = "/noSqlTargets", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> listNoSqlTargets() throws IOException {
        return ResponseEntity.ok(
                mapper.writeValueAsString(parser.getNoSqlTargets())
        );
    }

    private String getAsString(Object object) {
        try {
            return mapper.writeValueAsString(object);
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}