package com.lisa.sqltokeynosql.api.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.springframework.http.MediaType.*;

@RestController
public class SQLController {

    @Autowired
    private Parser parser;

    @Autowired
    private ObjectMapper mapper;

    private ResponseEntity<String> handleException(Exception ex) throws JsonProcessingException
    {
        Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
        return ResponseEntity.badRequest().body(mapper.writeValueAsString(ex.getMessage()));
    }

    @GetMapping(value = "/connectors", produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Get all SGBDs supported.")
    public ResponseEntity<String> listConnectors() throws IOException
    {
        try
        {
            var connectors = new ArrayList<>(Arrays.asList(Connector.values()));
            return ResponseEntity.ok(mapper.writeValueAsString(connectors));
        }
        catch (Exception ex)
        {
            return handleException(ex);
        }
    }

    @PostMapping(value = "/no-sql-target", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Create the SGDB Target.")
    public ResponseEntity<String> createNoSqlTarget(@RequestBody NoSqlTargetDTO noSqlTargetDTO) throws IOException
    {
        try
        {
            NoSQL noSQL = new NoSQL(noSqlTargetDTO.getName(),
                    noSqlTargetDTO.getUser(),
                    noSqlTargetDTO.getPassword(),
                    noSqlTargetDTO.getUrl(),
                    noSqlTargetDTO.getConnector());
            parser.addNoSqlTarget(noSQL);
            return ResponseEntity.ok(mapper.writeValueAsString(noSQL));
        }
        catch (Exception ex)
        {
            return handleException(ex);
        }
    }

    @GetMapping(value = "/no-sql-targets", produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Get all SGBDs previously registered.")
    public ResponseEntity<String> listNoSqlTargets() throws IOException
    {
        try
        {
            return ResponseEntity.ok(mapper.writeValueAsString(parser.getNoSqlTargets()));
        }
        catch (Exception ex)
        {
            return handleException(ex);
        }

    }

    @PostMapping(value = "/current-database", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Create a databse if is not exist and set as current database.")
    public ResponseEntity<String> choseCurrentDataBase(@RequestBody CurrentDataBaseRequestDTO currentDataBase) throws JsonProcessingException
    {
        try
        {
            parser.changeCurrentDB(currentDataBase.getName());
            return ResponseEntity.ok(currentDataBase.getName());
        }
        catch (Exception ex)
        {
            return handleException(ex);
        }

    }

    @GetMapping(value = "/current-database", produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Get the current database.")
    public ResponseEntity<String> getCurrentDataBase() throws IOException
    {
        try
        {
            return ResponseEntity.ok(mapper.writeValueAsString(parser.getCurrentDataBase()));
        }
        catch (Exception ex)
        {
            return handleException(ex);
        }

    }

    @GetMapping(value = "/databases", produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Get all database created.")
    public ResponseEntity<String> listDbs() throws IOException
    {
        try
        {
            return ResponseEntity.ok(mapper.writeValueAsString(parser.getRdbms()));
        }
        catch (Exception ex)
        {
            return handleException(ex);
        }

    }


    @PostMapping(value = "/query", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Run SQL Queries.")
    public ResponseEntity<String> query(@RequestBody SQLDTO query) throws JsonProcessingException
    {
        try
        {
            var value = query.getValue();
            var result = parser.run(value);
            return ResponseEntity.ok(mapper.writeValueAsString(result));
        }
        catch (Exception ex)
        {
            return handleException(ex);
        }

    }

    @PostMapping(value = "/query-file-sql-script", produces = APPLICATION_JSON_VALUE, consumes = MULTIPART_FORM_DATA_VALUE)
    @ApiOperation(value = "Run File Script SQL.")
    public ResponseEntity<String> saveProfileImage(@RequestPart("file") MultipartFile file) throws JsonProcessingException
    {
        try
        {
            var query = new String(file.getBytes());
            var result = parser.run(query);
            return ResponseEntity.ok(mapper.writeValueAsString(result));
        }
        catch (Exception ex)
        {
            return handleException(ex);
        }
    }
}