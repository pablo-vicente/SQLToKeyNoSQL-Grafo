package com.lisa.sqltokeynosql.web.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

//@org.springframework.stereotype.Controller
@Controller
public class QueryController {
    @RequestMapping("/")
    public String page(){
        return "html/index";
    }
}
