package com.example.demo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SnowController {
		   
	   @Autowired  
	    JdbcTemplate jdbc;    
	    @RequestMapping(value = "/selectstudents", method = RequestMethod.GET)  
	    public List<Map<String, Object>> showDatabases(){  
	       return jdbc.queryForList("select * from students");
	    }
	    
	    @RequestMapping(value = "/listRows", method = RequestMethod.GET)  
	    public List<Map<String, Object>> listRows(@RequestParam(value="acname") String acname, @RequestParam(value="username") String username, 
	    		@RequestParam(value = "password") String password, @RequestParam(value="whouse") String whouse, @RequestParam(value="dbname") String dbname,
	    		@RequestParam(value="schema") String schema, @RequestParam(value="tabname") String tabname, @RequestParam(value="noofrows") String noofrows) throws SQLException{
		DriverManagerDataSource ds = new DriverManagerDataSource();
	    ds.setUrl("jdbc:snowflake://"+acname+".southeast-asia.azure.snowflakecomputing.com/?warehouse="+whouse+"&db="+dbname+"&schema="+schema);
	    ds.setDriverClassName("net.snowflake.client.jdbc.SnowflakeDriver");
	    ds.setUsername(username);
	    ds.setPassword(password);
	    JdbcTemplate jdbc = new JdbcTemplate(ds);
	    Connection conn = jdbc.getDataSource().getConnection();
	    return jdbc.queryForList("select * from "+tabname+" LIMIT "+noofrows);
	    }
	    	        
 }
