package com.datamantra.fraudalertdashboard.dao;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

/**
 * Spring bean configuration for Cassandra db.
 * 
 * @author kafka
 *
 */
@Configuration
@EnableCassandraRepositories(basePackages = {"com.datamantra.fraudalertdashboard.dao"})
public class CassandraConfig extends AbstractCassandraConfiguration {
	
    @Value("${cassandra.contact-points:${CASSANDRA_HOST:localhost}}")
    private String contactPoints;
    
    @Value("${cassandra.port:${CASSANDRA_PORT:9042}}")
    private int port;
    
    @Value("${cassandra.keyspace-name:${CASSANDRA_KEYSPACE:creditcard}}")
    private String keyspaceName;
    
    @Override
    protected String getKeyspaceName() {
        return keyspaceName;
    }
    
    @Override
    protected String getContactPoints() {
        return contactPoints;
    }
    
    @Override
    protected int getPort() {
        return port;
    }
}
