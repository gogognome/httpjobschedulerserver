package nl.gogognome;

import com.zaxxer.hikari.HikariDataSource;
import nl.gogognome.dataaccess.transaction.CompositeDatasourceTransaction;
import nl.gogognome.jobscheduler.jobingester.database.JobIngesterProperties;
import nl.gogognome.jobscheduler.jobpersister.database.DatabaseJobPersister;
import nl.gogognome.jobscheduler.jobpersister.database.DatabaseJobPersisterProperties;
import nl.gogognome.jobscheduler.runnablejobfinder.FifoRunnableJobFinder;
import nl.gogognome.jobscheduler.scheduler.JobScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class BeanConfiguration {

    private final Logger logger = LoggerFactory.getLogger(JobRequestController.class);

    @Bean
    public JobScheduler jobScheduler(DatabaseJobPersister databaseJobPersister) {
        return new JobScheduler(new FifoRunnableJobFinder(), databaseJobPersister);
    }

    @Bean
    public DataSource dataSourceInit(Properties properties, JobIngesterProperties jobIngesterProperties,
                                     DatabaseJobPersisterProperties databaseJobPersisterProperties) {
        logger.trace("Registring data source");
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(properties.getDatabaseConnectionUrl());
        dataSource.setUsername(properties.getDatabaseUsername());
        dataSource.setPassword(properties.getDatabasePassword());
        CompositeDatasourceTransaction.registerDataSource(jobIngesterProperties.getConnectionName(), dataSource);
        CompositeDatasourceTransaction.registerDataSource(databaseJobPersisterProperties.getConnectionName(), dataSource);
        return dataSource;
    }

}
