package nl.gogognome;

import nl.gogognome.jobscheduler.jobingester.database.JobIngesterRunner;
import nl.gogognome.jobscheduler.scheduler.Job;
import nl.gogognome.jobscheduler.scheduler.JobScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@RestController
@DependsOn("dataSourceInit")
public class JobRequestController {

    private final Logger logger = LoggerFactory.getLogger(JobRequestController.class);

    private final JobScheduler jobScheduler;
    private final Properties properties;
    private final JobIngesterRunner jobIngesterRunner;

    public JobRequestController(JobScheduler jobScheduler, Properties properties, JobIngesterRunner jobIngesterRunner) {
        this.jobScheduler = jobScheduler;
        this.properties = properties;
        this.jobIngesterRunner = jobIngesterRunner;
    }

    @PostConstruct
    public void init() {
        logger.trace("init called");
        jobIngesterRunner.start();
    }

    @PreDestroy
    public void close() {
        logger.trace("close called");
        jobIngesterRunner.stop();
    }

    @RequestMapping("/nextjob")
    public JobResponse nextJob(@RequestParam(value="requesterId") String requesterId) {
        logger.trace("nextJob called with requester id " + requesterId);
        long timeoutTime = System.currentTimeMillis() + properties.getRequestTimeoutMilliseconds();
        do {
            Job job = jobScheduler.startNextRunnableJob(requesterId);
            if (job != null) {
                logger.debug("found job " + job.getId());
                JobResponse response = new JobResponse();
                response.setJobAvailble(true);
                response.setJobId(job.getId());
                response.setJobData(job.getData());
                return response;
            }
            try {
                Thread.sleep(properties.getPollingIntervalMilliseconds());
            } catch (InterruptedException e) {
                // ignore this exception
            }
        } while (System.currentTimeMillis() < timeoutTime);

        logger.debug("timed out - no job found");
        return new JobResponse();
    }
}
