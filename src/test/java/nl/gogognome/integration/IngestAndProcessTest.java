package nl.gogognome.integration;


import nl.gogognome.JobResponse;
import nl.gogognome.Properties;
import nl.gogognome.jobscheduler.jobingester.database.Command;
import nl.gogognome.jobscheduler.jobingester.database.JobIngestTestService;
import nl.gogognome.jobscheduler.jobingester.database.JobIngesterProperties;
import nl.gogognome.jobscheduler.scheduler.Job;
import nl.gogognome.jobscheduler.scheduler.JobState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Instant;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class IngestAndProcessTest {

    @Autowired
    private Properties properties;

    @Autowired
    private JobIngesterProperties jobIngesterProperties;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JobIngestTestService jobIngestTestService;

    @Before
    public void initProperties() {
        properties.setRequestTimeoutMilliseconds(5000);
        jobIngesterProperties.setSelectJobCommandsQuery("SELECT * FROM " + jobIngesterProperties.getTableName() + " LIMIT 100");
    }

    @Test
    public void noJobPresent_getJobViaHttpRequest_getsNoJob() {
        ResponseEntity<JobResponse> response =
                restTemplate.getForEntity("/nextjob?requesterId={requesterId}", JobResponse.class, "noJobPresentRequestId");

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertFalse(response.getBody().isJobAvailble());
    }

    @Test
    public void createOneJob_getJobViaHttpRequest_getsJob() {
        Job job = buildJob("1");
        jobIngestTestService.createJobCommand(Command.CREATE, job);

        ResponseEntity<JobResponse> response =
                restTemplate.getForEntity("/nextjob?requesterId={requesterId}", JobResponse.class, "noJobPresentRequestId");

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertTrue(response.getBody().isJobAvailble());
        assertEquals(job.getData(), response.getBody().getJobData());

        jobIngestTestService.createJobCommand(Command.DELETE, job);

        jobIngestTestService.waitUntilJobsAreIngested();
    }

    @Test
    public void performanceTest_manyJobs_oneThread() {
        Job[] jobs = new Job[1000];
        for (int i = 0; i<jobs.length; i++) {
            jobs[i] = buildJob(Integer.toString(i));
            jobIngestTestService.createJobCommand(Command.CREATE, jobs[i]);
        }

        for (int i = 0; i<jobs.length; i++) {
            ResponseEntity<JobResponse> response =
                    restTemplate.getForEntity("/nextjob?requesterId={requesterId}", JobResponse.class, "noJobPresentRequestId");
            assertEquals(HttpStatus.OK, response.getStatusCode());
            assertTrue(response.getBody().isJobAvailble());
            int index = Integer.parseInt(response.getBody().getJobId());
            jobIngestTestService.createJobCommand(Command.DELETE, jobs[index]);
        }

        jobIngestTestService.waitUntilJobsAreIngested();
    }

    private Job buildJob(String jobId) {
        Job job = new Job(jobId);
        job.setCreationTimestamp(Instant.now());
        job.setState(JobState.IDLE);
        job.setType("no-op");
        job.setData("{ data: \"test data\" }");
        return job;
    }
}
