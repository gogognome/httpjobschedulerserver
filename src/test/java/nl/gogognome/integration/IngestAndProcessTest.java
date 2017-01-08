package nl.gogognome.integration;


import nl.gogognome.JobResponse;
import nl.gogognome.dataaccess.transaction.NewTransaction;
import nl.gogognome.jobscheduler.jobingester.database.Command;
import nl.gogognome.jobscheduler.jobingester.database.JobIngestTestService;
import nl.gogognome.jobscheduler.scheduler.Job;
import nl.gogognome.jobscheduler.scheduler.JobState;
import org.junit.After;
import org.junit.Ignore;
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
    private TestRestTemplate restTemplate;

    @Autowired
    private JobIngestTestService jobIngestTestService;

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

    private Job buildJob(String jobId) {
        Job job = new Job(jobId);
        job.setCreationTimestamp(Instant.now());
        job.setState(JobState.IDLE);
        job.setType("no-op");
        job.setData("{ data: \"test data\" }");
        return job;
    }
}
