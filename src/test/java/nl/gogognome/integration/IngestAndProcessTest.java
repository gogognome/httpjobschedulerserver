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
import org.springframework.cglib.core.internal.Function;
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
        jobIngesterProperties.setSelectJobCommandsQuery("SELECT * FROM " + jobIngesterProperties.getTableName() + " LIMIT 1000");
    }

    @Test
    public void noJobPresent_getJobViaHttpRequest_getsNoJob() {
        ResponseEntity<JobResponse> response =
                restTemplate.getForEntity("/nextjob?requesterId={requesterId}", JobResponse.class, "noJobPresentRequestId");

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertFalse(response.getBody().isJobAvailable());
    }

    @Test
    public void createOneJob_getJobViaHttpRequest_getsJob() {
        Job job = buildJob("1");
        jobIngestTestService.createJobCommand(Command.CREATE, job);

        ResponseEntity<JobResponse> response =
                restTemplate.getForEntity("/nextjob?requesterId={requesterId}", JobResponse.class, "noJobPresentRequestId");

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertTrue(response.getBody().isJobAvailable());
        assertEquals(job.getData(), response.getBody().getJobData());

        jobIngestTestService.createJobCommand(Command.DELETE, job);

        jobIngestTestService.waitUntilJobsAreIngested();
    }

    @Test
    public void performanceTest_manyJobs_oneThread() throws InterruptedException {
        requestJobsWithMultipleThreads(10000, 1);
        jobIngestTestService.waitUntilJobsAreIngested();
    }

    @Test
    public void performanceTest_manyJobs_multipleThreads() throws InterruptedException {
        requestJobsWithMultipleThreads(10000, 10);
        jobIngestTestService.waitUntilJobsAreIngested();
    }

    private Job[] createJobs(int nrJobs) {
        Job[] jobs = new Job[nrJobs];
        for (int i = 0; i< nrJobs; i++) {
            jobs[i] = buildJob(Integer.toString(i));
            jobIngestTestService.createJobCommand(Command.CREATE, jobs[i]);
        }
        return jobs;
    }

    private void requestJobsWithMultipleThreads(int nrJobs, int nrThreads) throws InterruptedException {
        Job[] jobs = createJobs(nrJobs);
        Function<String, Job> getJobById = id -> jobs[Integer.parseInt(id)];

        Thread[] threads = new Thread[nrThreads];
        JobRequester[] jobRequesters = new JobRequester[threads.length];
        assertTrue("Nr job requesters must be a divisor of the number of jobs", nrJobs % jobRequesters.length == 0);
        for (int i=0; i<threads.length; i++) {
            jobRequesters[i] = new JobRequester("requester-" + i, nrJobs / jobRequesters.length, getJobById);
            threads[i] = new Thread(jobRequesters[i]);
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    private Job buildJob(String jobId) {
        Job job = new Job(jobId);
        job.setCreationInstant(Instant.now());
        job.setState(JobState.IDLE);
        job.setType("no-op");
        job.setData("{ data: \"test data\" }");
        return job;
    }

    private class JobRequester implements Runnable {

        private final String requesterId;
        private final int nrJobsToRequest;
        private final Function<String, Job> getJobById;

        JobRequester(String requesterId, int nrJobsToRequest, Function<String, Job> getJobById) {
            this.requesterId = requesterId;
            this.nrJobsToRequest = nrJobsToRequest;
            this.getJobById = getJobById;
        }

        @Override
        public void run() {
            for (int i = 0; i<nrJobsToRequest; i++) {
                ResponseEntity<JobResponse> response =
                        restTemplate.getForEntity("/nextjob?requesterId={requesterId}", JobResponse.class, requesterId);
                assertEquals(HttpStatus.OK, response.getStatusCode());
                assertTrue(response.getBody().isJobAvailable());
                jobIngestTestService.createJobCommand(Command.DELETE, getJobById.apply(response.getBody().getJobId()));
            }
        }
    }
}
