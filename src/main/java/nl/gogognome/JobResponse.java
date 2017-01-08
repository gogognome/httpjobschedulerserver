package nl.gogognome;

public class JobResponse {

    private boolean jobAvailble;
    private String jobId;
    private String jobData;

    public boolean isJobAvailble() {
        return jobAvailble;
    }

    public void setJobAvailble(boolean jobAvailble) {
        this.jobAvailble = jobAvailble;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobData() {
        return jobData;
    }

    public void setJobData(String jobData) {
        this.jobData = jobData;
    }
}
