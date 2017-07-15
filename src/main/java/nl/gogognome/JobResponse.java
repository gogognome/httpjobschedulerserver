package nl.gogognome;

public class JobResponse {

    private boolean jobAvailable;
    private String jobId;
    private byte[] jobData;

    public boolean isJobAvailable() {
        return jobAvailable;
    }

    public void setJobAvailable(boolean jobAvailable) {
        this.jobAvailable = jobAvailable;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public byte[] getJobData() {
        return jobData;
    }

    public void setJobData(byte[] jobData) {
        this.jobData = jobData;
    }
}
