# Http Job Scheduler Server

This application is an example that shows how the job scheduler library can be used
to implement a job scheduler that persists jobs in a database and offer an HTTP API 
to let other applications get jobs and execute them. 

This application 
schedules jobs first in, first out (FIFO); it does not execute the jobs.
Clients of the HTTP API are responsible for executing jobs and updating the status
of the job after execution. Thus, this application can be used as 
a micro service for scheduling jobs.

Jobs can be ingested by storing job commands in the database in a specific 
table. This table is scanned with an interval of 1 second.
 
Everything is configurable by overriding the application properties.
You typically have to override the database connection URL. This application
uses the H2 database which offers in-memory databases and databases stored
in the file system. Add the following line to application.properties
to use H2 with the database stored at c:\myPlace\jobscheduler.h2.db

    httpjobschedulerserver.databaseConnectionUrl=jdbc:h2:file:c:/myPlace/httpjobscheduler;AUTO_SERVER=TRUE

When the application is started it will listen at port 8080 for HTTP requests.
Perform a GET to `localhost:8080/nextjob?requesterId=<requester id>`, where
`<requester id>` is replaced by a string that represents the client making the request.

When such a request is made an a job is available to be executed, then the status
of the job is changed to `RUNNING` and a JSON representation of the job is returned
immediately.

When such a request is made and no job is available to be executed, then the request
is blocked until a job becomes available. To prevent connections to be closed 
because of time outs in the network stack, the request gets a response after
30 seconds indicating that no job was available to be executed.
 
