1. This monitoring solution has been written for RSAG based Always On but will serve well a regular Cluster Based Always On.
2. This solution does not include jobs, you will have to create them. 
You basically need 5 jobs, at the excel file you will find a reference for that at the Objects column, with “Executed by Job:” for every procedure that should be executed by a job.
3. Besides “Collect_Lags” procedure (read bullet 4), I wrapped the procedures with while loops to implement more frequent execution than 10 seconds (the max enabled from jobs) and to reduce spam of jobs history. So, all you need to do is set a step that will execute the procedure.
4. Collect_Lags procedure is not wrapped because it is relatively sensitive. I implemented a while loop at the job’s step with Waitfor-Delay of 3 seconds, because that felt right to me.
feel free to configure it however you think will be best for your organization.
5. I highly recommend to read the excel file completely. 
6. I highly recommend creating all objects in a dedicated database / DBA database.
7. By executing the “all together” file, all the objects will be created, at the 
current database that the session is set to.
