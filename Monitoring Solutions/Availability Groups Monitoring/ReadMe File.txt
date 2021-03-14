1. This monitoring solution monitors Always On Availability Groups. 
It does not relate to Cluster events.

2. I highly recommend to create a dedicated databse for this solution's objects, or
to locate it in a "DBAs" database, aside from the operationals/apps databases.
Execute the "All Together" script from this database.

3. This solution includes jobs. 
Please notice the Jobs script includes a Login Name by which the jobsa will be executed, 
and Database Name in which the solution has been executed. please replace tags:
<User_Executing_Jobs>
<AG_Monitor_DB_Name>
accordingly, to the right user and database names.

4. All configurations, like thresholds, main database name, and more, are configured in table
[DBA_AG_Configurations]. The "All Together" script includes insert commands to the data required
for this solution to function with thresholds configred according to my opinion.
You will have to replace "Main_DB_Name" to a name of one of the databses under an Avaiability Group, which
will indicate on the state of the server.
You might find better configurations according to your organization's needs.
feel free to configure it however you think will be best for your organization, but keep the structure.


5. I highly recommend to read the excel file completely. 

6. Of course, both  "All Together" file and "Jobs Creation" file shall be executed in every replica.




