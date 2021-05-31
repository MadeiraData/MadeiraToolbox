# Monitoring SQL Server Version Updates using SentryOne

This is a solution to automatically alert us when a SQL Server, monitored by SQL Sentry, becomes outdated.

* [Click here to read the corresponding blog post at madeiradata.com](https://www.madeiradata.com/post/monitoring-sql-server-version-updates-using-sentryone)

## Setup Step-by-Step

### Step 1: Create new database objects
This script will create a new table called `SQLVersions` and a stored procedure called `UpdateVersions`.

The UpdateVersions stored procedure will try to use the `clr_http_request` method.

So, if you want to avoid the security risk inherent in enabling OLE Automation procedures, download and install the `clr_http_request` assembly and stored procedure in the same database.

* [Click here to download the creation script SQLVersions_and_UpdateVersions.sql](SQLVersions_and_UpdateVersions.sql).

* [Click here to get the clr_http_request assembly](https://github.com/MadeiraData/ClrHttpRequest).

**NOTE:** If the script doesn't find the CLR procedure, it'll fall back to using the OLE Automation stored procedures.

Next, we'll need to create a scheduled job to periodically run the `UpdateVersions` stored procedure. Let's say, once a day.

* [Click here to get the SQL Agent job creation script](Check_SQL_Versions_Job.sql)

### Step 2: Create a new view inside the SentryOne database
This view should simplify for us the task of matching each SQL Server target and its current build, with its major version and its latest released build.

* [Click here to download the creation script SentryOne.dbo.SQLBuildVersionCheck.sql](SentryOne.dbo.SQLBuildVersionCheck.sql)

Note that this view also filters the records by hiding from us the SQL targets that are already up-to-date, and those where the latest released version is too old (1 year ago or more) or too recent (up to 1 month ago).

### Step 3: Create a new SentryOne advisory condition
Now we have something that periodically checks the latest build per each SQL Server major version, and a view that helps us find the SQL targets that are outdated.

Now we need to create an Advisory Condition in SentryOne which would detect for each SQL Server target whether it needs to be updated.

It should be as easy as creating a `SentryOne Database Query` predicate with the following query:

```sql
SELECT MessageText, DaysSinceRelease FROM dbo.SQLBuildVersionCheck
WHERE DeviceID = @ComputerID
```

![SQL Sentry Advisory Condition Editor](https://static.wixstatic.com/media/fc8278_779e7947f4644821a9f774856c37e2dd~mv2.png/v1/fill/w_740,h_45,al_c,q_90/fc8278_779e7947f4644821a9f774856c37e2dd~mv2.webp)

* [Click here to download the Advisory Condition file SQL Server Version Update.condition](SQL%20Server%20Version%20Update.condition) which you can easily import into your own SentryOne repository.

### Step 4: Add Actions to the new Advisory Condition
If we don't add any actions to our newly created condition, it's probably not going to help anyone, right?

* [Click here to learn about adding Actions in SentryOne](https://docs.sentryone.com/help/actions)

### Step 5: Configure the Response Ruleset
We probably want to avoid too much spam from our new alert.

We can do that by configuring a `Response Ruleset` for our newly created Action. For example: 

![Response Ruleset Editor](https://static.wixstatic.com/media/fc8278_81ac2beecdae457a9cb77e30aa9fce29~mv2.png/v1/fill/w_360,h_244,al_c,q_90/fc8278_81ac2beecdae457a9cb77e30aa9fce29~mv2.webp)

* [Click here to learn more about configuring Response Rulesets in SentryOne](https://docs.sentryone.com/help/response-rulesets)

## Conclusion
That's it!

You now have a fully automated mechanism to alert you when your SQL Server target(s) become outdated and need to get patched up.

Obviously, you don't really have to be limited to SentryOne. Once you have a table automatically updated with the latest SQL Server build version(s), and you have any kind of solution that's sampling or storing your current SQL Server build versions, then it should be piece of cake to connect the two and automatically generate alerts.

