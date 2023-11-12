---- 1. CREATE FOLDER FOR EVENTS ----
-- C:\XE -- repalce in whole script if you change it

---- 2. CHOOSE DATABASE FOR STRUCTURE ----
USE [_SQL_] --repalce [_SQL_] in whole script if you change it
GO

---- 3. CREATE SESSION ---

CREATE EVENT SESSION [Errors] ON SERVER 
ADD EVENT sqlserver.error_reported(
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_name,sqlserver.query_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.transaction_id,sqlserver.username)
    WHERE (([package0].[greater_than_int64]([severity],(10))) AND ([package0].[not_equal_int64]([error_number],(9104)))))
ADD TARGET package0.event_file(SET filename=N'C:\XE\errors_queries.xel',metadatafile = N'C:\XE\errors_queries.xem',max_file_size=(20),max_rollover_files=(10))
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=5 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=ON)
GO

ALTER EVENT SESSION [Errors] ON SERVER 
STATE = START;
GO

---- 4. CREATE STRUCTURE ----

USE [_SQL_]
GO
CREATE SCHEMA XE
GO

CREATE TABLE [_SQL_].[XE].[errors](
	[ID] [int] IDENTITY(1,1) CONSTRAINT PK_ID PRIMARY KEY CLUSTERED WITH FILLFACTOR = 100,
	[event_time] [datetime2](7) NULL,
	[error_number] [int] NULL,
	[severity] [int] NULL,
	[state] [int] NULL,
	[user_defined] [bit] NULL,
	[category] [nvarchar](max) NULL,
	[destination] [nvarchar](max) NULL,
	[is_intercepted] [bit] NULL,
	[message] [nvarchar](max) NULL,
	[transaction_id] [bigint] NULL,
	[session_id] [int] NULL,
	[database_name] [nvarchar](max) NULL,
	[client_hostname] [nvarchar](max) NULL,
	[client_app_name] [nvarchar](max) NULL,
	[username] [nvarchar](max) NULL,
	[sql_text] [nvarchar](max) NULL,
	[query_hash] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

CREATE TABLE [_SQL_].[XE].[errors_exceptions](
	[ID] [int] IDENTITY(1,1) CONSTRAINT PK_ID_EXCP PRIMARY KEY CLUSTERED WITH FILLFACTOR = 100,
	[error_number] [int] NULL,
    [sql_text] [nvarchar](max) NULL,
    [database_name] [nvarchar](max) NULL,
    [username] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

---- 5. CREATE PROCEDURE ----

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

----
IF NOT EXISTS (
  SELECT 1 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'XE'
     AND SPECIFIC_NAME = N'usp_XEGetErrors' 
)
   EXEC ('CREATE PROCEDURE [XE].[usp_XEGetErrors] AS SELECT 1');
GO
---
--EXEC [util].[XE].usp_XEGetErrors @profile_name = 'mail_profile', @email_rec = 'MSSQLAdmins@domain.com', @XE_Path='C:\XE', @MaxErrorsForNotification = 0;
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [XE].[usp_XEGetErrors] @profile_name NVARCHAR(128) = 'mail_profile',
										@email_rec NVARCHAR(MAX) = 'MSSQLAdmins@domain.com',
                                        @XE_Path NVARCHAR(MAX) = 'S:\XE',
                                        @MaxErrorsForNotification INT = 0
AS
BEGIN
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

	-- XE Patch
	DECLARE @XE_Path_XEL NVARCHAR(MAX) = @XE_Path + '\errors_queries*.xel'
	DECLARE @XE_Path_XEM NVARCHAR(MAX) = @XE_Path + '\errors_queries*.xem'

	--- GET DATA FROM XML --
	DECLARE @CurrentDate datetime2;
	SELECT @CurrentDate = GETDATE();
	DECLARE @StartDate datetime2 = NULL;
	SELECT @StartDate = ISNULL(MAX(event_time), CAST('2001-01-01 00:00:00.000' AS datetime2)) FROM [XE].[errors]

	SELECT DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), CURRENT_TIMESTAMP), x.event_data.value('(event/@timestamp)[1]', 'datetime2')) AS event_time,
		   x.event_data.value('(event/data[@name="error_number"])[1]', 'int') AS error_number,
		   x.event_data.value('(event/data[@name="severity"])[1]', 'int') AS severity,
		   x.event_data.value('(event/data[@name="state"])[1]', 'int') AS state,
		   x.event_data.value('(event/data[@name="user_defined"])[1]', 'bit') AS user_defined,
		   x.event_data.value('(event/data[@name="category"]/text)[1]', 'nvarchar(max)') AS category,
		   x.event_data.value('(event/data[@name="destination"]/text)[1]', 'nvarchar(max)') AS destination,
		   x.event_data.value('(event/data[@name="is_intercepted"])[1]', 'bit') AS is_intercepted,
		   x.event_data.value('(event/data[@name="message"])[1]', 'nvarchar(max)') AS message,
		   x.event_data.value('(event/action[@name="transaction_id"])[1]', 'bigint') AS transaction_id,
		   x.event_data.value('(event/action[@name="session_id"])[1]', 'int') AS session_id,
		   x.event_data.value('(event/action[@name="database_name"])[1]', 'nvarchar(max)') AS database_name,
		   x.event_data.value('(event/action[@name="client_hostname"])[1]', 'nvarchar(max)') AS client_hostname,
		   x.event_data.value('(event/action[@name="client_app_name"])[1]', 'nvarchar(max)') AS client_app_name,
		   x.event_data.value('(event/action[@name="username"])[1]', 'nvarchar(max)') AS username,
		   x.event_data.value('(event/action[@name="sql_text"])[1]', 'nvarchar(max)') AS sql_text,
		   x.event_data.value('(event/action[@name="query_hash"])[1]', 'nvarchar(max)') AS query_hash
	INTO #ERRORS
	FROM    sys.fn_xe_file_target_read_file (@XE_Path_XEL, @XE_Path_XEM, null, null)
			   CROSS APPLY (SELECT CAST(event_data AS XML) AS event_data) as x
	WHERE DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), CURRENT_TIMESTAMP), x.event_data.value('(event/@timestamp)[1]', 'datetime2')) > @StartDate
	ORDER BY event_time DESC
	--- DELETE EXCEPTIONS ---
	DELETE [t] FROM #ERRORS [t] INNER JOIN [XE].[errors_exceptions] [te] 
	ON [t].[sql_text]  LIKE '%' + [te].[sql_text] + '%'
	AND [t].[username] = [te].[username]
	AND [t].[database_name] = [te].[database_name]
	AND [t].[error_number] = [te].[error_number]
	--- DELETE SQL_TEXT NULL's
	DELETE [t] FROM #ERRORS [t] INNER JOIN [XE].[errors_exceptions] [te]
	ON [t].[username] = [te].[username]
	AND [t].[database_name] = [te].[database_name]
	AND [t].[error_number] = [te].[error_number]
	WHERE [t].[sql_text] IS NULL
	--- INSERT
	INSERT INTO [_SQL_].[XE].[errors]
	SELECT * FROM #ERRORS
	---- REPPORT ----
	Declare @Body varchar(max), @BodyW varchar(max),
			@TableHeadW varchar(max),
			@TableTailW varchar(max),
			@TableExample nvarchar(max), @TableExampleBody nvarchar(max), @BodyExample nvarchar(max),
			@Subject varchar(100),
			@NumberOfErrors INT = 0;
 
	Set NoCount On;
	/* -------------------------------------------------------------------------------------------------------------- */
	-- REPORT - NUMBER OF ERRORS BY HOURS
	IF OBJECT_ID('tempdb.dbo.#TempRap', 'U') IS NOT NULL
	  DROP TABLE #TempRap;
	IF OBJECT_ID('tempdb.dbo.#TempRap2', 'U') IS NOT NULL
	  DROP TABLE #TempRap2;

	DECLARE @RowCount INT = 0;

	SELECT TOP (10)
		   e.error_number,
		   m.text,
		   COUNT(*) AS Number_of_errors,
		   e.severity,
		   e.state,
		   e.category,
		   e.destination,
		   e.is_intercepted,
		   e.client_app_name
	INTO #TempRap
	FROM [XE].[errors] e
		LEFT JOIN sys.messages m
			ON e.error_number = m.message_id
	WHERE m.language_id = 1033
		  AND e.event_time > @StartDate
		  AND e.client_app_name NOT LIKE 'Microsoft SQL Server Management Studio%'
	GROUP BY e.error_number,
			 e.severity,
			 e.state,
			 e.category,
			 e.destination,
			 e.is_intercepted,
			 m.text,
			 e.client_app_name
	ORDER BY COUNT(*) DESC;

	SET @RowCount = @@ROWCOUNT;

	-- GET NUMBER OF ALL ERRORS --
		SELECT @NumberOfErrors = COUNT(*) 
		FROM [XE].[errors] e
		LEFT JOIN sys.messages m
			ON e.error_number = m.message_id
			WHERE m.language_id = 1033
		  AND e.event_time > @StartDate
		  AND e.client_app_name NOT LIKE 'Microsoft SQL Server Management Studio%'


	IF (@RowCount <> 0 AND @NumberOfErrors > @MaxErrorsForNotification AND @email_rec IS NOT NULL)
	BEGIN
		Set @TableTailW = '</table>';
		Set @TableHeadW = '<html><head>' +
						  '<style>' +
						  'td {border: solid black 1px;padding-left:5px;padding-right:5px;padding-top:1px;padding-bottom:1px;font-size:11pt;} ' +
						  '</style>' +
						  '</head>' +
						  '<body><table cellpadding=0 cellspacing=0 border=0><caption>TOP 10 NUMBER OF ERRORS. DATE FROM ' + CONVERT(CHAR(19), @StartDate, 121) + ' TO ' + CONVERT(CHAR(19), @CurrentDate, 121) + '</caption>' +
						  '<tr bgcolor=#c0f4c3>' +
						  '<td align=center><b>Error Number</b></td>' +
						  '<td align=center><b>Error Text</b></td>' +
						  '<td align=center><b>Number of errors</b></td>' +
						  '<td align=center><b>Severity</b></td>' +
						  '<td align=center><b>State</b></td>' +
						  '<td align=center><b>Category</b></td>' +
						  '<td align=center><b>Destination</b></td>' +
						  '<td align=center><b>Is intercepted?</b></td>' + 
						  '<td align=center><b>Client app name</b></td></tr>';

		Select @BodyW = (SELECT error_number AS [TD align=right]
							  ,ISNULL(text, 'n/a') AS [TD align=left]
							  ,ISNULL(Number_of_errors, 0) AS [TD align=right]
							  ,ISNULL(severity, 0) AS [TD align=right]
							  ,ISNULL(state, 0) AS [TD align=right]
							  ,ISNULL(category, 'n/a') AS [TD align=center]
							  ,ISNULL(destination, 'n/a') AS [TD align=center]
							  ,ISNULL(is_intercepted, 0) AS [TD align=center]
							  ,ISNULL(client_app_name, 'n/a') AS [TD align=center]
						FROM #TempRap
						WHERE client_app_name NOT LIKE 'Microsoft SQL Server Management Studio%'
						ORDER BY Number_of_errors DESC
						For XML raw('tr'), Elements)

		-- Replace the entity codes and row numbers
		Set @BodyW = Replace(@BodyW, '_x0020_', space(1))
		Set @BodyW = Replace(@BodyW, '_x003D_', '=')

		--- 
		DECLARE @CountTemp INT, @EN INT, @State INT, @Category NVARCHAR(MAX), @Destination NVARCHAR(MAX), @Severity INT, @is_intercepted BIT, @ClientAppName NVARCHAR(MAX);
		SELECT @CountTemp = COUNT(*) FROM #TempRap
		SET @BodyExample = '';
		WHILE (@CountTemp > 0)
		BEGIN
			-- GET FIRST ERROR ---
			SELECT TOP(1) @EN = error_number, @State = state, @Category = category, @Destination = destination, @Severity = severity, @is_intercepted = is_intercepted, @ClientAppName = client_app_name FROM #TempRap ORDER BY Number_of_errors DESC
			----------------------
			SELECT  TOP(3) ID 
					,CONVERT(CHAR(19), e.event_time, 121) AS event_time
					,e.message
					,e.sql_text
					,e.database_name
					,e.client_hostname
					,e.client_app_name
					,e.username
			INTO #TempRap2
			FROM [XE].[errors] e
			WHERE e.event_time > @StartDate AND e.error_number = @EN AND @State = state AND @Category = category AND @Destination = destination AND @Severity = severity AND @is_intercepted = is_intercepted AND @ClientAppName = client_app_name
			AND client_app_name NOT LIKE 'Microsoft SQL Server Management Studio%'
			ORDER BY e.event_time DESC;

			Set @TableExample = '<br><table cellpadding=0 cellspacing=0 border=0><caption>TOP 3 EXAMPLES OF ERROR NR ' + CAST(@EN AS VARCHAR(10)) + '</caption>' +
							  '<tr bgcolor=#90ffff>' +
							  '<td align=center><b>ID</b></td>' +
							  '<td align=center><b>Event time</b></td>' +
							  '<td align=center><b>Message</b></td>' +
							  '<td align=center><b>SQL Text</b></td>' +
							  '<td align=center><b>Database Name</b></td>' +
							  '<td align=center><b>Host Name</b></td>' +
							  '<td align=center><b>Application Name</b></td>' +
							  '<td align=center><b>User Name</b></td></tr>';

			Select @TableExampleBody = (SELECT ID AS [TD align=right]
								  ,event_time AS [TD align=center]
								  ,ISNULL(message, 'n/a') AS [TD align=left]
								  ,CAST(ISNULL(sql_text, 'n/a') AS NVARCHAR(255)) + ' [/cut]' AS [TD align=left]
								  ,ISNULL(database_name, 'n/a') AS [TD align=left]
								  ,ISNULL(client_hostname, 'n/a') AS [TD align=left]
								  ,ISNULL(client_app_name, 'n/a') AS [TD align=center]
								  ,ISNULL(username, 'n/a') AS [TD align=center]
							FROM #TempRap2
							WHERE	client_app_name NOT LIKE 'Microsoft SQL Server Management Studio%'
									ORDER BY event_time DESC
							For XML raw('tr'), Elements)

			select @TableExampleBody = ISNULL(@TableExampleBody, '');

			-- CLEAN UP ----------
			DELETE FROM #TempRap WHERE error_number = @EN and state = @State and category = @Category and destination = @Destination and severity = @Severity and is_intercepted = @is_intercepted and client_app_name = @ClientAppName
			----------------------
			Set @BodyExample = @BodyExample + @TableExample + @TableExampleBody + '</table>';
			IF OBJECT_ID('tempdb.dbo.#TempRap2', 'U') IS NOT NULL
			DROP TABLE #TempRap2;
			SET @CountTemp = @CountTemp - 1;
		END
		--- 
		Set @BodyExample = Replace(@BodyExample, '_x0020_', space(1));
		Set @BodyExample = Replace(@BodyExample, '_x003D_', '=');
	
		-- CREATE HTML BODY 
		Select @Body = @TableHeadW + @BodyW + @TableTailW + @BodyExample + '<br><br>Get full example: <br> SELECT * FROM [XE].[errors] WHERE ID = ... <br><br>
				All the Errors collected: <b>' + CAST(@NumberOfErrors AS VARCHAR(10))  + '</b><br>
				Errors notification level: <b>' + CAST(@MaxErrorsForNotification AS VARCHAR(10))  + '</b><br>
				<br>XE Errors 2023</body></html>'
	
		SET @Subject = '[' + @@servername + '] XE ERROR REPORT OF ' +  CONVERT(CHAR(10), GETDATE(), 121)
		-- return output
		 EXEC msdb.dbo.sp_send_dbmail
					@profile_name = @profile_name,
					@recipients = @email_rec,
					@body =  @Body,
					@subject = @Subject,
					@body_format = 'HTML';
	END -- IF ROWCOUNT <> 0
	---- CLEAN UP --
	IF OBJECT_ID('tempdb.dbo.#TempRap', 'U') IS NOT NULL
	  DROP TABLE #TempRap;
	IF OBJECT_ID('tempdb.dbo.#TempRap2', 'U') IS NOT NULL
	  DROP TABLE #TempRap2;
END
GO

---- 6. JOB ----

USE [msdb]
GO

DECLARE @Date datetime2 = GETDATE();
DECLARE @Name NVARCHAR(100) = ORIGINAL_LOGIN()
DECLARE @Description NVARCHAR(2000) = N'Presents report from errors captured by Extended Events - ' + CONVERT(CHAR(10), @Date, 121) + ' - ' + @Name;

DECLARE @jobId BINARY(16)
EXEC  msdb.dbo.sp_add_job @job_name=N'__XE_ERRORS__', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=@Description, 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
select @jobId
GO
EXEC msdb.dbo.sp_add_jobserver @job_name=N'__XE_ERRORS__', @server_name = @@SERVERNAME
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_add_jobstep @job_name=N'__XE_ERRORS__', @step_name=N'_report_', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_fail_action=2, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC [_SQL_].[XE].usp_XEGetErrors @profile_name = ''mail_profile'', @email_rec = ''MSSQLAdmins@domain.com'', @XE_Path=''C:\XE'', @MaxErrorsForNotification = 0;', 
		@database_name=N'master', 
		@flags=0
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_update_job @job_name=N'__XE_ERRORS__', 
		@enabled=1, 
		@start_step_id=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'', 
		@notify_page_operator_name=N''
GO
USE [msdb]
GO
DECLARE @schedule_id int
EXEC msdb.dbo.sp_add_jobschedule @job_name=N'__XE_ERRORS__', @name=N'workday at 6 AM', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=62, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20230721, 
		@active_end_date=99991231, 
		@active_start_time=60000, 
		@active_end_time=235959, @schedule_id = @schedule_id OUTPUT
select @schedule_id
GO
