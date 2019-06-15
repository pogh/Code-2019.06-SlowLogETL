CREATE PROCEDURE [staging].[Load_MariaDbSlowLog]
AS
BEGIN 

	SET NOCOUNT ON;

	---------------------------------------------------------------------------

	IF(SELECT COUNT(*) FROM [sys].[indexes] WHERE [name] = 'IX_MariaDbSlowLog_QueryStatementCheckSum') = 1
		DROP INDEX [IX_MariaDbSlowLog_QueryStatementCheckSum] ON [dbo].[MariaDbSlowLog];

	-- Remove file header
	UPDATE [staging].[TextFile]
	SET LineText = ''
	WHERE LineText LIKE '/usr/%'
	   OR LineText LIKE 'Tcp port: %'
	   OR LineText LIKE 'Time   %';

	---------------------------------------------------------------------------

	-- Use these to delimit block of lines that belong together
	DECLARE @blockFirstLineNumber INT;
	DECLARE @blockPointer INT;
	DECLARE @blockLastLineNumber INT;
	DECLARE @fileMaxLineNumber INT;
	
	-- Used to hold concat'ed lines
	DECLARE @header NVARCHAR(MAX);
	DECLARE @queryStatement NVARCHAR(MAX);

	-- Keep count of how many entries we have
	DECLARE @entryNumber int = 0;

	---------------------------------------------------------------------------
	-- Initialise variables
	
	SELECT @blockPointer = MIN(LineNumber)
	FROM [staging].[TextFile]
	WHERE LineText LIKE '# %';

	SELECT @fileMaxLineNumber = MAX(LineNumber)
	FROM [staging].[TextFile];

	---------------------------------------------------------------------------

	WHILE(@blockPointer < @fileMaxLineNumber)
	BEGIN

		SET @entryNumber += 1;
		SET @blockFirstLineNumber = @blockPointer;
	
		-- Find the end of the current block
		SELECT @blockLastLineNumber = MIN(LineNumber)
		FROM [staging].[TextFile]
		WHERE LineText NOT LIKE '# %'
		  AND LineNumber > @blockPointer;

		-- Concat the lines together
		-- Replace double empty space '  ' with # to make split easier
		-- User@host sometimes has pointless double space so replace with single space
		SET @header = NULL;
		SELECT @header = 
			COALESCE(@header, '') + 
			CASE WHEN LineText LIKE '# User@Host: %' THEN REPLACE(LineText, '  ', ' ') 
				 ELSE REPLACE(LineText, '  ', '#') END
		FROM [staging].[TextFile]
		WHERE LineNumber >= @blockPointer
		  AND LineNumber < @blockLastLineNumber
		ORDER BY LineNumber;

		-- See what's happening
		--PRINT @concatLineText;

		-- Used to step through the concat'ed string
		DECLARE @i INT = 0;
		DECLARE @j INT = 0;
		DECLARE @length INT = LEN(@header);

		DECLARE @ValueKeyPair VARCHAR(MAX);
		DECLARE @charIndex INT;

		-- Time values only appears now and then so remember it
		DECLARE @time NVARCHAR(16);

		-- Easiest to store extracted values as table
		DECLARE @values AS TABLE(
			ValueKey NVARCHAR(32),
			Value NVARCHAR(128)
			)

		WHILE(@j < @length)
		BEGIN
			SET @j = CHARINDEX('#', @header, @i);

			IF(@j = 0)
				SET @j = @length + 1;

			-- Extract value/key pair
			SET @ValueKeyPair = LTRIM(RTRIM(SUBSTRING(@header, @i, @j - @i)));
			SET @charIndex = CHARINDEX(': ', @ValueKeyPair);

			IF(@charIndex = 0) -- the time part of the date/time sits on it's own line
			BEGIN
				SELECT @time = Value + ' ' + @ValueKeyPair
				FROM @values
				WHERE ValueKey = 'Time';

				UPDATE @values
				SET Value = @time
				WHERE ValueKey = 'Time';
			END
			ELSE
			BEGIN
				INSERT INTO @values(ValueKey, Value)
				VALUES(SUBSTRING(@ValueKeyPair, 1, @charIndex - 1), SUBSTRING(@ValueKeyPair, @charIndex + 2, LEN(@ValueKeyPair) - @charIndex + 1));
			END

			SET @i = @j + 1;

		END

		----------------------------------------------------------------------------------------
		-- Move to the next block and concat the lines together to get the statement
		SET @blockPointer = @blockLastLineNumber;

		SELECT @blockLastLineNumber = MIN(LineNumber) - 1
		FROM staging.TextFile
		WHERE LineText LIKE '# %'
		  AND LineNumber > @blockPointer;

		IF(@blockLastLineNumber IS NULL)
		BEGIN
			SET @blockLastLineNumber = @fileMaxLineNumber
		END

		SET @queryStatement = NULL;
		SELECT @queryStatement = COALESCE(@queryStatement, '') + LineText + CHAR(13)
		FROM staging.TextFile
		WHERE LineNumber >= @blockPointer 
			AND LineNumber <= @blockLastLineNumber
		ORDER BY LineNumber;

		----------------------------------------------------------------------------------------
		-- The statements often have SET timestamp with a UTC-time.  If exists, then use that.

		SET @charIndex = CHARINDEX('SET timestamp=', @queryStatement);

		IF(@charIndex > 0)
		BEGIN
			-- Extract time, convert to local, convert to string
			SET @ValueKeyPair = REPLACE(CONVERT(NVARCHAR(MAX), CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, DATEADD(SECOND, CONVERT(INT, SUBSTRING(@queryStatement, @charIndex + 14, CHARINDEX(';', @queryStatement, @charIndex) - @charIndex - 14)), '19700101')), DATENAME(TzOffset, SYSDATETIMEOFFSET()))), 120), '-', '');

			UPDATE @values
			SET Value = @ValueKeyPair
			WHERE ValueKey = 'Time';

			IF(@@ROWCOUNT = 0)
				INSERT INTO @values(ValueKey, Value)
				VALUES ('Time', @ValueKeyPair);
		END

		----------------------------------------------------------------------------------------

		BEGIN TRY

			BEGIN TRANSACTION

				INSERT INTO dbo.MariaDbSlowLog (QueryDateTime, UserAndHost, ThreadId, SchemaName, QueryCacheHit, QueryTimeSeconds, LockTimeSeconds, RowsSent, RowsExamined, RowsAffected, FullScan, FullJoin, TmpTable, TmpTableOnDisk, Filesort, FilesortOnDisk, MergePasses, PriorityQueue, QueryStatement)
				SELECT 
					CONVERT(DATETIME2(0), ISNULL(Q.[Time], @time), 12) QueryDateTime, 
					Q.[User@Host] UserAndHost, 
					Q.[Thread_id] ThreadId, 
					Q.[Schema] SchemaName, 
					CASE Q.[QC_hit] WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 END QueryCacheHit, 
					Q.[Query_time] QueryTimeSeconds, 
					Q.[Lock_time] LockTimeSeconds, 
					Q.[Rows_sent] RowsSent, 
					Q.[Rows_examined] RowsExamined, 
					Q.[Rows_affected] RowsAffected, 
					ISNULL(CASE Q.[Full_scan] WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 END, 0) FullScan, 
					ISNULL(CASE Q.[Full_join] WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 END, 0) FullJoin, 
					ISNULL(CASE Q.[Tmp_table] WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 END, 0) TmpTable, 
					ISNULL(CASE Q.[Tmp_table_on_disk] WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 END, 0) TmpTableOnDisk, 
					ISNULL(CASE Q.[Filesort] WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 END, 0) Filesort, 
					ISNULL(CASE Q.[Filesort_on_disk] WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 END, 0) FilesortOnDisk, 
					ISNULL(Q.[Merge_passes], 0) MergePasses, 
					ISNULL(CASE Q.[Priority_queue] WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 END, 0) PriorityQueue,
					@queryStatement QueryStatement
				FROM @values P
				PIVOT (
					MAX(Value)
					FOR ValueKey IN ([Time], [User@Host], [Thread_id], [Schema], [QC_hit], [Query_time], [Lock_time], [Rows_sent], [Rows_examined], [Rows_affected], [Full_scan], [Full_join], [Tmp_table], [Tmp_table_on_disk], [Filesort], [Filesort_on_disk], [Merge_passes], [Priority_queue])
					) Q

				DELETE
				FROM [staging].[TextFile]
				OUTPUT deleted.LineNumber, deleted.LineText 
					INTO [staging].[TextFileLoaded](LineNumber, LineText)
				WHERE LineNumber BETWEEN @blockFirstLineNumber AND @blockLastLineNumber;

			COMMIT;

		END TRY  
		BEGIN CATCH 

			IF(XACT_STATE() != 0)
				ROLLBACK;

			DELETE
			FROM [staging].[TextFile]
			OUTPUT deleted.LineNumber, deleted.LineText 
				INTO [staging].[TextFileError](LineNumber, LineText)
			WHERE LineNumber BETWEEN @blockFirstLineNumber AND @blockLastLineNumber;

			INSERT INTO [staging].[TextFileErrorMessage](LineNumberStart, LineNumberEnd, Message)
			VALUES (@blockFirstLineNumber, @blockLastLineNumber, ERROR_MESSAGE());

		END CATCH 

		----------------------------------------------------------------------------------------

		DELETE FROM @values;

		SET @blockPointer = @blockLastLineNumber + 1;

	END;

	CREATE INDEX [IX_MariaDbSlowLog_QueryStatementCheckSum] ON [dbo].[MariaDbSlowLog] ([QueryStatementCheckSum]);

	RETURN @entryNumber;

END