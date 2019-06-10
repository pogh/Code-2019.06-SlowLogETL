
CREATE PROCEDURE [staging].[Load_MariaDbSlowLog]
AS
BEGIN 

	SET NOCOUNT ON;

	---------------------------------------------------------------------------

	-- Use these to delimit block of lines that belong together
	DECLARE @n INT;
	DECLARE @m INT;
	DECLARE @maxLineNumber INT;
	
	-- Used to hold concat'ed lines
	DECLARE @concatLineText NVARCHAR(MAX);

	-- Keep count of how many entries we have
	DECLARE @entryNumber int = 0;

	---------------------------------------------------------------------------
	-- Initialise variables
	
	SELECT @n = MIN(LineNumber)
	FROM [staging].[TextFile]
	WHERE LineText LIKE '# %';

	SELECT @maxLineNumber = MAX(LineNumber)
	FROM [staging].[TextFile];

	---------------------------------------------------------------------------

	WHILE(@n < @maxLineNumber)
	BEGIN

		SET @entryNumber += 1;

		-- Find the end of the current block
		SELECT @m = MIN(LineNumber)
		FROM [staging].[TextFile]
		WHERE LineText NOT LIKE '# %'
		  AND LineNumber > @n;

		-- Concat the lines together
		-- Replace double empty space '  ' with # to make split easier
		-- User@host sometimes has pointless double space so replace with single space
		SET @concatLineText = NULL;
		SELECT @concatLineText = 
			COALESCE(@concatLineText, '') + 
			CASE WHEN LineText LIKE '# User@Host: %' THEN REPLACE(LineText, '  ', ' ') 
				 ELSE REPLACE(LineText, '  ', '#') END
		FROM [staging].[TextFile]
		WHERE LineNumber >= @n
		  AND LineNumber < @m
		ORDER BY LineNumber;

		-- See what's happening
		--PRINT @concatLineText;

		-- Used to step through the concat'ed string
		DECLARE @i INT = 0;
		DECLARE @j INT = 0;
		DECLARE @length INT = LEN(@concatLineText);

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
			SET @j = CHARINDEX('#', @concatLineText, @i);

			IF(@j = 0)
				SET @j = @length + 1;

			-- Extract value/key pair
			SET @ValueKeyPair = LTRIM(RTRIM(SUBSTRING(@concatLineText, @i, @j - @i)));
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
		SET @n = @m;

		SELECT @m = MIN(LineNumber)
		FROM staging.TextFile
		WHERE LineText LIKE '# %'
		  AND LineNumber > @n;

		IF(@m IS NULL)
		BEGIN
			SELECT @m = MAX(LineNumber) + 1
			FROM staging.TextFile;
		END

		SET @concatLineText = NULL;
		SELECT @concatLineText = COALESCE(@concatLineText, '') + LineText + CHAR(13)
		FROM staging.TextFile
		WHERE LineNumber >= @n 
			AND LineNumber < @m
		ORDER BY LineNumber;

		----------------------------------------------------------------------------------------

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
			CASE Q.[Full_scan] WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 END FullScan, 
			CASE Q.[Full_join] WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 END FullJoin, 
			CASE Q.[Tmp_table] WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 END TmpTable, 
			CASE Q.[Tmp_table_on_disk] WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 END TmpTableOnDisk, 
			CASE Q.[Filesort] WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 END Filesort, 
			CASE Q.[Filesort_on_disk] WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 END FilesortOnDisk, 
			Q.[Merge_passes] MergePasses, 
			CASE Q.[Priority_queue] WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 END PriorityQueue,
			@concatLineText QueryStatement
		FROM @values P
		PIVOT (
			MAX(Value)
			FOR ValueKey IN ([Time], [User@Host], [Thread_id], [Schema], [QC_hit], [Query_time], [Lock_time], [Rows_sent], [Rows_examined], [Rows_affected], [Full_scan], [Full_join], [Tmp_table], [Tmp_table_on_disk], [Filesort], [Filesort_on_disk], [Merge_passes], [Priority_queue])
			) Q

		----------------------------------------------------------------------------------------

		DELETE FROM @values;

		SET @n = @m;

	END;

	RETURN @entryNumber;

END