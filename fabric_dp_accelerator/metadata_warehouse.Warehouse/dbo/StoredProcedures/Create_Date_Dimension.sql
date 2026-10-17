CREATE   PROCEDURE [dbo].[Create_Date_Dimension]
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

    IF NOT EXISTS (SELECT 1 FROM dbo.Date_Dimension)
    BEGIN

		-- Generate a numbers series via cross-joined VALUES to build all dates
		-- in a single set-based INSERT (no recursive CTE, no WHILE loop).
		-- 4 x 4-row sets → 256 combinations, cross-joined with 16 → 4,096 rows max.
		;WITH
		N1(n) AS (SELECT n FROM (VALUES (0),(1),(2),(3)) t(n)),
		N2(n) AS (SELECT a.n FROM N1 a CROSS JOIN N1 b),              -- 16
		N3(n) AS (SELECT a.n FROM N2 a CROSS JOIN N2 b),              -- 256
		Nums  AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS num FROM N3 a CROSS JOIN N2 b),  -- 4,096
		Dates AS (
			SELECT DATEADD(DAY, num, CAST('2025-03-26' AS DATE)) AS dt
			FROM Nums
			WHERE DATEADD(DAY, num, CAST('2025-03-26' AS DATE)) <= '2035-12-31'
		)
		INSERT INTO dbo.Date_Dimension
		SELECT
			CONVERT(INT, CONVERT(VARCHAR, dt, 112)),
			dt,
			CONVERT(VARCHAR, dt, 23),
			YEAR(dt),
			DATEPART(QUARTER, dt),
			MONTH(dt),
			DATENAME(month, dt),
			LEFT(DATENAME(month, dt), 3),
			DAY(dt),
			DATENAME(weekday, dt),
			DATEPART(WEEKDAY, dt),
			DATEPART(WEEK, dt),
			CASE WHEN DATEPART(WEEKDAY, dt) IN (1, 7) THEN 1 ELSE 0 END,
			CONCAT(LEFT(DATENAME(month, dt), 3), ', ', YEAR(dt)),
			YEAR(dt) * -1,
			DATEPART(QUARTER, dt) * -1,
			MONTH(dt) * -1,
			CONVERT(INT, CONVERT(VARCHAR, dt, 112)) * -1,
			DATEPART(WEEKDAY, dt) * -1,
			DATEPART(WEEK, dt) * -1,
			CAST(CONCAT(YEAR(dt), MONTH(dt)) AS INT) * -1
		FROM Dates;

    END

END

GO