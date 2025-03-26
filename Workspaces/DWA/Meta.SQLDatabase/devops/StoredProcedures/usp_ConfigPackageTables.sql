

/*
Description:	Return Templates, config.PipelineGroups, config.Pipelines tables of associated PackageGroup or PipelineID 
Used By:		Deploy-Artefacts

Example
	exec [devops].[usp_ConfigPackageTables] 'AW'

History:	
	06/01/2024 Aidan, Created Procedure

*/
CREATE     PROCEDURE [devops].[usp_ConfigPackageTables]
	@PackageGroup VARCHAR(8000) = NULL,
	@PipelineID INT = NULL
AS 
BEGIN
	IF @PipelineID IS  NULL
	BEGIN
		SELECT @PipelineID=-1
	END
	IF @PipelineID <> -1 AND @PipelineID IS NOT NULL
	BEGIN
		SELECT @PackageGroup='ALL'
	END;
	SELECT @PackageGroup = coalesce(@PackageGroup,'ALL');
	IF @PackageGroup = 'ALL' and @PipelineID <> -1
	BEGIN
		DECLARE @InitialPackageGroup NVARCHAR(255);
		SET @InitialPackageGroup = @PackageGroup;

		SELECT @PackageGroup = COALESCE(p.PackageGroup, pg.PackageGroup, @InitialPackageGroup)
		FROM config.Pipelines p
		LEFT JOIN config.PipelineGroups pg 
			ON p.PipelineGroupID = pg.PipelineGroupID
		WHERE p.PipelineID = @PipelineID;
	END;
	WITH pg AS 
	(
		SELECT CONVERT(VARCHAR(50), TRIM(value)) AS PackageGroup 
		FROM string_split (@PackageGroup, ',') 
		WHERE value <> 'ALL'
		
		UNION ALL
		
		SELECT PackageGroup 
		FROM config.PackageGroups pg 
		WHERE @PackageGroup = 'ALL'
		
		UNION 
		
		SELECT pgl.ChildPackageGroup AS PackageGroup 
		FROM string_split(@PackageGroup, ',') pg2 
		INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value)
		
		UNION 
		
		SELECT pgl2.ChildPackageGroup AS PackageGroup 
		FROM string_split(@PackageGroup, ',') pg2 
		INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value)
		INNER JOIN config.PackageGroupLinks pgl2 ON pgl2.PackageGroup = pgl.ChildPackageGroup
	)
	SELECT t.*
	FROM config.Templates t
	WHERE t.Template IN 
	(
		SELECT pg.Template 
		FROM config.PipelineGroups pg
		WHERE pg.PipelineGroupID IN 
		(
			SELECT p.PipelineGroupID
			FROM config.Pipelines p
			WHERE p.Enabled = 1 
			AND (p.PackageGroup = @PackageGroup 
				 OR p.PackageGroup IN (SELECT PackageGroup FROM pg)
				 OR @PackageGroup = 'ALL')
			AND (p.PipelineID = @PipelineID OR @PipelineID = -1)
		)
	) ORDER BY t.Template ASC;

	
		WITH pg AS 
	(
		SELECT CONVERT(VARCHAR(50), TRIM(value)) AS PackageGroup 
		FROM string_split (@PackageGroup, ',') 
		WHERE value <> 'ALL'
		
		UNION ALL
		
		SELECT PackageGroup 
		FROM config.PackageGroups pg 
		WHERE @PackageGroup = 'ALL'
		
		UNION 
		
		SELECT pgl.ChildPackageGroup AS PackageGroup 
		FROM string_split(@PackageGroup, ',') pg2 
		INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value)
		
		UNION 
		
		SELECT pgl2.ChildPackageGroup AS PackageGroup 
		FROM string_split(@PackageGroup, ',') pg2 
		INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value)
		INNER JOIN config.PackageGroupLinks pgl2 ON pgl2.PackageGroup = pgl.ChildPackageGroup
	)
	SELECT pg.*
	FROM config.PipelineGroups pg
	WHERE pg.PipelineGroupID IN 
	(
		SELECT p.PipelineGroupID
		FROM config.Pipelines p
		WHERE p.Enabled = 1 
		AND (p.PackageGroup = @PackageGroup 
			 OR p.PackageGroup IN (SELECT PackageGroup FROM pg)
			 OR @PackageGroup = 'ALL')
		AND (p.PipelineID = @PipelineID OR @PipelineID = -1)
	) ORDER BY pg.PipelineGroupID ASC;
		WITH pg AS 
	(
		SELECT CONVERT(VARCHAR(50), TRIM(value)) AS PackageGroup 
		FROM string_split (@PackageGroup, ',') 
		WHERE value <> 'ALL'
		
		UNION ALL
		
		SELECT PackageGroup 
		FROM config.PackageGroups pg 
		WHERE @PackageGroup = 'ALL'
		
		UNION 
		
		SELECT pgl.ChildPackageGroup AS PackageGroup 
		FROM string_split(@PackageGroup, ',') pg2 
		INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value)
		
		UNION 
		
		SELECT pgl2.ChildPackageGroup AS PackageGroup 
		FROM string_split(@PackageGroup, ',') pg2 
		INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value)
		INNER JOIN config.PackageGroupLinks pgl2 ON pgl2.PackageGroup = pgl.ChildPackageGroup
	)
	
	SELECT *
	FROM config.Pipelines p
	WHERE p.Enabled = 1 
	AND (p.PackageGroup = @PackageGroup 
		 OR p.PackageGroup IN (SELECT PackageGroup FROM pg)
		 OR @PackageGroup = 'ALL')
	AND (p.PipelineID = @PipelineID OR @PipelineID = '-1')
	ORDER BY p.PipelineID ASC;
	WITH RelevantPackages AS 
	(

		SELECT TRIM(value) AS PackageGroup 
		FROM string_split(@PackageGroup, ',') 
		WHERE value <> 'ALL'
    
		UNION ALL
    
		SELECT PackageGroup 
		FROM config.PackageGroups 
		WHERE @PackageGroup = 'ALL' and @PipelineID = -1
		)

		SELECT 
			pgl.PackageGroup, 
			pgl.ChildPackageGroup
		FROM 
			config.PackageGroupLinks pgl
		WHERE 
			pgl.PackageGroup IN (SELECT PackageGroup FROM RelevantPackages)
		ORDER BY 
			pgl.PackageGroup, 
			pgl.ChildPackageGroup;
		WITH pg AS 
		(
			SELECT CONVERT(VARCHAR(50), TRIM(value)) AS PackageGroup 
			FROM string_split (@PackageGroup, ',') 
			WHERE value <> 'ALL'
		
			UNION ALL
		
			SELECT PackageGroup 
			FROM config.PackageGroups pg 
			WHERE @PackageGroup = 'ALL' and @PipelineID = -1
		
			UNION 
		
			SELECT pgl.ChildPackageGroup AS PackageGroup 
			FROM string_split(@PackageGroup, ',') pg2 
			INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value)
		
			UNION 
		
			SELECT pgl2.ChildPackageGroup AS PackageGroup 
			FROM string_split(@PackageGroup, ',') pg2 
			INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value)
			INNER JOIN config.PackageGroupLinks pgl2 ON pgl2.PackageGroup = pgl.ChildPackageGroup
		)
		SELECT 
			pg.PackageGroup, 
			pg.Monitored, 
			pg.SortOrder
		FROM 
			config.PackageGroups pg
		WHERE 
			pg.PackageGroup IN (SELECT PackageGroup FROM pg) -- Check if the PackageGroup matches
		ORDER BY 
			pg.PackageGroup;

END

GO

