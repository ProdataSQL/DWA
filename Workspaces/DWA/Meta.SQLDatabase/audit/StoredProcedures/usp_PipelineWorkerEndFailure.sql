
/*
Description:	Log Error for a Pipeline
Used by:		ADF Pipeline-Worker
Example:		

     update audit.PipelineLog  set Status = 'In Progress', ErrorMessage =null, where LineageKey = 'e09e32d6-75c3-4228-8a71-e49c8e4f6002'
	 exec [audit].[usp_PipelineWorkerEndFailure] '[
  {
    "LineageKey": "e09e32d6-75c3-4228-8a71-e49c8e4f6002",
    "Errors": [
      {
        "ErrorCode": "3101",
        "ErrorMessage": "Semantic model refresh execution failed, error message received from semantic model refresh operation - ''{\"errorCode\":\"ModelRefresh_ShortMessage_ProcessingError\",\"errorDescription\":\"Retry attempts for failures while executing the refresh exceeded the retry limit set on the request.\\n0x414700EF: {\\\"RootActivityId\\\":\\\"77314CAD-FE4E-4CD5-BF80-36B13E605728\\\"}\\n0xC136003E: Column ''<oii>ScenarioKey<\\/oii>'' in Table ''<oii>Scenario<\\/oii>'' contains a duplicate value ''Actual'' and this is not allowed for columns on the one side of a many-to-one relationship or for columns that are used as the primary key of a table. Table: Scenario. Partition: Scenario-180ddbc1-ecb6-47cf-a0a3-a068bda0bc9c.\\n0xC11C0006: The current operation was cancelled because another operation in the transaction failed.\\n0xC112001C: The operation was canceled.. The exception was raised by the IDbCommand interface. Table: ReportAccountMap. Partition: ReportAccountMap-7705b288-2899-4e91-b62d-418d02cf2e28.\\n0xC112001C: The operation was canceled.. The exception was raised by the IDbCommand interface. Table: General Ledger. Partition: General Ledger-d699db49-9681-49fc-b81e-bf1140f35c13.\\n0xC112001C: The operation was canceled.. The exception was raised by the IDbCommand interface. Table: Department. Partition: Department-d6337107-b576-462b-b572-d473a86f9f30.\\n0xC14700F0: {\\\"RootActivityId\\\":\\\"77314CAD-FE4E-4CD5-BF80-36B13E605728\\\"}\"}'' ",
        "Artefact": "Semantic model refresh",
        "ArtefactType": "PBISemanticModelRefresh",
        "Activity": "Semantic model refresh"
      },{
        "ErrorCode": "3102",
        "ErrorMessage": "Semantic model refresh execution failed, error message received from semantic model refresh operation - ''{\"errorCode\":\"ModelRefresh_ShortMessage_ProcessingError\",\"errorDescription\":\"Retry attempts for failures while executing the refresh exceeded the retry limit set on the request.\\n0x414700EF: {\\\"RootActivityId\\\":\\\"77314CAD-FE4E-4CD5-BF80-36B13E605728\\\"}\\n0xC136003E: Column ''<oii>ScenarioKey<\\/oii>'' in Table ''<oii>Scenario<\\/oii>'' contains a duplicate value ''Actual'' and this is not allowed for columns on the one side of a many-to-one relationship or for columns that are used as the primary key of a table. Table: Scenario. Partition: Scenario-180ddbc1-ecb6-47cf-a0a3-a068bda0bc9c.\\n0xC11C0006: The current operation was cancelled because another operation in the transaction failed.\\n0xC112001C: The operation was canceled.. The exception was raised by the IDbCommand interface. Table: ReportAccountMap. Partition: ReportAccountMap-7705b288-2899-4e91-b62d-418d02cf2e28.\\n0xC112001C: The operation was canceled.. The exception was raised by the IDbCommand interface. Table: General Ledger. Partition: General Ledger-d699db49-9681-49fc-b81e-bf1140f35c13.\\n0xC112001C: The operation was canceled.. The exception was raised by the IDbCommand interface. Table: Department. Partition: Department-d6337107-b576-462b-b572-d473a86f9f30.\\n0xC14700F0: {\\\"RootActivityId\\\":\\\"77314CAD-FE4E-4CD5-BF80-36B13E605728\\\"}\"}'' ",
        "Artefact": "Semantic model refresh",
        "ArtefactType": "PBISemanticModelRefresh",
        "Activity": "Semantic model refresh"
      }
    ]
  },
  {
    "LineageKey": "cd2db98e-3994-4cec-8851-4c4f169e9725",
    "Errors": [
      {
        "ErrorCode": "2451",
        "ErrorMessage": "Notebook execution failed at Notebook service with http status code - ''200'', please check the Run logs on Notebook, additional details - ''Error name - KeyError, Error value - ''directory'''' : ",
        "Artefact": "Notebook",
        "ArtefactType": "TridentNotebook",
        "Activity": "Notebook"
      }
    ]
  },
  {
    "LineageKey": "f69a6d2e-3551-4298-945c-ab1745966fc5",
    "Errors": [
      {
        "ErrorCode": "InvalidTemplate",
        "ErrorMessage": "The expression ''json(string(pipeline().parameters.TargetSettings)).directory'' cannot be evaluated because property ''directory'' cannot be selected.",
        "Artefact": "Copy-SFTP",
        "ArtefactType": "Copy",
        "Activity": "Copy-SFTP"
      }
    ]
  }
]'

    select * From audit.PipelineLog  where LineageKey = 'e09e32d6-75c3-4228-8a71-e49c8e4f6002'
    
History:
	12/03/2026 Jim, Created for Fabric DWA

*/
CREATE      PROC [audit].[usp_PipelineWorkerEndFailure] @Json varchar(max) AS
BEGIN
	SET NOCOUNT ON 

	UPDATE p
        SET p.ErrorCode = err.ErrorCode
        , p.ErrorMessage = err.ErrrorMessage 
        , p.EndDateTime = getdate()
        , Status = 'Failed'
    FROM audit.PipelineLog p
    INNER JOIN  (  
        SELECT LineageKey, Errors as ErrrorMessage
        ,JSON_VALUE(j.Errors,'$[0].ErrorCode')      AS ErrorCode
        FROM OPENJSON (@Json)
        WITH (
            LineageKey uniqueidentifier '$.LineageKey', 
            Errors nvarchar(max) '$.Errors' AS JSON
        ) j
    ) err on p.LineageKey = err.LineageKey
    WHERE p.Status <> 'Failed' or p.EndDateTime is null
   
END

GO

