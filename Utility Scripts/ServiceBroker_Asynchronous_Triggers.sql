USE AdventureWorks2008R2
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF OBJECT_ID('Purchasing.usp_AT_uPurchaseOrderDetail', 'P') IS NOT NULL
	DROP PROCEDURE Purchasing.usp_AT_uPurchaseOrderDetail;
GO
CREATE PROCEDURE Purchasing.usp_AT_uPurchaseOrderDetail
	@inserted	XML,
	@deleted	XML = NULL
AS
    SET NOCOUNT ON;

    BEGIN TRY
        IF EXISTS ( SELECT NULL FROM @inserted.nodes('inserted/row') AS T(X) )
        BEGIN
			-- Insert record into TransactionHistory 
            INSERT INTO [Production].[TransactionHistory]
                ([ProductID]
                ,[ReferenceOrderID]
                ,[ReferenceOrderLineID]
                ,[TransactionType]
                ,[TransactionDate]
                ,[Quantity]
                ,[ActualCost])
            SELECT 
                inserted.[ProductID]
                ,inserted.[PurchaseOrderID]
                ,inserted.[PurchaseOrderDetailID]
                ,'P'
                ,GETDATE()
                ,inserted.[OrderQty]
                ,inserted.[UnitPrice]
            FROM
            (
				SELECT
					  X.query('.').value('(row/PurchaseOrderID)[1]', 'int') AS PurchaseOrderID
					, X.query('.').value('(row/PurchaseOrderDetailID)[1]', 'int') AS PurchaseOrderDetailID
					, X.query('.').value('(row/ProductID)[1]', 'int') AS ProductID
					, X.query('.').value('(row/OrderQty)[1]', 'smallint') AS OrderQty
					, X.query('.').value('(row/UnitPrice)[1]', 'money') AS UnitPrice
				FROM @inserted.nodes('inserted/row') AS T(X)
			) AS inserted 
            INNER JOIN [Purchasing].[PurchaseOrderDetail] 
            ON inserted.[PurchaseOrderID] = [Purchasing].[PurchaseOrderDetail].[PurchaseOrderID];

            -- Update SubTotal in PurchaseOrderHeader record. Note that this causes the 
            -- PurchaseOrderHeader trigger to fire which will update the RevisionNumber.
            UPDATE [Purchasing].[PurchaseOrderHeader]
            SET [Purchasing].[PurchaseOrderHeader].[SubTotal] = 
                (SELECT SUM([Purchasing].[PurchaseOrderDetail].[LineTotal])
                    FROM [Purchasing].[PurchaseOrderDetail]
                    WHERE [Purchasing].[PurchaseOrderHeader].[PurchaseOrderID] 
                        = [Purchasing].[PurchaseOrderDetail].[PurchaseOrderID])
            WHERE [Purchasing].[PurchaseOrderHeader].[PurchaseOrderID] 
                IN (
					SELECT inserted.[PurchaseOrderID]
					FROM (
							SELECT
								  X.query('.').value('(row/PurchaseOrderID)[1]', 'int') AS PurchaseOrderID
							FROM @inserted.nodes('inserted/row') AS T(X)
						) AS inserted
					);

            UPDATE [Purchasing].[PurchaseOrderDetail]
            SET [Purchasing].[PurchaseOrderDetail].[ModifiedDate] = GETDATE()
            FROM (
					SELECT
						  X.query('.').value('(row/PurchaseOrderID)[1]', 'int') AS PurchaseOrderID
						, X.query('.').value('(row/PurchaseOrderDetailID)[1]', 'int') AS PurchaseOrderDetailID
					FROM @inserted.nodes('inserted/row') AS T(X)
				) AS inserted
            WHERE inserted.[PurchaseOrderID] = [Purchasing].[PurchaseOrderDetail].[PurchaseOrderID]
                AND inserted.[PurchaseOrderDetailID] = [Purchasing].[PurchaseOrderDetail].[PurchaseOrderDetailID];
        END;
    END TRY
    BEGIN CATCH
        -- Since we're in an Asynchronous Trigger, rolling back an update operation
        -- is a lot more complicated than in a regular trigger.
        -- For now, for this scenario we'll take the risk of having partial data.

        EXECUTE [dbo].[uspLogError];
    END CATCH;
GO
ALTER TRIGGER [Purchasing].[uPurchaseOrderDetail] ON [Purchasing].[PurchaseOrderDetail] 
AFTER UPDATE AS 
BEGIN
    DECLARE @Count int;

    SET @Count = @@ROWCOUNT;
    IF @Count = 0 
        RETURN;

    SET NOCOUNT ON;

    BEGIN TRY
        IF UPDATE([ProductID]) OR UPDATE([OrderQty]) OR UPDATE([UnitPrice])
        BEGIN
			DECLARE
				@inserted	XML,
				@deleted	XML;
			
			SELECT @inserted =
				( SELECT * FROM inserted FOR XML PATH('row'), ROOT('inserted') );
			
			SELECT @deleted = 
				( SELECT * FROM deleted FOR XML PATH('row'), ROOT('deleted') );
			
			EXECUTE SB_AT_Fire_Trigger 'Purchasing.usp_AT_uPurchaseOrderDetail', @inserted, @deleted;
			
        END;
    END TRY
    BEGIN CATCH
        EXECUTE [dbo].[uspPrintError];

        -- Rollback any active or uncommittable transactions before
        -- inserting information in the ErrorLog
        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK TRANSACTION;
        END

        EXECUTE [dbo].[uspLogError];
    END CATCH;
END;
GO
/*
====================================================

					Test script

====================================================
*/
-- See the data before the update
SELECT *
FROM Purchasing.PurchaseOrderDetail
WHERE PurchaseOrderID = 8

-- Update the data without actually performing any change
UPDATE
	Purchasing.PurchaseOrderDetail
SET ProductID = ProductID
WHERE PurchaseOrderID = 8

-- Wait 5 seconds
WAITFOR DELAY '00:00:05';

-- See the updated data (ModifiedDate should be updated)
SELECT *
FROM Purchasing.PurchaseOrderDetail
WHERE PurchaseOrderID = 8
GO

SELECT *
FROM SB_AT_ServiceBrokerLogs

SELECT *
FROM sys.conversation_endpoints

/* -- cleanup closed conversations (SQL Server eventually does this automatically)
declare @q uniqueidentifier;
select @q = conversation_handle from sys.conversation_endpoints where state='CD';
end conversation @q with cleanup
*/
