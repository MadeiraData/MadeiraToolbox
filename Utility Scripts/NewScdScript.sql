/******************************************************************************
This Script Goal Is to Show How we can create a new Scd table for a specific Field
From an Scd table.
In This Example we create a customer table which is SCD Table.
A customer can have several Status:
2 - Active customer
3 - Cancel Customer
4 - Waiting Customer (a Customer that is being wait to become Active)
Then we want to add 2 new Field to this table:
A) Customer_Status_From_Date
B) Customer_Status_To_Date
In order to calculate these to fields we need to create kind of a new SCD only for the "Status" field.
******************************************************************************/


/*******************************************
Create A new Customer Table
*******************************************/

CREATE TABLE [dbo].[Dim_Customers](
       [PK_Customer] [bigint] NOT NULL,
       [From_Date] [date] NULL,
       [To_Date] [date] NULL,
       [Current_Ind] [int] NOT NULL,
       [Customer_Full_ID] [varchar](11) NOT NULL,
       [First_Name] [varchar](30) NULL,
       [Family_Name] [varchar](30) NULL,
       [Status_Code] [smallint] NULL,
       [Status_Desc] [varchar](14) NULL,
       [Family_Head_Full_ID] [varchar](11) NULL,
       [Family_Relative_Type] [smallint] NULL,
       [Birth_Date] [date] NULL,       
       [Gender_Code] [smallint] NULL,
       [Gender_Desc] [varchar](7) NULL,
       [City_Code] [numeric](5, 0) NULL,
       [City_Desc] [varchar](50) NULL,
       [Street] [nvarchar](50) NULL,
       [Home_Num] [varchar](30) NULL,
       [Zip_Code] [int] NULL,
       [Home_Phone_No] [varchar](11) NULL,
       [Work_Phone_No] [varchar](11) NULL,
       [Online_Mail] [nvarchar](100) NULL,
       [Online_Start_Date] [date] NULL,
       [Online_Ind] [smallint] NULL,
       [On_Line_Ind_Desc] [varchar](50) NULL,
       [Insurance_Date] [date] NULL,
       )


/*************************************
Insert values to a specific customer
*************************************/
insert into [dbo].[Dim_Customers]
values (13572558920100301,'2010-03-01','2010-10-31',0,'1-35725589','Benny','Cohen',2,'Active','1-35728461',1,'1986-08-03',1,'Male',3000,'Hod Hasharon','Sokolov','',12568,'077-5425698','00-0',null,null,0,'Not registrate to Online','2010-01-14'),
       (13572558920101101,'2010-11-01','2010-12-31',0,'1-35725589','Benny','Cohen',2,'Active','1-35728461',1,'1986-08-03',1,'Male',3000,'Hod Hasharon','Habanim','',12568,'052-5486132','00-0',null,null,0,'Not registrate to Online','2010-01-14'),
       (13572558920110101,'2011-01-01','2012-09-30',0,'1-35725589','Benny','Cohen',2,'Active','1-35728461',1,'1986-08-03',1,'Male',3000,'Hod Hasharon','Habanim','',12568,'052-5486132','03-5478563',null,null,0,'Not registrate to Online','2010-01-14'),
       (13572558920121001,'2012-10-01','2013-07-31',0,'1-35725589','Benny','Cohen',2,'Active','1-35728461',1,'1986-08-03',1,'Male',3000,'Hod Hasharon','Habanim','',12568,'052-5486132','03-5478563',null,null,0,'Not registrate to Online','2013-03-03'),
       (13572558920130801,'2013-08-01','2013-08-31',0,'1-35725589','Benny','Cohen',3,'Cancel','1-35728461',1,'1986-08-03',1,'Male',3000,'Hod Hasharon','Habanim','',12568,'052-5486132','03-5478563',null,null,0,'Not registrate to Online','2013-03-03'),
       (13572558920130901,'2013-09-01','2013-09-12',0,'1-35725589','Benny','Cohen',4,'Waiting','1-35728461',1,'1986-08-03',1,'Male',3000,'Hod Hasharon','Habanim','',12568,'052-5486132','03-5478563',null,null,0,'Not registrate to Online','2013-03-03'),
       (13572558920130913,'2013-09-13','2013-09-30',0,'1-35725589','Benny','Cohen',4,'Waiting','1-35728461',1,'1986-08-03',1,'Male',3000,'Hod Hasharon','Habanim','10',12568,'052-5486132','03-5478563',null,null,0,'Not registrate to Online','2013-03-03'),
       (13572558920131001,'2013-10-01','2013-10-15',0,'1-35725589','Benny','Cohen',4,'Waiting','1-35728461',1,'1986-08-03',1,'Male',3000,'Hod Hasharon','Habanim','10',12568,'052-5486132','03-5478563',null,null,0,'Not registrate to Online','2013-03-03'),
       (13572558920131016,'2013-10-16','2013-10-31',0,'1-35725589','Benny','Cohen',4,'Waiting','1-35728461',1,'1986-08-03',1,'Male',3000,'Hod Hasharon','Habanim','10',12568,'052-5486132','03-5478563',null,null,0,'Not registrate to Online','2013-10-10'),
       (13572558920131101,'2013-11-01','2013-11-30',0,'1-35725589','Benny','Cohen',2,'Active','1-35728461',1,'1986-08-03',1,'Male',3000,'Hod Hasharon','Habanim','10',12568,'052-5486132','03-5478563',null,null,0,'Not registrate to Online','2013-10-10'),
	   (13572558920131201,'2013-12-01','2999-12-31',1,'1-35725589','Benny','Cohen',2,'Active','1-35728461',1,'1986-08-03',1,'Male',3000,'Hod Hasharon','Habanim','10',12568,'052-5486132','03-5478563','w.m@gmail.com','2011-09-08',1,'registrate to Online','2013-10-10')


/******************************
Add 2 new fields To Dim_Customer Table:
Customer_Status_From_Date
Customer_Status_To_Date
******************************/

Alter table [dbo].[Dim_Customers]
add Customer_Status_From_Date [date] null,
    Customer_Status_To_Date [date] null


/**********************************
Calculate The Value of the new 2 fields - 
**********************************/


SELECT Customer_Full_ID,status_code,MIN(nf.from_date) AS FROM_date,  max(nf.to_date)  as to_date
From 
( 
select 
RANK() over( PARTITION BY Customer_Full_Id order by from_date) rnk_date , -->Rank Partition by Customer order by From Value
RANK () OVER (PARTITION BY Customer_Full_Id, status_code ORDER BY from_date ) rnk_date_and_status, --> Rank  Partition by Customer And Status Order by From Value
RANK() over( PARTITION BY Customer_Full_Id order by from_date) - RANK () OVER (PARTITION BY Customer_Full_Id, status_code ORDER BY from_date ) AS Sub_RANK, --> Rank According to First Rank - Second Rank
*
from [dbo].[Dim_Customers]
)nf
group by Customer_Full_ID,status_code,Sub_RANK
order by 1,3


