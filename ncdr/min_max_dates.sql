drop table if exists #min_max_dates;

SELECT 
	max(Interchange_Received_Date) max_Interchange_Received_Date,
	max(CDS_Extract_Date) max_CDS_Extract_Date,
	max(cast(Report_Period_Start_Date as datetime)) max_Report_Period_Start_Date,
	max(cast(Report_Period_End_Date as datetime)) max_Report_Period_End_Date,
	max(CDS_Applicable_Date) max_CDS_Applicable_Date,
	max(cast(CDS_Activity_Date as datetime)) max_CDS_Activity_Date,
	max(cast(Query_Date as datetime)) max_Query_Date,
	max(cast(RTT_Period_Start_Date as datetime)) max_RTT_Period_Start_Date,
	max(cast(RTT_Period_End_Date as datetime)) max_RTT_Period_End_Date,
	max(Arrival_Date) max_Arrival_Date,
	max(cast(EC_Initial_Assessment_Date as datetime)) max_EC_Initial_Assessment_Date,
	max(cast(EC_Seen_For_Treatment_Date as datetime)) max_EC_Seen_For_Treatment_Date,
	max(cast(EC_Conclusion_Date as datetime)) max_EC_Conclusion_Date,
	max(cast(EC_Departure_Date as datetime)) max_EC_Departure_Date,
	max(cast(EC_Decision_To_Admit_Date as datetime)) max_EC_Decision_To_Admit_Date,
	max(cast(EC_Injury_Date as datetime)) max_EC_Injury_Date,
	max(Der_EC_Arrival_Date_Time) max_Der_EC_Arrival_Date_Time,
	max(Der_EC_Departure_Date_Time) max_Der_EC_Departure_Date_Time,
	max(Clinically_Ready_To_Proceed_Timestamp) max_Clinically_Ready_To_Proceed_Timestamp,
	min(Interchange_Received_Date) min_Interchange_Received_Date,
	min(CDS_Extract_Date) min_CDS_Extract_Date,
	min(cast(Report_Period_Start_Date as datetime)) min_Report_Period_Start_Date,
	min(cast(Report_Period_End_Date as datetime)) min_Report_Period_End_Date,
	min(CDS_Applicable_Date) min_CDS_Applicable_Date,
	min(cast(CDS_Activity_Date as datetime)) min_CDS_Activity_Date,
	min(cast(Query_Date as datetime)) min_Query_Date,
	min(cast(RTT_Period_Start_Date as datetime)) min_RTT_Period_Start_Date,
	min(cast(RTT_Period_End_Date as datetime)) min_RTT_Period_End_Date,
	min(Arrival_Date) min_Arrival_Date,
	min(cast(EC_Initial_Assessment_Date as datetime)) min_EC_Initial_Assessment_Date,
	min(cast(EC_Seen_For_Treatment_Date as datetime)) min_EC_Seen_For_Treatment_Date,
	min(cast(EC_Conclusion_Date as datetime)) min_EC_Conclusion_Date,
	min(cast(EC_Departure_Date as datetime)) min_EC_Departure_Date,
	min(cast(EC_Decision_To_Admit_Date as datetime)) min_EC_Decision_To_Admit_Date,
	min(cast(EC_Injury_Date as datetime)) min_EC_Injury_Date,
	min(Der_EC_Arrival_Date_Time) min_Der_EC_Arrival_Date_Time,
	min(Der_EC_Departure_Date_Time) min_Der_EC_Departure_Date_Time,
	min(Clinically_Ready_To_Proceed_Timestamp) min_Clinically_Ready_To_Proceed_Timestamp
into 
	#min_max_dates
FROM
	[NHSE_SUSPlus_Faster_SUS].[dbo].[tbl_Data_SUS_EC];


declare @cols nvarchar(max)
SELECT 
	@cols = coalesce(@cols+N',', N'') + quotename(name) 
FROM 
	tempdb.sys.columns 
WHERE 
	object_id = object_id('tempdb..#min_max_dates');

declare @qry nvarchar(max)  
select @qry = N'
	select
		col_name,
		date
	from
		#min_max_dates
	unpivot (
		date for col_name in (
			' + @cols + '
		)
	) unpvt
'

exec sp_executesql @qry 