WITH regressed as
(
select 
            query_id,
            max(query_text_id)query_text_id,
            max(runtime_stats_id_1)runtime_stats_id_1,    
            max(planID_1)planID_1,
            max(avg_duration_1)avg_duration_1,
            max(avg_duration_2)avg_duration_2,
            max(DiffDuration_Sec)DiffDuration_Sec,
            max(planID_2)planID_2,
            max(runtime_stats_id_2)runtime_stats_id_2,
            max(last_duration)last_duration
from
(
            select top 200
                q.query_text_id,
                q.query_id,      
                rs1.runtime_stats_id AS runtime_stats_id_1,     
                        p1.plan_id AS planID_1,            
                cast((rs1.avg_duration)/100000 as int) AS avg_duration_1,
                cast((rs2.avg_duration)/100000 as int) AS avg_duration_2,
                        cast((rs2.avg_duration - rs1.avg_duration)/100000 as int) DiffDuration_Sec,
                p2.plan_id AS planID_2,                        
                rs2.runtime_stats_id AS runtime_stats_id_2,
                        cast((rs2.last_duration)/100000 as int) as last_duration
            FROM   
            sys.query_store_query AS q   
            JOIN sys.query_store_plan AS p1
                ON q.query_id = p1.query_id
            JOIN sys.query_store_runtime_stats AS rs1
                ON p1.plan_id = rs1.plan_id
            JOIN sys.query_store_runtime_stats_interval AS rsi1
                ON rsi1.runtime_stats_interval_id = rs1.runtime_stats_interval_id
            JOIN sys.query_store_plan AS p2
                ON q.query_id = p2.query_id
            JOIN sys.query_store_runtime_stats AS rs2
                ON p2.plan_id = rs2.plan_id
            JOIN sys.query_store_runtime_stats_interval AS rsi2
                ON rsi2.runtime_stats_interval_id = rs2.runtime_stats_interval_id
            WHERE
				rsi1.start_time > DATEADD(day, -7, GETUTCDATE())
                AND rsi2.start_time > DATEADD(day, -7, GETUTCDATE())
                AND rsi2.start_time > rsi1.start_time
                AND p1.plan_id <> p2.plan_id
                AND rs2.avg_duration > rs1.avg_duration  --2*rs1.avg_duration
                --AND cast((rs1.avg_duration)/100000 as int)>1
            ORDER BY DiffDuration_Sec desc
)tbl
group by query_id
)
SELECT              
            qt.query_sql_text,        
            rg.avg_duration_1,
            rg.avg_duration_2,
            rg.DiffDuration_Sec,
            rg.last_duration,
            'EXEC sp_query_store_force_plan '+cast(rg.query_id as varchar)+','+cast(rg.planID_1 as varchar) EXEC_Script_HotFix,
            cast(pl1.query_plan as xml) as GoodPlan,
            cast(pl2.query_plan as xml) as BadPlan  
from regressed rg
left join sys.query_store_plan pl1
            on rg.planID_1 = pl1.plan_id
left join sys.query_store_plan pl2
            on rg.planID_1 = pl2.plan_id
left join sys.query_store_query_text qt
            on qt.query_text_id = rg.query_text_id
ORDER BY DiffDuration_Sec desc