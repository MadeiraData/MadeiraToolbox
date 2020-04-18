using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
namespace WMI_Collector_Console
{
    class Program
    {
        static void Main(string[] args)
        {
            String ServerName = ".\\latintest";
            System.Data.DataTable table = new DataTable("CounterResultTable");
            DataColumn column;
            DataRow row;

            // add the CounterId to DataTable.    
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Int64");
            column.ColumnName = "CounterId";
            column.ReadOnly = false;
            column.Unique = true;
            column.AutoIncrement = false;
            // Add the Column to the DataColumnCollection.
            table.Columns.Add(column);

            // add the CounterValue to DataTable. 
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Double");
            column.ColumnName = "CounterValue";
            column.AutoIncrement = false;
            column.ReadOnly = false;
            column.Unique = false;
            table.Columns.Add(column);

            SqlConnection sqlConnection1 = new SqlConnection("Data Source = " + ServerName + "; Integrated Security=true; Initial Catalog = DB_DBA;");
            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;

            cmd.CommandText = "SELECT [Id],[TypeId],[DisplayName],[CounterName],[InstanceName],[ObjectName]  FROM [Perfmon].[Counters] WHERE [TypeId] = 2";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();

            reader = cmd.ExecuteReader();
            
            PerformanceCounter Counter = new PerformanceCounter();

            while (reader.Read())
            {
                Counter.CategoryName = reader["ObjectName"].ToString();
                Counter.CounterName = reader["CounterName"].ToString();
                Counter.InstanceName = reader["InstanceName"].ToString();
                
                // will always start at 0
                dynamic firstValue = Counter.NextValue();
                System.Threading.Thread.Sleep(1000);
                // now matches task manager reading
                dynamic secondValue = decimal.Parse(Counter.NextValue().ToString());

                row = table.NewRow();
                row["CounterId"] = reader["Id"];
                row["CounterValue"] = secondValue;
                table.Rows.Add(row);

            }
            
            reader.Close();

            cmd.CommandText = "[Perfmon].[usp_CounterCollectorOutCall]";
            cmd.CommandType = CommandType.StoredProcedure;

            SqlParameter Parameter;
            Parameter = cmd.Parameters.AddWithValue("@Table", table);
            Parameter.SqlDbType = SqlDbType.Structured;
            Parameter.TypeName = "[Perfmon].[CounterCollectorType]";

            cmd.ExecuteNonQuery();
          
            sqlConnection1.Close();
        }
    }
}
