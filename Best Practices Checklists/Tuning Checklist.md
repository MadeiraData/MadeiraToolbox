Tuning Checklist

| **Else?** | **Why?** | **What?** |
| --- | --- | --- |
| Temp tables | No statistics | Table variables |
| Remove, use parameters or if possible option (recompile) | No parameter sniffing | Local variables |
| Change to deterministic if possible, if possible option (recompile) | Value is unknown during compilation | Non-deterministic functions GETDATE() |
| Inline Table Function | Calculated per row, function is a black box - can&#39;t optimize query, can&#39;t parallel | Scalar Functions |
| Inline Table Function | Table variable, function is a black box - can&#39;t optimize query | Multi Statement Table Function |
| Write columns&#39; names in a list | New columns can create bad plans (key lookups) | Select \* |
| Change the data type | Requires an implicit conversion | Using the wrong data type |
| Change function to the right side of the operator | Cannot use indexes | Using function in the WHERE or ON clause on the tables columns |
| If possible run as one statement on all rows at once | Usually requires much more io | CURSOR |
| Rewrite the query as clean as possible | Complicated queries that are not always best optimized | Nested Views |
