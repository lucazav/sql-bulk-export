# Sql Bulk Export

This PowerShell module contains two functions that are useful for exporting large amounts of data from tables, views, or queries on an (Azure) SQL Server database into CSV files compliant with the RFC 4180 standard.

## Functions provided by the module

The two functions in question are:

* **Export-SqlBulkCsv**
  * Exports the contents of a table, view, or query of a SQL Server database to an RFC 4180-compliant CSV file. This function supports the export of huge result sets by writing the contents of the CSV file in multiple batches.
* **Export-SqlBulkCsvByPeriod**
  * Exports the contents of a SQL Server database table, view, or query into multiple RFC 4180-compliant CSV files, split by time period (yearly, monthly, or daily), based on the contents of a selected date field. This function supports the export of huge result sets by writing the contents of each CSV file in multiple batches.

Both functions require the following parameters:

* *ServerName* : The SQL Server instance name to connect to.
* *Port* : The SQL Server instance port number. By default, it is 1433.
* *DatabaseName* : The SQL Server database name to connect to.
* *SchemaName* : The database schema of a table of view from which extract data. By default, it is ‘dbo’.
* *TableViewName* : The database table or view name from which extract data.
* *Query* : The T-SQL query with which extract data.
* *User* : The username to use to connect to database.
* *Password* : The password of the username to connect to database.
* *ConnectionTimeout* : The connection timeout in seconds. By default it is 30 seconds.
* *DatabaseCulture* : The database culture code (es. it-IT). It’s used to understand the decimal separator properly. By default, it is ‘en-US’.
* *BatchSize* : The size (number of rows) of batches that are written to the output file until data to extract is over.
* *OutputFileFullPath* : Full path (including filename and csv extension) of the output file.
* *SeparatorChar* : Character used to build string separators shown in console.

The *Export-SqlBulkCsvByPeriod* function provides three more mandatory parameters to be able to partition the result set according to a time period:

* *DateColumnName* : Date/time type column by which data will be broken down by the time period.
* *StartPeriod* : Time period string (allowed formats: "yyyy", "yyyy-MM", "yyyy-MM-dd") representing the period from which to start extracting data (period in question included).
* *EndPeriod* : Time period string (allowed formats: "yyyy", "yyyy-MM", "yyyy-MM-dd") representing the period up to which to extract data (period in question included).

It's evident that the formats used for the two input periods must be consistent with each other.

It's important to note that extracting multiple CSV files broken down by a time period using the *Export-SqlBulkCsvByPeriod* function is only possible using a table/view, and not a query. If there are, for example, special needs for selecting fields and filters to be applied to a table, one must then first expose a view with these logics to then be able to extract multiple CSV files by time period.

Moreover, the *Export-SqlBulkCsvByPeriod* function involves the use of the string token "{}" (curly brackets open and closed) within the name of the output CSV file, which token will be replaced by the string associated with the time period of the transactions contained in the CSV file in question.

Both functions automatically recognize when to connect using Windows authentication or SQL Server authentication based on whether or not the User and Password parameters are passed.

## How to use the PowerShell module

First, installation of a version greater than or equal to PowerShell 7.0 is required. You can download it from [here](https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell-on-windows?WT.mc_id=AI-MVP-5003688#installing-the-msi-package).

Also download the latest version of the *SqlBulkExport* module from this repository and unzip the folder containing it on your machine.

Supposing the upon mentioned folder is `C:\<your-path>\sql-bulk-export-main`, we assume that, before executing the commands in the sections that follow, you have changed the working directory with the  `cd C:\<your-path>` command and imported the module with the `Import-Module -Name ".\SqlBulkExport.psd1"` command.

That said, examples of using the module functions will follow.

### Exporting the content of a table/view in one CSV file

In order to export the content of a database table (or view) into the `output.csv` file in batches of 30K rows, supposing a SQL authentication is required, here the command:

```powershell
Export-SqlBulkCsv -ServerName "<your-server-name>" -DatabaseName "<your-database-name>" -User "<username>" -Password "<password>" -TableViewName "<your-table-or-view-name>" -BatchSize 30000 -OutputFileFullPath "C:\Temp\output.csv"
```

### Exporting the output of a query in one CSV file

In order to export the content of a query result set into the `output.csv` file in batches of 30K rows, supposing a SQL authentication is required, here the command:

```powershell
Export-SqlBulkCsv -ServerName "<your-server-name>" -DatabaseName "<your-database-name>" -User "<username>" -Password "<password>" -Query "SELECT <continue-your-query>" -BatchSize 30000 -OutputFileFullPath "C:\Temp\output.csv"
```

### Exporting the content of a table/view in multiple monthly CSV files

In order to export the content of a database table (or view) into multiple monthly CSV files in batches of 100K rows, starting from January 2022 to December 2022, supposing a SQL authentication is required, here the command:

```powershell
Export-SqlBulkCsv -ServerName "<your-server-name>" -DatabaseName "<your-database-name>" -User "<username>" -Password "<password>" -TableViewName "<your-table-or-view-name>" -DateColumnName "<your-date-column-name>" -StartPeriod "2022-01" -EndPeriod "2022-12" -BatchSize 100000 -OutputFileFullPath "C:\Temp\output_{}.csv"
```

Note that the `Export-SqlBulkCsvByPeriod` function involves the use of the string token `{}` (curly brackets open and closed) within the name of the output CSV file, which token will be replaced by the string associated with the time period of the transactions contained in the CSV file in question.

## References

For more details on the module, refer to the article posted on the Towards Data Science channel at [this link](https://medium.com/towards-data-science/extracting-data-from-azure-sql-server-huge-tables-in-rfc-4180-compliant-csv-files-1cb09a7a0883).
