

function GenerateSeparator {
    param (
        [Parameter(Mandatory)]
        [string]$Title,

        [string]$SeparatorChar = "="
    )

    $SeparatorChar * ($Title.length + 1)
    
}

function GetTimespanString {
    param (
        [timespan]$t
    )
    
    return "$([System.Math]::Floor($t.TotalHours))h $($t.Minutes)' $($t.Seconds)"" $($t.Milliseconds.ToString().PadLeft(3, '0'))ms"
}


<#
    .SYNOPSIS
    Exports the content of a SQL Server database table, view or query
    in an RFC 4180-Compliant CSV file.

    .DESCRIPTION
    Exports the content of a SQL Server database table, view or query
    in an RFC 4180-Compliant CSV file.
    This function supports the export of huge resultsets, writing the
    CSV file content in multiple batches.

    .PARAMETER ServerName
    The SQL Server instance name to connect to.

    .PARAMETER Port
    The SQL Server instance port number. By default, it is 1433.

    .PARAMETER DatabaseName
    The SQL Server database name to connect to.

    .PARAMETER SchemaName
    The database schema of a table of view from which extract data.
    By default, it is 'dbo'.

    .PARAMETER TableViewName
    The database table or view name from which extract data. This
    parameter is mutually exclusive with 'Query'.

    .PARAMETER Query
    The T-SQL query with which extract data. This parameter is
    mutually exclusive with 'TableViewName'.

    .PARAMETER User
    The username to use to connect to database.

    .PARAMETER Password
    The password of the username to connect to database.

    .PARAMETER ConnectionTimeout
    The connection timeout in seconds. By default it is 30 seconds.

    .PARAMETER DatabaseCulture
    The database culture code (es. it-IT). It's used to understand the
    decimal separator properly. By default, it is 'en-US'.

    .PARAMETER BatchSize
    The size (number of rows) of batches that are written to the output
    file until data to extract is over.

    .PARAMETER OutputFileFullPath
    Full path (including filename and csv extension) of the output file.

    .PARAMETER SeparatorChar
    Character used to build string separators shown in console.

    .EXAMPLE
    Import-Module -Name "C:\your-folder\SqlBulkExport.psm1"

    Export-SqlBulkCsv -ServerName "YourServerName" -DatabaseName "YourDatabaseName" -SchemaName "yourschema" -TableViewName "your_table_name" -OutputFileFullPath "C:\your-output-folder\output.csv"

#>
function Export-SqlBulkCsv {

    param(
        
        [Parameter(Mandatory)]
        [string]$ServerName,

        [string]$Port               = 1433,

        [Parameter(Mandatory)]
        [string]$DatabaseName,
        
        [string]$SchemaName         = "dbo",

        [string]$TableViewName,

        [string]$Query,

        [string]$User,

        [string]$Password,

        [int]$ConnectionTimeout     = 30,

        [string]$DatabaseCulture    = "en-US",

        [int]$BatchSize             = 100000,
        
        [Parameter(Mandatory)]
        [string]$OutputFileFullPath,

        [string]$SeparatorChar = "="
    )


    # Invoke-Sqlcmd -Query "SELECT * FROM [$SchemaName].[$TableViewName]" -ServerInstance "$ServerName" -Database "$DatabaseName" `
    # | Export-Csv -Path "$OutputFileFullPath" -NoTypeInformation -UseQuotes AsNeeded -Encoding utf8

    $timer = [Diagnostics.Stopwatch]::StartNew()

    # Set the right culture needed to use the correct decimal number separators
    [cultureinfo]::currentculture = $DatabaseCulture

    if ($PSBoundParameters.ContainsKey("User")) {

        if ($PSBoundParameters.ContainsKey("Password")) {

            $pass = $Password

        } else {

            $pass = ""

        }
        
        if ($ServerName.ToLower() -contains "database.windows.net") {

            $SqlConnectionString = 'Server=tcp:{0},{1};Initial Catalog={2};Persist Security Info=False;User ID={3};Password={4};Encrypt=True;Connection Timeout={5}' -f $ServerName, $Port, $DatabaseName, $User, $pass, $ConnectionTimeout;

        } else {
            
            $SqlConnectionString = 'Server={0},{1};Database={2};User Id={3};Password={4};Connection Timeout={5}' -f $ServerName, $Port, $DatabaseName, $User, $pass, $ConnectionTimeout;

        }

    } else {

        if ($ServerName.ToLower() -contains "database.windows.net") {

            $SqlConnectionString = 'Server=tcp:{0},{1};Initial Catalog={2};Authentication=Active Directory Integrated;Encrypt=True;Connection Timeout={3}' -f $ServerName, $Port, $DatabaseName, $ConnectionTimeout;

        } else {

            $SqlConnectionString = 'Data Source={0},{1};Initial Catalog={2};Integrated Security=SSPI;Connection Timeout={3}' -f $ServerName, $Port, $DatabaseName, $ConnectionTimeout;

        }

    }

    
    
    if ($PSBoundParameters.ContainsKey('TableViewName')) {

        $SqlQuery = "SELECT * FROM [$SchemaName].[$TableViewName];";
        $titleStr = " Extracting data from [$DatabaseName].[$SchemaName].[$TableViewName]"

    }
    elseif ($PSBoundParameters.ContainsKey('Query')) {

        $SqlQuery = $Query;
        $titleStr = " Extracting data from query."

    }
    else {

        Write-Error "`b`bERROR! At least a table of view name, or a query must be passed." -CategoryActivity " `b"

    }

    
    $separator = GenerateSeparator -Title $titleStr -SeparatorChar $SeparatorChar

    Write-Host ""
    Write-Host ""
    Write-Host $separator
    Write-Host $titleStr
    Write-Host " Query: $SqlQuery"
    Write-Host $separator
    

    try {

        Write-Host "... reading data from SQL Server"
        
        $SqlConnection = New-Object -TypeName System.Data.SqlClient.SqlConnection -ArgumentList $SqlConnectionString;
        $SqlCommand = $SqlConnection.CreateCommand();
        $SqlCommand.CommandText = $SqlQuery;
        $SqlCommand.CommandTimeout = 0;
        
        $SqlConnection.Open();
        $SqlDataReader = $SqlCommand.ExecuteReader();
        
        $t0 = $timer.elapsed

        
        Write-Host "... connection to the data source obtained in $(GetTimespanString($t0))"
        Write-Host "... now writing the first batch of data"

        #Fetch data and write out to files
        if ($SqlDataReader.HasRows) {
        
            # Get the table schema from the data reader
            $schemaTable = $SqlDataReader.GetSchemaTable();
            
            # Define the data table that will contain the batch rows
            $dataTable = New-Object System.Data.DataTable
        
            # Define column names and types for the batch data table
            foreach ($row in $schemaTable.Rows) {
                
                $colName = $row.ColumnName;
                $t = $row.DataType;
                
                [void]$dataTable.Columns.Add($colName, $t);
            }
            
            $totalRows = 0
            $numOfBatches = 0
            $i = 1 # current row number of the data reader
        
            $t1 = $t0

            # for each row of the data reader...
            while ($SqlDataReader.Read()) {
        
                # Add the current row to the batch data table
                $newRow = $dataTable.Rows.Add();
        
                foreach ($col in $dataTable.Columns)
                {
                    $newRow[$col.ColumnName] = $SqlDataReader[$col.ColumnName];
                }
                    
                # If the current row number IS a multiple of the batch size...
                if ($i % $BatchSize -eq 0) {
                
                    # ... write the batch data table to the file.

                    $numOfBatches = $numOfBatches + 1
        
                    # If it's the first batch data table...
                    if ($i -eq $BatchSize) {
        
                        # ... then just write or overwrite the output file
                        $dataTable | Export-Csv -Path "$OutputFileFullPath" -NoTypeInformation -UseQuotes AsNeeded -Encoding utf8
        
                    } else { # it isn't the first batch data table
        
                        # so just append the data table to the output file
                        $dataTable | Export-Csv -Path "$OutputFileFullPath" -NoTypeInformation -UseQuotes AsNeeded -Encoding utf8 -Append
        
                    }
                    
                    $rowCount = $dataTable.Rows.count
                    $totalRows = $totalRows + $rowCount

                    $t2 = $timer.elapsed
                    $delta = $t2 - $t1

                    Write-Host "... $totalRows rows written after $(GetTimespanString($t2)) ( Δt = $(GetTimespanString($delta)) )"
        
                    # Current batch time becomes the referene one for the next batch
                    $t1 = $t2

                    # then flush the batch data table
                    $dataTable.Clear()
        
                } #[if_batchsize]
            
                # Finally, increment the current row number
                $i = $i + 1
        
            } #[while_reader]

            # If there are pending rows to be appended to the output file...
            if ($dataTable.Rows.count -gt 0) {

                # If it's the first batch being written to file...
                if ($numOfBatches -eq 0) {
        
                    # ... just write or overwrite the output file
                    $dataTable | Export-Csv -Path "$OutputFileFullPath" -NoTypeInformation -UseQuotes AsNeeded -Encoding utf8
    
                } else { # it isn't the first batch data table
    
                    # ... just append rows
                    $dataTable | Export-Csv -Path "$OutputFileFullPath" -NoTypeInformation -UseQuotes AsNeeded -Encoding utf8 -Append
    
                }

                

                # .. adding rows number to counters
                $rowCount = $dataTable.Rows.count
                $totalRows = $totalRows + $rowCount

                $t3 = $timer.elapsed
                $delta = $t3 - $t1

                Write-Host "... $totalRows rows written after $(GetTimespanString($t3)) ( Δt = $(GetTimespanString($delta)) )"

                # and then flush the data table
                $dataTable.Clear()
            }

        } #[if_datareader_hasrows]


        Write-Host "... DONE!"
        Write-Host $separator

        if ($numOfBatches -gt 0) {

            $allBatchesElapsed = $t2 - $t0
            $avgElapsedPerBatch = $allBatchesElapsed / $numOfBatches

            Write-Host "In average, a batch of $BatchSize rows took $(GetTimespanString($avgElapsedPerBatch))"

        }
        
        Write-Host "Data exported to the file $OutputFileFullPath"
        
    }
    catch [Exception] {

        Write-Host $separator
        Write-Error "`b`bERROR!" -CategoryActivity " `b"
        Write-Error "`b`b$($_.Exception.Message)" -CategoryActivity " `b"
        
        if ($SqlDataReader.HasRows -and $numOfBatches -gt 0) {
            Write-Warning -Message "If it was created, the output file is definitely not complete."
        }

    }
    finally {

        Write-Host $separator

        $timer.Stop();

        $dataTable.Dispose();

        if (-Not $SqlDataReader.IsClosed) {
            $SqlDataReader.Close();
        }
        $SqlDataReader.Dispose();
        
        
        $SqlConnection.Close();
        $SqlConnection.Dispose();

    }
}


<#
    .SYNOPSIS
    Exports the content of a SQL Server database table, view or query
    in multiple RFC 4180-Compliant CSV files, broken down by "year-month"
    based on the contents of a date field.

    .DESCRIPTION
    Exports the content of a SQL Server database table, view or query
    in multiple RFC 4180-Compliant CSV files, broken down by "year-month"
    based on the contents of a date field.
    This function supports the export of huge result sets, writing each
    CSV file content in multiple batches.

    .PARAMETER ServerName
    The SQL Server instance name to connect to.

    .PARAMETER Port
    The SQL Server instance port number. By default, it is 1433.

    .PARAMETER DatabaseName
    The SQL Server database name to connect to.

    .PARAMETER SchemaName
    The database schema of a table of view from which extract data.
    By default, it is 'dbo'.

    .PARAMETER TableViewName
    The database table or view name from which extract data.
    This parameter is mutually exclusive with 'Query'.

    .PARAMETER Query
    The T-SQL query with which extract data. This parameter is
    mutually exclusive with 'TableViewName'.

    .PARAMETER DateColumnName
    Date/time type column by which data will be broken down by
    the time period.

    .PARAMETER StartYearMonth
    Time period string (allowed formats: "yyyy", "yyyy-MM",
    "yyyy-MM-dd") representing the period from which to start
    extracting data (period in question included).

    .PARAMETER EndYearMonth
    Time period string (allowed formats: "yyyy", "yyyy-MM",
    "yyyy-MM-dd") representing the period up to which to extract
    data (period in question included).

    .PARAMETER User
    The username to use to connect to database.

    .PARAMETER Password
    The password of the username to connect to database.

    .PARAMETER ConnectionTimeout
    The connection timeout in seconds. By default it is 30 seconds.

    .PARAMETER DatabaseCulture
    The database culture code (es. it-IT). It's used to understand the
    decimal separator properly. By default, it is 'en-US'.

    .PARAMETER BatchSize
    The size (number of rows) of batches that are written to the output
    file until data to extract is over.

    .PARAMETER OutputFileFullPath
    Full path (including filename and csv extension) of the output file.

    .PARAMETER SeparatorChar
    Character used to build string separators shown in console.

    .EXAMPLE
    Import-Module -Name "C:\your-folder\SqlBulkExport.psm1"

    Export-SqlBulkCsvByPeriod -ServerName "YourServerName" -DatabaseName "YourDatabaseName" -SchemaName "yourschema" -TableViewName "your_table_name" -DateColumnName "date_column" -StartPeriod "2022-01" -EndPeriod "2022-04" -OutputFileFullPath "C:\your-output-folder\output_{}.csv"

#>
function Export-SqlBulkCsvByPeriod {

    param(
        
        [Parameter(Mandatory)]
        [string]$ServerName,

        [string]$Port               = 1433,

        [Parameter(Mandatory)]
        [string]$DatabaseName,
        
        [string]$SchemaName         = "dbo",

        [string]$TableViewName,

        [string]$Query,

        [string]$DateColumnName,
        
        [string]$StartPeriod,
        
        [string]$EndPeriod,

        [string]$User,

        [string]$Password,

        [int]$ConnectionTimeout     = 30,

        [string]$DatabaseCulture    = "en-US",

        [int]$BatchSize             = 100000,
        
        [Parameter(Mandatory)]
        [string]$OutputFileFullPath
    )

    $regexDaily     = "^\d{4}-\d{2}-\d{2}$"
    $regexMonthly   = "^\d{4}-\d{2}$"
    $regexYearly    = "^\d{4}$"


    if (($StartPeriod -match $regexDaily) -and ($EndPeriod -match $regexDaily)) {

        $StartPeriodParsed=[Datetime]::ParseExact($StartPeriod, "yyyy-MM-dd", $null)
        $EndPeriodParsed=[Datetime]::ParseExact($EndPeriod, "yyyy-MM-dd", $null)
        $DateToken = "yyyyMMdd"
        $PeriodDescr = "DAILY"
    
    } elseif (($StartPeriod -match $regexMonthly) -and ($EndPeriod -match $regexMonthly)) {
        
        $StartPeriodParsed=[Datetime]::ParseExact($StartPeriod, "yyyy-MM", $null)
        $EndPeriodParsed=[Datetime]::ParseExact($EndPeriod, "yyyy-MM", $null)
        $DateToken = "yyyyMM"
        $PeriodDescr = "MONTHLY"

    } elseif (($StartPeriod -match $regexYearly) -and ($EndPeriod -match $regexYearly)) {
        
        $StartPeriodParsed=[Datetime]::ParseExact($StartPeriod, "yyyy", $null)
        $EndPeriodParsed=[Datetime]::ParseExact($EndPeriod, "yyyy", $null)
        $DateToken = "yyyy"
        $PeriodDescr = "YEARLY"

    }
    else # [start and end time period types are different]
    {

        Write-Error "`b`bERROR! Start and end time period types must match." -CategoryActivity " `b"

    }


    if ($StartPeriodParsed -and $EndPeriodParsed -and ($StartPeriodParsed -le $EndPeriodParsed)) {

        $timer = [Diagnostics.Stopwatch]::StartNew()

        $titleStr1 = "  EXTRACTING $($PeriodDescr) DATA FROM [$($SchemaName)].[$($TableViewName)] "
        $titleStr2 = "  FOR PERIODS FROM $($StartPeriod) TO $($EndPeriod)"
        $separator = GenerateSeparator -Title $titleStr1

        Write-Host $separator
        Write-Host $titleStr1
        Write-Host $titleStr2
        Write-Host $separator
        
        $start = $StartPeriodParsed

        while($start -le $EndPeriodParsed) {

            $startDateStr = $start.ToString("yyyy-MM-dd")
            $endDateStr = $start.AddMonths(1).ToString("yyyy-MM-dd")

            if ($PSBoundParameters.ContainsKey("User")) {

                if ($PSBoundParameters.ContainsKey("Password")) {
        
                    $pass = $Password

                } else {
        
                    $pass = ""
        
                }
                
                Export-SqlBulkCsv -ServerName "$ServerName" -DatabaseName "$DatabaseName" -User "$User" -Password "$pass" -Query "SELECT * FROM [$SchemaName].[$TableViewName] WHERE [$DateColumnName] >= '$startDateStr' AND [$DateColumnName] < '$endDateStr'" -BatchSize $BatchSize -DatabaseCulture "$DatabaseCulture" -OutputFileFullPath "$($OutputFileFullPath.Replace('{}', $start.ToString($DateToken)))" -SeparatorChar "-"
        
                Write-Host $Command
            } else {
        
                Export-SqlBulkCsv -ServerName "$ServerName" -DatabaseName "$DatabaseName" -Query "SELECT * FROM [$SchemaName].[$TableViewName] WHERE [$DateColumnName] >= '$startDateStr' AND [$DateColumnName] < '$endDateStr'" -BatchSize $BatchSize -DatabaseCulture "$DatabaseCulture" -OutputFileFullPath "$($OutputFileFullPath.Replace('{}', $start.ToString($DateToken)))" -SeparatorChar "-"
        
            }

            $start = $start.AddMonths(1)

        }

        $t = $timer.Elapsed

        Write-Host ""
        Write-Host $separator
        Write-Host "  ALL THE CSV FILES EXTRACTED IN $(GetTimespanString($t))"
        Write-Host $separator
        Write-Host ""

    }
    else
    {

        Write-Error "`b`bERROR! Input year-month strings are not valid." -CategoryActivity " `b"

    }
}