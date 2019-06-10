# ETL 

ETL process to read in a MariaDb ‘slow query log’.

## Introduction

We use a MariaDb at work, which is a split from MySql.  

Like everything to do with MariaDb / MySql there’s lots of badly documented half baked solutions which you cobble together so you can achieve something and trying
to ‘optimise’ is one of those things.  I’ve been given this task, but no one really know what ‘optimise’ means or how we might measure it.

MariaDb writes an entry into a ‘slow query log’ if a query takes more than a certain threshold.  The file size for
our log for a database that is not really that busy is 7GB.  The network admin uses mysterious script to summarise the raw data but I decide to load the ‘slow query log’ itself to perform some analysis that way.


## Visual Studio Solution

I created a Visual Studio Solution with two projects:

![Solution](images/Solution.PNG)

1. A database project to hold the staging table and results.
2. A SSIS project to move the data into the database project.

### Database Project

This is not about Database Projects per se, but I can really recommend considering them.  You can put your databases into source control and all those other good things that come with that.
In this project, I’ve created two tables and a stored procedure.

![ProjectDatabase](images/ProjectDatabase.PNG)

#### Text File Table

The first table is to read in the text file.  I need to the automatically generated line numbers to make sure I keep the order when reading the lines out of the table.

![TableTextFile](images/TableTextFile.PNG)

#### Slow Queries Table

The second table stores the process rows from the Text File table.  I’ve created a **checksum** as a persisted calculated column
so I can group similar queries.  I’m taking the checksum of the **SELECT** and **FROM** clauses.  This has the affect
of removing **USE** statements and everything from the **WHERE** onwards.  If the **SELECT** and the **FROM** (include the **JOIN**s) are the same, I’m assuming it’s the same query.  There’s room for improvement here, but it’s a start.

![TableMariaDbSlowLog](images/TableMariaDbSlowLog.PNG)

### SSIS

The SSIS project has a single package to read in the text file.

![ProjectSSIS](images/ProjectSSIS.PNG)

#### Control Flow

The control flow clears the staging table, loads the text file into the staging table, and the stored procedure transforms it and loads it into the result table. 

![SSISControlFlow](images/SSISControlFlow.PNG)

#### Data Flow

No magic here... reading a text file into a table.

![SSISDataFlow](images/SSISDataFlow.PNG)

#### Stored Procedure

I’ve included the source code to the stored procedure in this repository.

## Results

This is what it looks like in action.

![SSISSuccess](images/SSISSuccess.PNG)

### Text File Table

You can see the text file has been loaded with row numbers with the following structure:

1. Header lines starting with #
2. *n* lines for the query, not starting with a #

The stored procedure loops through the lines and does this:

1. Read in the header lines (the ones starting with a #) and concatenate them (to make the text-split easier)
2. Build the sql statement by looping until I find a line with a #, i.e. the next block
3. Insert values into the table

This the log file after it’s read into the FileText table.

![RowsTextFile](images/RowsTextFile.PNG)

And this is what the final result looks like after the stored procedure has run:

![RowsTableMariaDbSlowLog](images/RowsTableMariaDbSlowLog.PNG)

Now we can query the table to find out which queries took a long time, or which ones executed particularly often, etc.

Beats a text file!
