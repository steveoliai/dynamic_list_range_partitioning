# dynamic list range partitioning
Stored Procedures to create and maintain composite (list/range) partitioned tables in and Oracle Database.

This was created to be a very flexible way to partition a table.  If you have data that you are considering to partition by date, but also have another key in the table where the number of rows in a given date range are widely skewed for that key, this would help create subpartitions for the keys with different date ranges.

The date range must be represented by number of months. The smallest date range supported it 1 month and the largest is 12. The number must be a factor of 12 (1, 2, 3, 4, 6, or 12).

The procedures expect any table that is to be partitioned to have a PART_ID column.  Populated it with a value that corresponds to which MAIN/LIST partition the row should belong to.

## Metadata Table
PARTITION_MSTR:

This is the table where you define the structure of how you would like to have you table partitioned.

create table PARTITION_MSTR (PART_ID NUMBER, PART_SIZE char(1), NUM_MONTH NUMBER);

Sample inserts are in the script.

## Procedure Name:
create_part: 

Once you've defined your partitioning scheme in PARTITION_MSTR and have also updated the rows in your table with a PART_ID value which exists in PARTITION_MSTR, you can run this procedure to partition the table.

## Paremeters:

1. v_schema_name IN VARCHAR2:  The schema you're working in.
2. v_tab_name IN VARCHAR2: The name of the table you want to partition
3. v_date_col IN VARCHAR2:  The date column to be used for the subpartitions
4. v_table_space IN VARCHAR2:  The name of the table_space to create the partitions in which hold more recent data
5. v_online IN NUMBER:  This can be 0 or 1.  It's a flag on whether to perform the operation online.
6. v_prev_years IN NUMBER: The number of calendar years you want to go back and have paritioned
7. v_arc_table_space IN VARCHAR2:  The name of the table_space to create the partitions in which hold older data.  This is an option to put older data in less expensive storage.
8. v_arc_years IN NUMBER: The number of years to go back to identify parititions which should be created in the tablespace specified in v_arc_table_space. 


## Procedure Name:
maintain_part:

If you need to create paritions for the next calendar year OR if you add a row to PARTITION_MSTR for a new PART_ID, this procedure will add the new paritions.

## Paremeters:

1. v_schema_name IN VARCHAR2:  The schema you're working in.
2. v_tab_name IN VARCHAR2: The name of the table you want to partition
4. v_table_space IN VARCHAR2:  The name of the table_space to create the partitions in which hold more recent data


## Note:
If you decide to change the scheme entire and want to re-partition your data, define you new scheme in PARTITION_MSTR, and re-run the create_part procedure.