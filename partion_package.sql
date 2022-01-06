--create table to store partition metadata

create table PARTITION_MSTR (PART_ID NUMBER, PART_SIZE char(1), NUM_MONTH NUMBER);

--sample inserts


--this will create 3 LIST partitions of your table based on the PART_ID column having one of these 3 values below (1,2, or 3)
--further, it will create RANGE subpartitions based on the monthly interval specified in the NUM_MONTH column. (Monthly, Quarterly and Annually respectively)
--the value assigned to NUM_MONTH MUST be a factor or 12 (1, 2, 3, 4, 6, or 12)

insert into PARTITION_MSTR values (1, 'L', 1);
insert into PARTITION_MSTR values (2, 'M', 3);
insert into PARTITION_MSTR values (3, 'S', 12);

--procedure to create initial partition

create or replace procedure  create_part( v_schema_name IN VARCHAR2, v_tab_name IN VARCHAR2, v_date_col IN VARCHAR2, v_table_space IN VARCHAR2, v_online IN NUMBER, v_prev_years IN NUMBER, v_arc_table_space IN VARCHAR2, v_arc_years IN NUMBER)
AS
v_part_name VARCHAR2(4);
v_part_id NUMBER;
v_mainpartcheck NUMBER;
v_startstat VARCHAR2(200);
v_partstat VARCHAR2(1000);
v_substatlast CLOB;
v_substatprev VARCHAR2(2000);
v_substat VARCHAR2(2000);
v_commstat CLOB;
v_indexstat VARCHAR(1000);
v_indexcheck NUMBER;
v_bigcommstat CLOB;
v_thisyear varchar2(4);
v_lastyear varchar2(4);
v_month varchar2(2);
v_monthnum NUMBER;
v_subpart_name varchar2(20);
v_subpartcheck NUMBER;
v_subdateval varchar2(10);
v_prev_year NUMBER;
v_interval NUMBER;
v_count NUMBER;
v_prev_table_space VARCHAR2(128);

cursor mainpart_cur is select distinct('P'||to_char(part_id)), part_id from PARTITION_MSTR  where num_month is not null and num_month > 0 order by part_id;


BEGIN

    v_thisyear := to_char(current_date,'yyyy');
    --v_lastyear := to_char(current_date - 365,'yyyy');

    v_commstat := '';  --initialize variable

    --start by creating the begining of the command statement
    v_startstat:= 'alter table '||v_schema_name||'.'||v_tab_name||' MODIFY partition by LIST (part_id) subpartition by RANGE ('||v_date_col||')';


    select count(*) into v_count from PARTITION_MSTR  where num_month is not null and num_month > 0 ;
    --now start creating the commands for partition and subpartition scheme
    open mainpart_cur;  -- this is the list of partitions we need to create
    loop
        fetch mainpart_cur into v_part_name, v_part_id;
        exit when v_count  = 0;

            v_count := v_count - 1;

            v_partstat := 'partition '||v_part_name||' VALUES ('||to_char(v_part_id)||') TABLESPACE '||v_table_space;

            v_prev_year := v_prev_years; -- needs to be initialized for each partition loop
            v_substatlast := '';  -- needs to be initialized for each partition loop
            v_substatprev := '';  -- needs to be initialized for each partition loop

            if v_prev_year > 0 then --add subpartitions for the previous years
                while v_prev_year > 0 loop
                    v_lastyear := v_thisyear - v_prev_year;
                    v_prev_year := v_prev_year - 1; -- to exit loop
                    
                    --logic for assigning tablespace to previous years

                    if v_prev_year >= v_arc_years then
                        v_prev_table_space := v_arc_table_space;
                    else    
                        v_prev_table_space := v_table_space;
                    end if;
                    

                    with lastmonthlist(monthnum) AS (
                    select '1' as monthnum from dual
                    union
                    select '2' as monthnum from dual
                    union
                    select '3' as monthnum from dual
                    union
                    select '4' as monthnum from dual
                    union
                    select '5' as monthnum from dual
                    union
                    select '6' as monthnum from dual
                    union
                    select '7' as monthnum from dual
                    union
                    select '8' as monthnum from dual
                    union
                    select '9' as monthnum from dual
                    union
                    select '10' as monthnum from dual
                    union
                    select '11' as monthnum from dual
                    union
                    select '12' as monthnum from dual)
                    select LISTAGG('SUBPARTITION p'||to_char(m.part_id)||'_'||v_lastyear||case when m.num_month <> 12 then '_'||monthnum else ''end||'  VALUES LESS THAN (to_date(''01-'||case when m.num_month <> 12 and monthnum <> '12' then to_char(cast(monthnum as number)+1)  else '01' end||'-'||case when m.num_month <> 12 and monthnum <> '12' then v_lastyear else to_char(v_lastyear + '1') end||''', ''dd-mm-yyyy'')) TABLESPACE '||v_prev_table_space,', ')
                    within group (order by cast(monthnum as number))
                    into v_substatprev from lastmonthlist l, PARTITION_MSTR m
                        where mod(cast(monthnum as number), m.num_month) = 0 and part_id = v_part_id order by part_id, monthnum;
                    v_substatprev := v_substatprev||', ';    
                    v_substatlast := v_substatlast||v_substatprev;
                end loop;        
            else
                    v_substatlast := '';    
            end if;

            with monthlist(monthnum) AS (
            select '1' as monthnum from dual
            union
            select '2' as monthnum from dual
            union
            select '3' as monthnum from dual
            union
            select '4' as monthnum from dual
            union
            select '5' as monthnum from dual
            union
            select '6' as monthnum from dual
            union
            select '7' as monthnum from dual
            union
            select '8' as monthnum from dual
            union
            select '9' as monthnum from dual
            union
            select '10' as monthnum from dual
            union
            select '11' as monthnum from dual
            union
            select '12' as monthnum from dual)
            select LISTAGG('SUBPARTITION p'||to_char(m.part_id)||'_'||v_thisyear||case when m.num_month <> 12 then '_'||monthnum else ''end||'  VALUES LESS THAN (to_date(''01-'||case when m.num_month <> 12 and monthnum <> '12' then to_char(cast(monthnum as number)+1)  else '01' end||'-'||case when m.num_month <> 12 and monthnum <> '12' then to_char(current_date,'yyyy') else to_char(current_date + 365,'yyyy') end||''', ''dd-mm-yyyy'')) TABLESPACE '||v_table_space,', ') 
            within group (order by cast(monthnum as number))
            into v_substat from monthlist l, PARTITION_MSTR m
                where mod(cast(monthnum as number), m.num_month) = 0 and part_id = v_part_id order by part_id, monthnum;

            if v_count <> 0 then     
                v_commstat := v_commstat || v_partstat||'('||v_substatlast||v_substat||'), ';  
            else
                v_commstat := v_commstat || v_partstat||'('||v_substatlast||v_substat||')';  
            end if;


    end loop;
    close mainpart_cur;

    v_indexstat := '';
    select count(*) into v_indexcheck from all_indexes where upper(table_owner) = upper(v_schema_name) and upper(table_name) = upper(v_tab_name) and UNIQUENESS <> 'UNIQUE';
    if v_indexcheck > 0 then -- there are indexes we are converting to local
        select ' UPDATE INDEXES ('||LISTAGG(INDEX_NAME||' LOCAL', ',')||')' into v_indexstat from all_indexes where upper(table_owner) = upper(v_schema_name) and upper(table_name) = upper(v_tab_name) and UNIQUENESS <> 'UNIQUE';
    end if;

    if v_online  = 1 then -- do it online
        v_bigcommstat := v_startstat || '('|| v_commstat||') ENABLE ROW MOVEMENT ONLINE' || v_indexstat;
    else
        v_bigcommstat := v_startstat || '('|| v_commstat||') ENABLE ROW MOVEMENT' || v_indexstat;
    end if;    
    execute immediate v_bigcommstat;
    --insert into proc_debug values (v_bigcommstat);
    commit;


END;

/



--procedure to maintain partitions

create or replace procedure  maintain_part( v_schema_name IN VARCHAR2, v_tab_name IN VARCHAR2, v_table_space IN VARCHAR2)
AS
v_part_name VARCHAR2(4);
v_part_id NUMBER;
v_mainpartcheck NUMBER;
v_partstat VARCHAR2(1000);
v_substat VARCHAR2(2000);
v_commstat VARCHAR2(4000);
v_thisyear varchar2(4);
v_nextyear varchar2(4);
v_month varchar2(2);
v_monthnum NUMBER;
v_subpart_name varchar2(20);
v_subpartcheck NUMBER;
v_subdateval varchar2(10);
v_interval NUMBER;

cursor mainpart_cur is select distinct('P'||to_char(part_id)), part_id from PARTITION_MSTR  where num_month is not null and num_month > 0 ;

BEGIN

    open mainpart_cur;  -- this is the list of partitions we (may) need to create
    loop
        fetch mainpart_cur into v_part_name, v_part_id;
        exit when mainpart_cur%notfound;
        --check if the partition has already been created for the table passed
        select count(*) into v_mainpartcheck from all_tab_partitions where upper(table_name) = upper(v_tab_name) and upper(partition_name) = upper(v_part_name); 
    
        if v_mainpartcheck = 0 then -- the partition has not been created so add it
            --v_commstat := 'alter table '||v_schema_name||'.'||v_tab_name||' add partition '||v_part_name||' VALUES ('||to_char(v_part_id)||')';
            v_partstat := 'alter table '||v_schema_name||'.'||v_tab_name||' add partition '||v_part_name||' VALUES ('||to_char(v_part_id)||') TABLESPACE '||v_table_space;

            v_thisyear := to_char(current_date,'yyyy'); --init and reset value

            with monthlist(monthnum) AS (
            select '1' as monthnum from dual
            union
            select '2' as monthnum from dual
            union
            select '3' as monthnum from dual
            union
            select '4' as monthnum from dual
            union
            select '5' as monthnum from dual
            union
            select '6' as monthnum from dual
            union
            select '7' as monthnum from dual
            union
            select '8' as monthnum from dual
            union
            select '9' as monthnum from dual
            union
            select '10' as monthnum from dual
            union
            select '11' as monthnum from dual
            union
            select '12' as monthnum from dual)
            select LISTAGG('SUBPARTITION p'||to_char(m.part_id)||'_'||v_thisyear||case when m.num_month <> 12 then '_'||monthnum else ''end||'  VALUES LESS THAN (to_date(''01-'||case when m.num_month <> 12 and monthnum <> '12' then to_char(cast(monthnum as number)+1)  else '01' end||'-'||case when m.num_month <> 12 and monthnum <> '12' then to_char(current_date,'yyyy') else to_char(current_date + 365,'yyyy') end||''', ''dd-mm-yyyy'')) TABLESPACE '||v_table_space,', ') 
            within group (order by cast(monthnum as number))
            into v_substat from monthlist l, PARTITION_MSTR m
                where mod(cast(monthnum as number), m.num_month) = 0 and part_id = v_part_id order by part_id, monthnum;
            v_commstat := v_partstat||'('||v_substat||')';  
            --v_commstat := 'alter table '||v_schema_name||'.'||v_tab_name||' SPLIT PARTITION PDEF VALUES ('||to_char(v_part_id)||') INTO (partition '||v_part_name||', partition PDEF)';
            execute immediate v_commstat;
            --insert into proc_debug values (v_commstat);
            --commit;
        end if;

        select num_month into v_interval from PARTITION_MSTR where part_id = v_part_id;

    
        --create sub partitions based on current date being executed
    
        --loop for current year
        v_thisyear := to_char(current_date,'yyyy');
        v_nextyear := to_char(current_date + 365,'yyyy');
        v_monthnum := 0;
        while  v_monthnum < 12  --this year's loop
        loop
            v_monthnum := v_monthnum + v_interval; 

            if v_interval <> 12 then -- not a yearly partition
                v_subpart_name := v_part_name||'_'||v_thisyear||'_'||to_char(v_monthnum);
                if v_monthnum <> 12 then
                    v_subdateval := '01-'||to_char(v_monthnum + 1)||'-'||v_thisyear;
                else
                    v_subdateval := '01-'||'01'||'-'||v_nextyear;        
                end if;    
            else
                v_subpart_name := v_part_name||'_'||v_thisyear;
                v_subdateval := '01-'||'01'||'-'||v_nextyear;

                v_monthnum := 13; -- setting this to exit loop
            end if;


            --check if subpartition exists
            select count(*) into v_subpartcheck from all_tab_subpartitions where upper(table_name) = upper(v_tab_name) and upper(partition_name) = upper(v_part_name) and upper(subpartition_name) = upper(v_subpart_name);
            if v_subpartcheck  = 0 then -- need to create the subpartition
                v_commstat := 'alter table '||v_schema_name||'.'||v_tab_name||' modify partition '||v_part_name||' add subpartition '||v_subpart_name||' VALUES LESS THAN (to_date('''||v_subdateval||''',''dd-mm-yyyy'')) TABLESPACE '||v_table_space;
                execute immediate v_commstat;
                --insert into proc_debug values (v_commstat);
                --commit;
            end if;

        end loop;


        --loop for next year
        v_thisyear := to_char(current_date + 365,'yyyy');
        v_nextyear := to_char(current_date + (365*2),'yyyy');
        v_monthnum := 0;
        while  v_monthnum < 12  --this year's loop
        loop
            v_monthnum := v_monthnum + v_interval; 

            if v_interval <> 12 then -- not a yearly partition
                v_subpart_name := v_part_name||'_'||v_thisyear||'_'||to_char(v_monthnum);
                if v_monthnum <> 12 then
                    v_subdateval := '01-'||to_char(v_monthnum + 1)||'-'||v_thisyear;
                else
                    v_subdateval := '01-'||'01'||'-'||v_nextyear;        
                end if;    
            else
                v_subpart_name := v_part_name||'_'||v_thisyear;
                v_subdateval := '01-'||'01'||'-'||v_nextyear;

                v_monthnum := 13; -- setting this to exit loop
            end if;


          --check if subpartition exists
            select count(*) into v_subpartcheck from all_tab_subpartitions where upper(table_name) = upper(v_tab_name) and upper(partition_name) = upper(v_part_name) and upper(subpartition_name) = upper(v_subpart_name);
            if v_subpartcheck  = 0 then -- need to create the subpartition
                v_commstat := 'alter table '||v_schema_name||'.'||v_tab_name||' modify partition '||v_part_name||' add subpartition '||v_subpart_name||' VALUES LESS THAN (to_date('''||v_subdateval||''',''dd-mm-yyyy'')) TABLESPACE '||v_table_space;
                execute immediate v_commstat;
                --insert into proc_debug values (v_commstat);
                --commit;
            end if;

        end loop;
    end loop;
    close mainpart_cur;


END;

/
