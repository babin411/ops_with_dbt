with actual_drop_off_raw as
(
	select 
		jsonb_extract_path_text(_airbyte_data, 'Day') as day,
		jsonb_extract_path_text(_airbyte_data, 'Date') as date,
		jsonb_extract_path_text(_airbyte_data, 'Name') as name,
		jsonb_extract_path_text(_airbyte_data, 'Time') as time,
		jsonb_extract_path_text(_airbyte_data, 'Gender') as gender,
		jsonb_extract_path_text(_airbyte_data, 'Status') as status,
		jsonb_extract_path_text(_airbyte_data, 'Shift') as shift,
		jsonb_extract_path_text(_airbyte_data, 'Address') as address,
		jsonb_extract_path_text(_airbyte_data, 'Department') as department,
		jsonb_extract_path_text(_airbyte_data, 'Source') as source,
		'_airbyte_emitted_at'
	from {{source('ops_ia','_airbyte_raw_test_actual_drop_off')}}
)
,
cleaned_drop_off as
(
	select 
		upper(tado.name) as name,
		upper(tado.department) as department ,
		tado.gender as gender,
		case 
			when position('(' in tado.address)!=0
				then trim(left(tado.address , position('(' in tado.address)-1))
			else tado.address 
		end as address,
		case
			when tado.address like '%(%)%'
				then trim(substring(tado.address, position('(' in tado.address), position(')' in tado.address)))
			else null
		end as address_remarks,
		tado.time  as time,
		tado.date as date,
		case 
			when upper(tado.status) is null then 'NOT FILLED'
			else upper(tado.status)
		end as status,
		tado.shift  as shift,
		tado.source as source,
		row_number()
		over (partition by tado.department , tado.name , tado.date  order by tado.date) as row_number
	from actual_drop_off_raw tado 
	where tado.name is not null
)
,
actual_drop_off_cleaned as (
		select
			c.name,
			c.department,
			c.gender,
			c.time,
			c.date,
			c.status,
			c.shift,
			c.source,
			c.address,
			c.address_remarks
		from cleaned_drop_off c
		where c.row_number=1
	)
select
	*
from actual_drop_off_cleaned


