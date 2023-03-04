with airbyte_raw_data as (
	select 
		jsonb_extract_path_text(_airbyte_data, 'Employee_Name') as "employee_name",
		jsonb_extract_path_text(_airbyte_data, 'Employee_Email') as "employee_email",
		jsonb_extract_path_text(_airbyte_data, 'Gender') as "gender",
		jsonb_extract_path_text(_airbyte_data, 'Main_Department') as "main_department",
		jsonb_extract_path_text(_airbyte_data, 'Sub_Department') as "sub_department",
		'_airbyte_emitted_at'
	from {{source('ops_ia','_airbyte_raw_employee_list')}}
)
,
cleaned_data as (
	select
		trim(upper(cte1.employee_name)),
		trim(cte1.employee_email),
		trim(cte1.gender),
		trim(upper(cte1.main_department)) as main_department,
		case
			when cte1.sub_department is null then trim(upper(cte1.main_department))
			else trim(upper(cte1.sub_department))
		end as sub_department
	from airbyte_raw_data cte1
)
select
	*
from cleaned_data