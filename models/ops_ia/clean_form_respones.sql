with airbyte_raw_form_responses as (
	select 
		jsonb_extract_path_text(_airbyte_data, 'Day') as day,
		jsonb_extract_path_text(_airbyte_data, 'Email') as email,
		jsonb_extract_path_text(_airbyte_data, 'Shift') as shift,
		jsonb_extract_path_text(_airbyte_data, 'Timestamp') as timestamp,
		jsonb_extract_path_text(_airbyte_data, 'Employee_Name') as employee_name,
		jsonb_extract_path_text(_airbyte_data, 'Food_Coupon_ID') as food_coupon_id,
		jsonb_extract_path_text(_airbyte_data, 'Main_Department') as main_department,
		jsonb_extract_path_text(_airbyte_data, 'Sub_Department') as sub_department,
		jsonb_extract_path_text(_airbyte_data, 'Drop_Off_Required') as drop_off_required,
		'_airbyte_emitted_at'
	from dbt_transformed.dbt_employee_list."_airbyte_raw_test_form_responses" 
)
,
cleaned_data as (
	select
		md5(trim(upper(arfr.employee_name))) as employee_name,
		md5(trim(arfr.email)) as email,
		cel.gender,
		arfr.timestamp::date as date,
		trim(upper(arfr.day)) as day,
		case 
			when position('(' in arfr.food_coupon_id)!=0
				then trim(left(arfr.food_coupon_id, position('(' in arfr.food_coupon_id)-1))
			else arfr.food_coupon_id
		end as food_coupon_id,
		case 
			when upper(arfr.drop_off_required) similar to '%NO%|%DAY%|%PATHO%' then 'NO'
			when upper(arfr.drop_off_required) like '%YES%' then 'YES'
			when upper(arfr.drop_off_required) like '%OWN VEHICLE%' then 'OWN VEHICLE'
			when arfr.drop_off_required is null then null
			else 'OTHERS'
		end as drop_off_required,
		cel.main_department,
		cel.sub_department
	from airbyte_raw_form_responses arfr
	left join {{'clean_employee_list'}} cel 
	on email = cel.employee_email 
)
select
	*
from cleaned_data;