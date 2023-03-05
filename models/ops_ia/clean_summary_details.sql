with airbyte_raw_summary_details as (
	select 
		jsonb_extract_path_text(_airbyte_data, 'Day 1') as day_1,
		jsonb_extract_path_text(_airbyte_data, 'Day 2') as day_2,
		jsonb_extract_path_text(_airbyte_data, 'Day 3') as day_3,
		jsonb_extract_path_text(_airbyte_data, 'HODs') as hod,
		jsonb_extract_path_text(_airbyte_data, 'Staff using own vechile (Y/N)') as own_vehicle,
		jsonb_extract_path_text(_airbyte_data, 'Gender') as gender,
		jsonb_extract_path_text(_airbyte_data, 'Department') as department,
		jsonb_extract_path_text(_airbyte_data, 'Project Name') as project_name,
		jsonb_extract_path_text(_airbyte_data, 'Days in office') as days_in_office,
		jsonb_extract_path_text(_airbyte_data, 'Team member name') as team_member_name,
		jsonb_extract_path_text(_airbyte_data, 'Drop off location') as drop_off_location,
		jsonb_extract_path_text(_airbyte_data, 'Inside Valley (Y/N)') as inside_valley,
		jsonb_extract_path_text(_airbyte_data, 'Booking Responsible Person') as booking_responsible_person,
		jsonb_extract_path_text(_airbyte_data, 'Needs Drop off (Y/N) (Applicable for evening shift only)') as needs_drop_off,
		jsonb_extract_path_text(_airbyte_data, 'Location (as per the HR record - Latest information as of Aug 2022)') as location,
		_airbyte_emitted_at
	from {{source('ops_ia','_airbyte_raw_test_summary_details')}}
)
,
summary_details_cleaned as (
		select 
			trim(upper(tsd.booking_responsible_person))  as Booking_responsible_person,
		trim(upper(tsd.department)) as department,
		trim(upper(tsd.team_member_name))  as team_member_name,
		tsd.gender  as gender,
		case
			when tsd.inside_valley='Yes' then 'Y'
			when tsd.inside_valley='No' then 'N'
			else 'Not Filled'
		end as inside_valley,
		case
			when tsd.needs_drop_off='Yes' then 'Y'
			when tsd.needs_drop_off='No' then 'N'
			else 'Not Filled'
		end as needs_drop_off,
		case 
			when tsd.location='-' then 'Not Filled'
			else tsd.location
		end as drop_off_location,
		tsd.days_in_office,
		tsd.day_1 as day_1,
		tsd.day_2 as day_2,
		tsd.day_3 as day_3,
	    case
	      	when trim(lower(tsd.own_vehicle))='yes' 
	      		or trim(lower(tsd.own_vehicle))='y' 
	      		or trim(lower(tsd.own_vehicle)) like '%need allowance%' then 'Y'
			when trim(upper(tsd.own_vehicle)) = 'NO'
		    	or trim(upper(tsd.own_vehicle)) = 'N' then 'N'
		  	else tsd.own_vehicle
	    end as using_own_vehicle,
	    tsd.location as Location_from_HR_record_Aug_2022,
	    case 
	    	when tsd.hod='Yes' then 'Y'
	    	else null
	    end as HODs
	from airbyte_raw_summary_details tsd 
)
,
final_table as (
	select
		upper(department) as department,
		team_member_name,
		gender,
		inside_valley,
		needs_drop_off,
		day_1,
		day_2,
		day_3,
		using_own_vehicle
	from summary_details_cleaned
	where department is not null
)
select * from final_table


