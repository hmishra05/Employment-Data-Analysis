
-------------------------CREATING TABLE graduate_employment_data TO ACCOMMODATE THE VALUES --------------------------------------

----------------------Creating a table to import the graduate_employement_records------------------------------------------------

create table graduate_data.graduate_employment_data (
    card_id UUID,
    recipient_primary_major VARCHAR(255),
    recipient_secondary_majors TEXT,
    recipient_education_level VARCHAR(50),
    recipient_primary_college VARCHAR(255),
    recipient_gender VARCHAR(10),
    complete_term INTEGER,
    response_status VARCHAR(50),
    outcome VARCHAR(50),
    employer_industry VARCHAR(255),
    employment_category VARCHAR(255),
    employment_type VARCHAR(50),
    job_function VARCHAR(255),
    offer_date DATE,
    annual_salary NUMERIC(15, 2),
    pay_schedule VARCHAR(50),
    acad_career_descr VARCHAR(50),
    acad_plan_adj_descr VARCHAR(255),
    acad_plan_type_descr VARCHAR(50),
    degree_descr VARCHAR(255),
    campus VARCHAR(50),
    arizona_residency_descr VARCHAR(50),
    minority_status_descr VARCHAR(50),
    citz_country_descr VARCHAR(100)
);


----Selecting all the columns to view the data----

select * from graduate_data.graduate_employment_data


----Checking the count of rows to ensure if all the records have been imported successfully-----

select count(*) as total_data_count from graduate_data.graduate_employment_data


------------------------- DATA CLEANING OF GRADUATE STUDENTS EMPLOYMENT DATA ------------------

---Identifying if any entry is being repeated (duplicate values) in the dataset using window function----

with row_number_query AS (
select card_id, row_number() over ( partition by card_id,
recipient_primary_major, recipient_secondary_majors,
recipient_education_level, recipient_primary_college, recipient_gender,
complete_term, response_status, outcome, employer_industry,
employment_category, employment_type, job_function, offer_date,
annual_salary, pay_schedule, acad_career_descr, acad_plan_adj_descr,
acad_plan_type_descr, degree_descr, campus, arizona_residency_descr,
minority_status_descr, citz_country_descr
order by card_id) as row_number_column
from graduate_data.graduate_employment_data
)
select * from row_number_query
where row_number_column > 1;

---Checking if the data with row_number_column > 1 are exactly the same entries before deleting them ----

select * from graduate_data.graduate_employment_data
where card_id = '01495981-e4c4-3008-008f-a6d581749b0f';

select * from graduate_data.graduate_employment_data
where card_id = 'b7c83db8-5a34-b7ad-12b9-52062533342a';


-----Deleting the duplicate values from the dataset (ctid serves as a unique row identifier in PostgreSQL and is a safe practice to ensure that only the selected value is being deleted)---

with row_number_query as (
select ctid, row_number() over ( partition by 
card_id, recipient_primary_major, recipient_secondary_majors,
recipient_education_level, recipient_primary_college, recipient_gender,
complete_term, response_status, outcome, employer_industry,
employment_category, employment_type, job_function, offer_date,
annual_salary, pay_schedule, acad_career_descr, acad_plan_adj_descr,
acad_plan_type_descr, degree_descr, campus, arizona_residency_descr,
minority_status_descr, citz_country_descr
order by card_id) as row_number_column
from graduate_data.graduate_employment_data
)
delete from graduate_data.graduate_employment_data
where ctid in (
    select ctid
    from row_number_query
    where row_number_column > 1
);


------FIXING THE DATA STRUCTURE AND ERRORS ENSURING DATA INTEGRITY FOR ANALYSIS -----------

-- Handling recipient_primary_majors with incorrect structure and null values ----
select distinct(recipient_primary_major) from graduate_data.graduate_employment_data
order by recipient_primary_major

update graduate_data.graduate_employment_data
set recipient_primary_major = 'Not Known'
where recipient_primary_major is null;

-- Handling recipient_secondary_majors with incorrect structure and null values ----
select distinct(recipient_secondary_majors) from graduate_data.graduate_employment_data
order by recipient_secondary_majors

update graduate_data.graduate_employment_data
set recipient_secondary_majors = 'Not Reported'
where recipient_secondary_majors is null;

-- Handling recipient_gender with incorrect structure and null values ----
select distinct(recipient_gender) from graduate_data.graduate_employment_data

update graduate_data.graduate_employment_data
set recipient_gender = 
case 
	when recipient_gender = 'Male' then 'Male'
	when recipient_gender = 'Female' then 'Female'
	when recipient_gender = 'Intersex' then 'Intersex'
	when recipient_gender = 'Agender' then 'Agender'
	when recipient_gender = 'Man' then 'Male'
    else 'Unknown'
end;

---Handling incorrect recipient_primary_college data with null values ---
select distinct(recipient_primary_college) from graduate_data.graduate_employment_data
order by recipient_primary_college

update graduate_data.graduate_employment_data
set recipient_primary_college = 'W.P. Carey School of Business'
where recipient_primary_college = 'W. P. Carey School of Business'

select count(*) as cnt from graduate_data.graduate_employment_data
where recipient_primary_college is null;

update graduate_data.graduate_employment_data
set recipient_primary_college = 'Other'
where recipient_primary_college is null;

---Handling incorrect outcome data and its null values ---
select distinct(outcome) from graduate_data.graduate_employment_data

select count(*) from graduate_data.graduate_employment_data
where outcome is null;

update graduate_data.graduate_employment_data
set outcome = 'Unknown'
where outcome is null;

---Handling incorrect employer_industry data and its null values ---
select distinct(employer_industry) from graduate_data.graduate_employment_data
order by employer_industry

select count(*) from graduate_data.graduate_employment_data
where employer_industry is null;

update graduate_data.graduate_employment_data
set employer_industry = 'Unspecified Industry'
where employer_industry is null;

---Handling incorrect employer_industry data and its null values ---
select distinct(employment_category) from graduate_data.graduate_employment_data
order by employment_category

select count(*) from graduate_data.graduate_employment_data
where employment_category is null;

update graduate_data.graduate_employment_data
set employment_category = 'Other'
where employment_category is null;

---Handling incorrect employer_industry data and its null values ---
select distinct(employment_type) from graduate_data.graduate_employment_data
order by employment_type

select count(*) from graduate_data.graduate_employment_data
where employment_type is null;

update graduate_data.graduate_employment_data
set employment_type = 'Unknown'
where employment_type is null;

---Handling incorrect job_function data and its null values ---
select distinct(job_function) from graduate_data.graduate_employment_data
order by job_function

select count(*) from graduate_data.graduate_employment_data
where job_function is null;

update graduate_data.graduate_employment_data
set job_function = 'Unknown'
where job_function is null;

---Handling incorrect job_function data and its null values ---
select distinct(campus) from graduate_data.graduate_employment_data
order by campus

update graduate_data.graduate_employment_data
set campus = case 
    when campus in ('Tempe', 'TEMPE') then 'Tempe'
    when campus in ('POLY', 'Polytechnic') then 'Polytechnic'
    when campus in ('Downtown', 'DTPHX', 'TBIRD') then 'Downtown'
    when campus in ('West', 'WEST') then 'West Valley'
    else campus  -- leaving the other values as they are
end;

---Handling incorrect pay_schedule data and its null values ---
select distinct(pay_schedule) from graduate_data.graduate_employment_data
order by pay_schedule

select count(*) from graduate_data.graduate_employment_data
where pay_schedule is null;

update graduate_data.graduate_employment_data
set pay_schedule = 'Unknown'
where pay_schedule is null;


---------------Creating a column to accommodate the 'Graduation year' and 'Term' and decoding complete_term column -------------------------

-- Create a new column for graduation year
alter table graduate_data.graduate_employment_data
add column graduation_year int;

-- Update the graduation_year column with the correct year
update graduate_data.graduate_employment_data
set graduation_year = cast(substring(cast(complete_term as varchar) from 2 for 2) as int) + 2000;

-- Create a new column for graduation term
alter table graduate_data.graduate_employment_data
add column graduation_term varchar(10);

-- Update the graduation_term column based on the last digit of the complete_term
update graduate_data.graduate_employment_data
set graduation_term = 
    case 
        when right(cast(complete_term as varchar), 1) = '1' then 'Spring'
        when right(cast(complete_term as varchar), 1) = '4' then 'Summer'
        when right(cast(complete_term as varchar), 1) = '7' then 'Fall'
        else 'Unknown'
    end;

alter table graduate_data.graduate_employment_data
drop column complete_term;

alter table graduate_data.graduate_employment_data
drop column acad_plan_type_descr;



----Keeping the graduate_employment_data only for ('Tempe', 'West Valley', 'Polytechnic', 'Downtown') Campuses----------

delete from graduate_data.graduate_employment_data
where campus not in ('Tempe', 'West Valley', 'Polytechnic', 'Downtown');



----------------------------------------- ANALYZING GRADUATE STUDENTS EMPLOYMENT DATA ---------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------

-- 1)	What are the outcome trends of BAA (Black/African American) students over the course of two years and how do they compare to other minority students?

-------Analyzing each employment outcome type for Black/African American and comparing it to that of other minorities for 2022 and 2023 ----------
-------------------------------------------(MAIN QUERY)------------------------------------------------------------------------------------------

with BAA_Outcomes as (
    select
        graduation_year,
        outcome,
        count(*) as baa_count
    from
        graduate_data.graduate_employment_data
    where
        minority_status_descr = 'Black/African American'
    group by
        graduation_year, outcome
),
other_minority_outcomes as (
    select
        graduation_year,
        outcome,
        count(*) as other_minority_count
    from
        graduate_data.graduate_employment_data
    where
        minority_status_descr <> 'Black/African American'
        and minority_status_descr is not null
    group by
        graduation_year, outcome
)
select
    b.graduation_year,
    b.outcome,
    b.baa_count,
    o.other_minority_count,
    ROUND((b.baa_count::NUMERIC / nullif(o.other_minority_count, 0)) * 100, 2) as baa_to_other_minority_percentage
from
    BAA_Outcomes b
left join
    other_minority_outcomes o
    on b.graduation_year = o.graduation_year
    and b.outcome = o.outcome
order by
    b.graduation_year, b.outcome;


-------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------MISCELLANEOUS ANALYSIS FOR (QUESTION - 1)----------------------------------------------------------
------------------Analyzing the job function distribution for Black/African American are employed in ---------------------

with job_function_cte as (select job_function, sum(case when minority_status_descr = 'Black/African American' then 1 else 0 end) as BAA_job_function, 
sum(case when minority_status_descr <> 'Black/African American' then 1 else 0 end) as other_minorities_job_function
from graduate_data.graduate_employment_data
group by job_function)
select job_function, 
round(cast(BAA_job_function as decimal) / cast(other_minorities_job_function as decimal) * 100, 2) as BAA_job_functions
from job_function_cte
order by BAA_job_functions desc

---------------------------What degree_description has the most jobs for BAA---------------------------------------------------------

select acad_career_descr, outcome,
sum(case when graduation_year = 2021 then 1 else 0 end) as "2021",
sum(case when graduation_year = 2022 then 1 else 0 end) as "2022",
sum(case when graduation_year = 2023 then 1 else 0 end) as "2023"
from graduate_data.graduate_employment_data
where minority_status_descr = 'Black/African American'
group by acad_career_descr, outcome

-------------------------------BAA salary outcomes by gender of other non BAA minorities -----------------------------------------

select outcome, 
round(avg(case when recipient_gender = 'Female' and minority_status_descr = 'Black/African American' then coalesce(annual_salary,0) else 0 end),2) as "BAA Female Annual Salary",
round(avg(case when recipient_gender = 'Male' and minority_status_descr = 'Black/African American' then coalesce(annual_salary,0) else 0 end),2) as "BAA Male Annual Salary",
round(avg(case when recipient_gender = 'Intersex' and minority_status_descr = 'Black/African American' then coalesce(annual_salary,0) else 0 end),2) as "BAA Intersex Annual Salary",
round(avg(case when recipient_gender = 'Agender' and minority_status_descr = 'Black/African American' then coalesce(annual_salary,0) else 0 end),2) as "BAA Agender Annual Salary",
round(avg(case when recipient_gender = 'Unknown' and minority_status_descr = 'Black/African American' then coalesce(annual_salary,0) else 0 end),2) as "BAA Unknown Annual Salary"
from graduate_data.graduate_employment_data
where annual_salary is not null
group by outcome
----------------------------------------------------------------------------------------------------------------------------------------------


-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------Q2)	Are there differences in outcomes as it relates to gender or Arizona residency status?

-------------(MAIN QUERY FOR Question-2, 1st part) Analysis of outcomes for all the recipient_gender in graduate_employment_data -------------------------------------

select 
	recipient_gender,
    sum(case when outcome = 'Volunteering' then 1 else 0 end) as Volunteering,
    sum(case when outcome = 'Working' then 1 else 0 end) as Working,
    sum(case when outcome = 'Still Looking' then 1 else 0 end) as Still_Looking,
    sum(case when outcome = 'Continuing Education' then 1 else 0 end) as Continuing_Education,
    sum(case when outcome = 'Not Seeking' then 1 else 0 end) as Not_Seeking,
    sum(case when outcome = 'Military' then 1 else 0 end) as Military,
    sum(case when outcome = 'Unknown' then 1 else 0 end) as "Unknown"
from 
    graduate_data.graduate_employment_data
group by 
    recipient_gender
order by 
    recipient_gender

-----------------------------------------------------(MISCELLANEOUS ANALYSIS FOR QUESTION 2, 1ST PART)---------------------------------------------------------------------
--------------------------Sum of corresponding outcome for all the recipient_gender over all the graduation years (Miscellaneous Analysis) ----------------------------

select 
    graduation_year,
	recipient_gender,
    sum(case when outcome = 'Volunteering' then 1 else 0 end) as Volunteering,
    sum(case when outcome = 'Working' then 1 else 0 end) as Working,
    sum(case when outcome = 'Still Looking' then 1 else 0 end) as Still_Looking,
    sum(case when outcome = 'Continuing Education' then 1 else 0 end) as Continuing_Education,
    sum(case when outcome = 'Not Seeking' then 1 else 0 end) as Not_Seeking,
    sum(case when outcome = 'Military' then 1 else 0 end) as Military,
    sum(case when outcome = 'Unknown' then 1 else 0 end) as "Unknown"
from 
    graduate_data.graduate_employment_data
group by 
    recipient_gender, graduation_year
order by 
    graduation_year

---------------------Percentage of corresponding outcome with respect to the gender group (Miscellaneous Analysis)-------------------------------------

with total_outcomes as (
    select 
        recipient_gender,
        count(*) as total_outcomes
    from 
        graduate_data.graduate_employment_data
    group by 
        recipient_gender
)
select 
    a.recipient_gender,
    round(sum(case when a.outcome = 'Volunteering' then 1 else 0 end) * 100.0 / b.total_outcomes, 2) as Volunteering_Percentage,
    round(sum(case when a.outcome = 'Working' then 1 else 0 end) * 100.0 / b.total_outcomes, 2) as Working_Percentage,
    round(sum(case when a.outcome = 'Still Looking' then 1 else 0 end) * 100.0 / b.total_outcomes, 2) as Still_Looking_Percentage,
    round(sum(case when a.outcome = 'Continuing Education' then 1 else 0 end) * 100.0 / b.total_outcomes, 2) as Continuing_Education_Percentage,
    round(sum(case when a.outcome = 'Not Seeking' then 1 else 0 end) * 100.0 / b.total_outcomes, 2) as Not_Seeking_Percentage,
    round(sum(case when a.outcome = 'Military' then 1 else 0 end) * 100.0 / b.total_outcomes, 2) as Military_Percentage,
    round(sum(case when a.outcome = 'Unknown' then 1 else 0 end) * 100.0 / b.total_outcomes, 2) as Unknown_Percentage
from graduate_data.graduate_employment_data a
join total_outcomes b
on a.recipient_gender = b.recipient_gender
group by a.recipient_gender, b.total_outcomes
order by a.recipient_gender;



------------- (MAIN QUERY FOR Question-2, 2nd part) Analysis of outcome with respect to the arizona_residency_descr group in  graduate_data.graduate_employment_data -------------------------------------
select 
    arizona_residency_descr,
    sum(case when outcome = 'Volunteering' then 1 else 0 end) as Volunteering,
    sum(case when outcome = 'Working' then 1 else 0 end) as Working,
    sum(case when outcome = 'Still Looking' then 1 else 0 end) as Still_Looking,
    sum(case when outcome = 'Continuing Education' then 1 else 0 end) as Continuing_Education,
    sum(case when outcome = 'Not Seeking' then 1 else 0 end) as Not_Seeking,
    sum(case when outcome = 'Military' then 1 else 0 end) as Military,
    sum(case when outcome = 'Unknown' then 1 else 0 end) as "Unknown"
from 
    graduate_data.graduate_employment_data
group by 
    arizona_residency_descr
order by 
    arizona_residency_descr;

-----------------------------------------------------(MISCELLANEOUS ANALYSIS FOR QUESTION 2, 2ND PART)---------------------------------------------------------------------
-----------------Analysis of outcome for various residencies with respect to gender and graduation year-------------------------------

select 
    arizona_residency_descr,
	graduation_year, 
	recipient_gender,
    sum(case when outcome = 'Volunteering' then 1 else 0 end) as Volunteering,
    sum(case when outcome = 'Working' then 1 else 0 end) as Working,
    sum(case when outcome = 'Still Looking' then 1 else 0 end) as Still_Looking,
    sum(case when outcome = 'Continuing Education' then 1 else 0 end) as Continuing_Education,
    sum(case when outcome = 'Not Seeking' then 1 else 0 end) as Not_Seeking,
    sum(case when outcome = 'Military' then 1 else 0 end) as Military,
    sum(case when outcome = 'Unknown' then 1 else 0 end) as "Unknown"
from 
    graduate_data.graduate_employment_data
group by 
    arizona_residency_descr, graduation_year, recipient_gender
order by 
    arizona_residency_descr, graduation_year, recipient_gender


-------------Percentage of corresponding outcome with respect to the arizona_residency_descr group -------------------------------------

with total_outcomes as (
    select 
        arizona_residency_descr,
        count(*) as total_outcomes
    from 
        graduate_data.graduate_employment_data
    group by 
        arizona_residency_descr
)
select 
    g.arizona_residency_descr,
    round(sum(case when g.outcome = 'Volunteering' then 1 else 0 end) * 100.0 / t.total_outcomes, 2) as Volunteering_Percentage,
    round(sum(case when g.outcome = 'Working' then 1 else 0 end) * 100.0 / t.total_outcomes, 2) as Working_Percentage,
    round(sum(case when g.outcome = 'Still Looking' then 1 else 0 end) * 100.0 / t.total_outcomes, 2) as Still_Looking_Percentage,
    round(sum(case when g.outcome = 'Continuing Education' then 1 else 0 end) * 100.0 / t.total_outcomes, 2) as Continuing_Education_Percentage,
    round(sum(case when g.outcome = 'Not Seeking' then 1 else 0 end) * 100.0 / t.total_outcomes, 2) as Not_Seeking_Percentage,
    round(sum(case when g.outcome = 'Military' then 1 else 0 end) * 100.0 / t.total_outcomes, 2) as Military_Percentage,
    round(sum(case when g.outcome = 'Unknown' then 1 else 0 end) * 100.0 / t.total_outcomes, 2) as Unknown_Percentage
from 
    graduate_data.graduate_employment_data g
join 
    total_outcomes t 
on 
    g.arizona_residency_descr = t.arizona_residency_descr
group by 
    g.arizona_residency_descr, t.total_outcomes
order by 
    g.arizona_residency_descr;



---------------------------------------------------------------------------------------------------------------------------------------------
----------Q3) Of the graduates whose outcome is working full-time, what is the avg starting salary by college over the two years? 
----------------------------(MAIN QUERY FOR QUESTION-3)--------------------------------------------------------------------------------------------
select
    recipient_primary_college as primary_college_name,
    coalesce(round(avg(case when graduation_year = 2021 then annual_salary end),2),0) as "2021",
    coalesce(round(avg(case when graduation_year = 2022 then annual_salary end),2),0) as "2022",
    coalesce(round(avg(case when graduation_year = 2023 then annual_salary end),2),0) as "2023"
from 
    graduate_data.graduate_employment_data
where 
    outcome = 'Working' and
	employment_type = 'Full-Time'
    and annual_salary is not null
group by 
    recipient_primary_college
order by 
    recipient_primary_college;


-----------------------------------------------------(MISCELLANEOUS ANALYSIS FOR QUESTION 3)---------------------------------------------------------------------
---Checking the Job Function and annual salary of all the years for Herberger Institute for Design and the Arts to justify high average salary in 2021---------

select
    job_function,
    graduation_year,
    count(*) as count_jobs,
    round(avg(annual_salary), 2) as average_salary
from
    graduate_data.graduate_employment_data
where
    recipient_primary_college = 'Herberger Institute for Design and the Arts'
    and outcome = 'Working'
    and employment_type = 'Full-Time'
    and annual_salary is not null
group by
    job_function, graduation_year
order by
    graduation_year, average_salary desc;

-----------Analyzing the categories making exceptionally high income and to view the employer_industry, employment_category and job_function ------

select employer_industry, employment_category, job_function, annual_salary
from graduate_data.graduate_employment_data
where outcome = 'Working' and employment_type = 'Full-Time'
and annual_salary is not null
order by annual_salary desc


-----------------------------Highest annual salary for all the years with respect to employment_category-----------------------
select employment_category, round(avg(annual_salary),3) as average_annual_salary
from graduate_data.graduate_employment_data
group by employment_category
order by average_annual_salary desc


















