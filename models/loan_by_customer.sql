{{ config(materialized='table') }}

with client_account_relations as (
    SELECT * 
    from {{ source('dbt_retail_banking', 'client_account_relations') }} 
)
, loan_master as (
    SELECT * 
    from gebu-data-ml-day0-01-333910.dbt_retail_banking.loan_master 
)
, district_master as (
    SELECT * 
    from `gebu-data-ml-day0-01-333910.dbt_retail_banking_trans.district_master`
)
, account_master as (
    SELECT * 
    from {{ source('dbt_retail_banking', 'account_master') }}
)
, client_information as (
    SELECT * 
    from {{ source('dbt_retail_banking', 'client_information') }}
)
select ci.client_id as client_id,ci.first||' '||ci.middle||' '||ci.last as client_name,ci.sex as client_sex,
ci.age, ci.email, ci.phone, 
dm.city, dm.state_name,dm.state_abbrev,dm.region,dm.division,
lm.loan_id, lm.amount, lm.duration, 
{{ var('interest_rate') }} as interest_rate,
{{ interest_amount('lm.amount','lm.duration') }} as interest,
lm.purpose, lm.status

from client_account_relations ca 
join client_information ci on (ca.client_id = ci.client_id)
join loan_master lm on (lm.account_id=ca.account_id)
join account_master am on (am.account_id=ca.account_id)
join {{ ref('district_master') }} dm on (dm.district_id=am.district_id)
