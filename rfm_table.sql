----------------------------------------------------------------------------------------------------------------------------------------------------------
-- Snowsql code to create the RFM score for customer of one country
-- It calculated the RFM score daily and then it keep just the changes of the segment
-- I use many temporary table so the system is not overload and at the end I might consider to create pro
----------------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------------
//YEAR SNAPSHOT: create a calendar and filling it up with the customer
----------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace temporary table calendar as
(select dateadd(day, seq4(), dateadd('year', -1, current_date)) as snapshot_date
            from table(generator(rowcount=> 365)));
create or replace temporary table one_year_daily as
(select client.*, snapshot_date
            from calendar
left join (select distinct c.customer_id as customer_id
           from database_one.schema_one.table_one c
            where c.country = 'Country I Want') client); //filter country

----------------------------------------------------------------------------------------------------------------------------------------------------------
//RFM
----------------------------------------------------------------------------------------------------------------------------------------------------------

create or replace temporary table rfm_temporary
 as
 with sc as
 (  select
     snap.customer_id,
     snap.snapshot_date,
     min(ma.transaction_date) as first_succeeded_transaction_date,
     max(ma.transaction_date) as last_succeeded_transaction_date,
  //Raw R
     datediff(day, last_succeeded_transaction_date, snapshot_date) as days_since,
  //Raw F
     count(case
               when ma.transaction_date >= add_months(snapshot_date,-3) and (ma.transaction_date < snapshot_date)
                   then ma.tranx_id
               else null
           end) as tx_count_3m,
  //Raw M
      sum(case
               when ma.transaction_date >= add_months(snapshot_date, -3) and (ma.transaction_date < snapshot_date)
                   then ma.revenue
               else 0
           end) as revenue_sum_3m,

  //RFM score with quintile
      ntile(5) over(partition by snapshot_date order by days_since desc) as recency_ntile_3m,
      ntile(5) over(partition by snapshot_date order by tx_count_3m) as tx_count_ntile_3m,
      ntile(5) over(partition by snapshot_date order by revenue_sum_3m) as revenue_sum_ntile_3m,

  //Final Score
       to_varchar(recency_ntile_3m)||to_varchar(tx_count_ntile_3m)||to_varchar(revenue_sum_ntile_3m) as final_score

   from database_one.schema_four.table_four ma
   inner join one_year_daily snap
   on ma.customer_id = snap.customer_id and ma.transaction_date < snap.snapshot_date
   group by 1, 2 having last_succeeded_transaction_date >= Dateadd(Month, -6, snap.snapshot_date)
 )
   select
     s.customer_id,
     s.snapshot_date,
     s.first_succeeded_transaction_date,
     s.last_succeeded_transaction_date,
     s.days_since, //raw r
     s.tx_count_3m, //raw f
     s.revenue_sum_3m, //raw m
     s.recency_ntile_3m, //r quantile
     s.tx_count_ntile_3m, //quantile f
     s.revenue_sum_ntile_3m, //quantile m
     s.final_score,
     map.segment,
     map.number_segment,
     lead(segment) over (partition by s.customer_id order by s.snapshot_date desc) as pre_seg_1,
     lead(number_segment) over (partition by s.customer_id order by s.snapshot_date desc) as pre_seg_number_1
   from sc s
   left join mapping_segment map
   on s.final_score = map.score

   create or replace temporary table final_rfm  cluster by (customer_id, snapshot_date)
   as
   with rfm as
    (select * from rfm_temporary
     where segment!=pre_seg_1
     order by customer_id, snapshot_date desc)
     select customer_id,
               snapshot_date,
               days_since as r_raw,
               recency_ntile_3m as r_score,
               tx_count_3m as f_raw,
               tx_count_ntile_3m as f_score,
               revenue_sum_3m as m_raw,
               revenue_sum_ntile_3m as m_score,
               final_score,
               segment,
               number_segment,
               pre_seg_1,
               pre_seg_number_1,
               ifnull(lead(rfm.snapshot_date) over (partition by rfm.customer_id order by rfm.snapshot_date) - snapshot_date, current_date - snapshot_date) as time_as,
               first_succeeded_transaction_date
      from rfm
