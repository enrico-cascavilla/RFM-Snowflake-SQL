create or replace view database_two.ds.rfm_view_snowflake
as
//create a calendar date
with cals as
 (select dateadd(day, seq4(), dateadd('month', -3, add_months(current_date, 1))) as snapshot_date
            from table(generator(rowcount=> 90))) //select * from cals

//taking the customer for those calendar snapshot
, one_year as
(select client.*, snapshot_date
            from cals
left join (select distinct c.customer_id as customer_id
           from database_one.schema_1.table_one c
            where c.country = 'Specific country')

//take the feature to create the R-F-M score for the customer in those snapshot
, temporary_rfm as
(select
      snap.customer_id,
      snap.snapshot_date,
      min(bi.date_transaction) as first_succeeded_transaction_date,
      max(bi.date_transaction) as last_succeeded_transaction_date,

      datediff(day, last_succeeded_transaction_date, snapshot_date) as days_since,

      count(case
                when (bi.date_transaction >= add_months(snapshot_date,-3) and (bi.date_transaction < snapshot_date))
                    then bi.tr_id
                else null
            end) as tx_count_3m,

       sum(case
                when bi.date_transaction >= add_months(snapshot_date, -3) and (bi.date_transaction < snapshot_date)
                    then bi.revenue
                else 0
            end) as revenue_sum_3m,

       ntile(5) over(partition by snapshot_date order by days_since desc) as recency_ntile_3m,
       ntile(5) over(partition by snapshot_date order by tx_count_3m) as tx_count_ntile_3m,
       ntile(5) over(partition by snapshot_date order by revenue_sum_3m) as revenue_sum_ntile_3m,

       to_varchar(recency_ntile_3m)||to_varchar(tx_count_ntile_3m)||to_varchar(revenue_sum_ntile_3m) as final_score

    from database_three.schema_three.table_three bi
    inner join one_year snap
    on bi.customer_id = snap.customer_id
    group by 1, 2, 3 having last_succeeded_transaction_date >= Dateadd(Month, -6, snap.snapshot_date))

//have the 125 combination of the scores for the 10 segment in a separete mapping table
, maps as
  (select
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
    from temporary_rfm s
    left join mapping_segment map
    on s.final_score = map.score)

//select the last date of the daily calculation
, today_rfm as
 (select *
  from maps
  where snapshot_date = current_date)

//keep just the change of segments
, only_change as
 (select customer_id,
         snapshot_date,
         segment,
         pre_seg_1
  from maps
  where (segment!=pre_seg_1)
  order by customer_id, snapshot_date desc)

//save just the last change to display to have actual segment and previous segment (optionally you can keep both it depend how much space you can occupy)
, last_change as
 (select customer_id,
         snapshot_date as date_ofchange,
         pre_seg_1
         //row_number() over(partition by customer_id order by snapshot_date desc) as last_snap
  from only_change
  qualify row_number() over(partition by customer_id order by snapshot_date desc) = 1)

//select all the features necessary to take an action today with the others department
, tab as
(select distinct t.customer_id,
        t.snapshot_date,
        t.days_since as r_raw,
        t.recency_ntile_3m as r_score,
        t.tx_count_3m as f_raw,
        t.tx_count_ntile_3m as f_score,
        t.revenue_sum_3m as m_raw,
        t.revenue_sum_ntile_3m as m_score,
        t.final_score,
        t.segment,
        t.number_segment,
        datediff('day', t.first_succeeded_transaction_date, t.snapshot_date) as days_active,
        t.last_succeeded_transaction_date,
        l.pre_seg_1,
        l.date_ofchange,
        c.extra_features //after getting the score can cross with other features you might find interesting
 from today_rfm t
 left join last_change l
 on t.customer_id = l.customer_id
 left join database_four.schema_four.table_four c
 on t.customer_id = c.customer_id)

 select *
 from tab
