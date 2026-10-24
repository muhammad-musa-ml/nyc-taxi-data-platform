-- Is the replay difference floating-point order? Aggregate the same month twice, once with parallel workers
-- and once without, and once over the rows in reverse physical order, then compare the results.
create temp table a as
  select pickup_date, sum(total_amount) revenue, avg(avg_speed_mph) speed, avg(tip_pct) tip
  from marts.fct_trips where source_month = '2025-03-01' group by 1;
set max_parallel_workers_per_gather = 0;
create temp table b as
  select pickup_date, sum(total_amount) revenue, avg(avg_speed_mph) speed, avg(tip_pct) tip
  from marts.fct_trips where source_month = '2025-03-01' group by 1;
create temp table c as
  select pickup_date, sum(total_amount) revenue, avg(avg_speed_mph) speed, avg(tip_pct) tip
  from (select * from marts.fct_trips where source_month = '2025-03-01' order by source_row desc) t group by 1;
select 'parallel vs serial' as comparison, count(*) days,
       count(*) filter (where a.revenue <> b.revenue or a.speed <> b.speed or a.tip <> b.tip) days_differing,
       max(abs(a.revenue - b.revenue) / a.revenue) max_rel_revenue,
       max(abs(a.speed - b.speed) / a.speed) max_rel_speed
from a join b using (pickup_date)
union all
select 'serial vs reverse row order', count(*),
       count(*) filter (where c.revenue <> b.revenue or c.speed <> b.speed or c.tip <> b.tip),
       max(abs(c.revenue - b.revenue) / b.revenue), max(abs(c.speed - b.speed) / b.speed)
from c join b using (pickup_date);
