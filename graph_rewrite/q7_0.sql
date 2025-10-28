create or replace TEMP view semiJoinView2257342756492553520 as select src as v2, dst as v4 from Graph AS g2 where (src) in (select (dst) from Graph AS g1);
create or replace TEMP view semiJoinView3758061375649775533 as select v2, v4 from semiJoinView2257342756492553520 where (v4) in (select (src) from Graph AS g5);
create or replace TEMP view semiJoinView4451165522232975171 as select v2, v4 from semiJoinView3758061375649775533 where (v2) in (select (dst) from Graph AS g4);
create or replace TEMP view semiJoinView3502425281332267320 as select distinct v2, v4 from semiJoinView4451165522232975171 where (v4) in (select (src) from Graph AS g3);
select distinct v2, v4 from semiJoinView3502425281332267320;
