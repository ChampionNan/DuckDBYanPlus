create or replace TEMP view semiJoinView2787879662804989217 as select src as v2, dst as v4 from Graph AS g2 where (src) in (select (dst) from Graph AS g1);
create or replace TEMP view semiJoinView9135620920946566068 as select distinct src as v4, dst as v6 from Graph AS g3 where (src) in (select (v4) from semiJoinView2787879662804989217);
select distinct v4, v6 from semiJoinView9135620920946566068;
