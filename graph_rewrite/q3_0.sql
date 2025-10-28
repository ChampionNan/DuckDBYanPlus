create or replace TEMP view semiJoinView7319264090273833601 as select src as v2, dst as v4 from Graph AS g2 where (dst) in (select (src) from Graph AS g3);
create or replace TEMP view semiJoinView4844140302667565429 as select src as v1, dst as v2 from Graph AS g1 where (dst) in (select (v2) from semiJoinView7319264090273833601);
create or replace TEMP view g1Aux32 as select v1 from semiJoinView4844140302667565429;
select distinct v1 from g1Aux32;
