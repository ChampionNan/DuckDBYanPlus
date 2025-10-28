create or replace TEMP view aggView7425934403353747572 as select dst as v2 from Graph as g1;
create or replace TEMP view aggJoin147843436043585945 as select dst as v4 from Graph as g2, aggView7425934403353747572 where g2.src=aggView7425934403353747572.v2;
create or replace TEMP view aggView6804602141933539119 as select src as v4 from Graph as g3;
create or replace TEMP view aggJoin3996526219101249500 as select * from aggJoin147843436043585945 join aggView6804602141933539119 using(v4);
select COUNT(*) as v7 from aggJoin3996526219101249500;
