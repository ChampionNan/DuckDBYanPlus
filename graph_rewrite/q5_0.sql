create or replace TEMP view bag177 as select g1.dst as v2, g2.dst as v4, g1.src as v6 from Graph as g1, Graph as g2, Graph as g3 where g1.dst=g2.src and g2.dst=g3.src and g3.dst=g1.src;
create or replace TEMP view aggView6870778310610267346 as select v2, COUNT(*) as annot from bag177 group by v2;
create or replace TEMP view aggJoin2564388104882511237 as select dst as v12, annot from Graph as g7, aggView6870778310610267346 where g7.src=aggView6870778310610267346.v2;
create or replace TEMP view bag178 as select g5.dst as v10, g4.src as v12, g4.dst as v8 from Graph as g4, Graph as g5, Graph as g6 where g4.dst=g5.src and g5.dst=g6.src and g6.dst=g4.src;
create or replace TEMP view aggView1077130602336209553 as select v12, COUNT(*) as annot from bag178 group by v12;
create or replace TEMP view aggJoin4993763997025197749 as select aggJoin2564388104882511237.annot * aggView1077130602336209553.annot as annot from aggJoin2564388104882511237 join aggView1077130602336209553 using(v12);
select SUM(annot) as v15 from aggJoin4993763997025197749;
