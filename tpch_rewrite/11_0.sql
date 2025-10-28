create or replace TEMP view aggView3831231169004133284 as select n_nationkey as v9 from nation as nation where (n_name = 'GERMANY');
create or replace TEMP view aggJoin5893632559309347731 as select s_suppkey as v2 from supplier as supplier, aggView3831231169004133284 where supplier.s_nationkey=aggView3831231169004133284.v9;
create or replace TEMP view aggView1098737882852909146 as select v2, COUNT(*) as annot from aggJoin5893632559309347731 group by v2;
create or replace TEMP view aggJoin2693237594835423724 as select ps_partkey as v1, ps_availqty as v3, ps_supplycost as v4, annot from partsupp as partsupp, aggView1098737882852909146 where partsupp.ps_suppkey=aggView1098737882852909146.v2;
create or replace TEMP view aggView350760057862656883 as select v1, SUM((v4 * v3) * annot) as v18 from aggJoin2693237594835423724 group by v1;
select v1,SUM(v18) as v18 from aggView350760057862656883 group by v1;
