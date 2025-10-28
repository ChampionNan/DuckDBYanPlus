create or replace TEMP view aggView7554140104403187908 as select Person2Id as v2, COUNT(*) as annot, Person1Id as v1 from Person_knows_Person as pkp1 group by Person2Id,Person1Id;
create or replace TEMP view aggJoin388811357266822316 as select Person2Id as v4, v1, annot from Person_knows_Person as pkp2, aggView7554140104403187908 where pkp2.Person1Id=aggView7554140104403187908.v2 and v1<Person2Id;
create or replace TEMP view aggView4465768378416061097 as select PersonId as v4, COUNT(*) as annot from Person_hasInterest_Tag as Person_hasInterest_Tag group by PersonId;
create or replace TEMP view aggJoin5522843423450799075 as select v4, v1, aggJoin388811357266822316.annot * aggView4465768378416061097.annot as annot from aggJoin388811357266822316 join aggView4465768378416061097 using(v4);
create or replace TEMP view aggView6456561082973136451 as select Person1Id as v4, COUNT(*) as annot from Person_knows_Person as pkp3 group by Person1Id;
create or replace TEMP view aggJoin4953168813008835323 as select v4, v1, aggJoin5522843423450799075.annot * aggView6456561082973136451.annot as annot from aggJoin5522843423450799075 join aggView6456561082973136451 using(v4);
select SUM(annot) as v9 from aggJoin4953168813008835323;
