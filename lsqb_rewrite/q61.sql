create or replace TEMP view aggView6174847943139349857 as select Person2Id as v2, COUNT(*) as annot, Person1Id as v1 from Person_knows_Person as pkp1 group by Person2Id,Person1Id;
create or replace TEMP view aggJoin6761181161326564158 as select Person2Id as v5, v1, annot from Person_knows_Person as pkp2, aggView6174847943139349857 where pkp2.Person1Id=aggView6174847943139349857.v2 and v1<Person2Id;
create or replace TEMP view aggView7549552288493753889 as select PersonId as v5, COUNT(*) as annot from Person_hasInterest_Tag as Person_hasInterest_Tag group by PersonId;
create or replace TEMP view aggJoin1855580375695696630 as select v5, v1, aggJoin6761181161326564158.annot * aggView7549552288493753889.annot as annot from aggJoin6761181161326564158 join aggView7549552288493753889 using(v5);
select SUM(annot) as v7 from aggJoin1855580375695696630;
