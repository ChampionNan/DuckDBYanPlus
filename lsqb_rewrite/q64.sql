create or replace TEMP view aggView2026432801047187078 as select Person2Id as v2, COUNT(*) as annot, Person1Id as v1 from Person_knows_Person as pkp1 group by Person2Id,Person1Id;
create or replace TEMP view aggJoin9096448129985140708 as select Person2Id as v5, v1, annot from Person_knows_Person as pkp2, aggView2026432801047187078 where pkp2.Person1Id=aggView2026432801047187078.v2 and v1<Person2Id;
create or replace TEMP view aggView3557373720003173972 as select PersonId as v5, COUNT(*) as annot from Person_hasInterest_Tag as Person_hasInterest_Tag group by PersonId;
create or replace TEMP view aggJoin3352596409725695188 as select v5, v1, aggJoin9096448129985140708.annot * aggView3557373720003173972.annot as annot from aggJoin9096448129985140708 join aggView3557373720003173972 using(v5);
select SUM(annot) as v7 from aggJoin3352596409725695188;
