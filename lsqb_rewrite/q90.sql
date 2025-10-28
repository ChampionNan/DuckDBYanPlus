create or replace TEMP view aggView3488884067431343759 as select Person2Id as v2, COUNT(*) as annot, Person1Id as v1 from Person_knows_Person as pkp1 group by Person2Id,Person1Id;
create or replace TEMP view aggJoin1905697325327643117 as select Person2Id as v4, v1, annot from Person_knows_Person as pkp2, aggView3488884067431343759 where pkp2.Person1Id=aggView3488884067431343759.v2 and v1<Person2Id;
create or replace TEMP view aggView2987208514625319895 as select PersonId as v4, COUNT(*) as annot from Person_hasInterest_Tag as Person_hasInterest_Tag group by PersonId;
create or replace TEMP view aggJoin1649183196640324079 as select v4, v1, aggJoin1905697325327643117.annot * aggView2987208514625319895.annot as annot from aggJoin1905697325327643117 join aggView2987208514625319895 using(v4);
create or replace TEMP view aggView4437919099495099470 as select Person1Id as v4, COUNT(*) as annot from Person_knows_Person as pkp3 group by Person1Id;
create or replace TEMP view aggJoin9169905460749924220 as select v4, v1, aggJoin1649183196640324079.annot * aggView4437919099495099470.annot as annot from aggJoin1649183196640324079 join aggView4437919099495099470 using(v4);
select SUM(annot) as v9 from aggJoin9169905460749924220;
