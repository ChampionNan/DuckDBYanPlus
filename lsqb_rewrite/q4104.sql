create or replace TEMP view aggView797991541452723093 as select MessageId as v1, COUNT(*) as annot from Message_hasTag_Tag_T as Message_hasTag_Tag_T group by MessageId;
create or replace TEMP view aggJoin1701560560363822366 as select MessageId as v1, annot from Message_hasCreator_Person_T as Message_hasCreator_Person_T, aggView797991541452723093 where Message_hasCreator_Person_T.MessageId=aggView797991541452723093.v1;
create or replace TEMP view aggView6049985244318633081 as select ParentMessageId as v1, COUNT(*) as annot from Comment_replyOf_Message_T as Comment_replyOf_Message_T group by ParentMessageId;
create or replace TEMP view aggJoin7976867136229083624 as select v1, aggJoin1701560560363822366.annot * aggView6049985244318633081.annot as annot from aggJoin1701560560363822366 join aggView6049985244318633081 using(v1);
create or replace TEMP view aggView4014079712582921723 as select v1, SUM(annot) as annot from aggJoin7976867136229083624 group by v1;
create or replace TEMP view aggJoin8114286969253405097 as select annot from Person_likes_Message_T as Person_likes_Message_T, aggView4014079712582921723 where Person_likes_Message_T.MessageId=aggView4014079712582921723.v1;
select SUM(annot) as v9 from aggJoin8114286969253405097;
