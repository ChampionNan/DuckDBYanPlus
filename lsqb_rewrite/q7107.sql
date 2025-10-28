create or replace TEMP view aggView7997022948866190413 as select MessageId as v1, COUNT(*) as annot from Message_hasTag_Tag_T as Message_hasTag_Tag_T group by MessageId;
create or replace TEMP view aggJoin2446417268738930778 as select MessageId as v1, annot from Message_hasCreator_Person_T as Message_hasCreator_Person_T, aggView7997022948866190413 where Message_hasCreator_Person_T.MessageId=aggView7997022948866190413.v1;
create or replace TEMP view aggView2026882477539774289 as select ParentMessageId as v1, COUNT(*) as annot from Comment_replyOf_Message_T as Comment_replyOf_Message_T group by ParentMessageId;
create or replace TEMP view aggJoin5569760619115629183 as select v1, aggJoin2446417268738930778.annot * aggView2026882477539774289.annot as annot from aggJoin2446417268738930778 join aggView2026882477539774289 using(v1);
create or replace TEMP view aggView1548663685970432496 as select v1, SUM(annot) as annot from aggJoin5569760619115629183 group by v1;
create or replace TEMP view aggJoin8480351051837410827 as select annot from Person_likes_Message_T as Person_likes_Message_T, aggView1548663685970432496 where Person_likes_Message_T.MessageId=aggView1548663685970432496.v1;
select SUM(annot) as v9 from aggJoin8480351051837410827;
