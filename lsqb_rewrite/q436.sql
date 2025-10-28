create or replace TEMP view aggView2957534732491998764 as select MessageId as v1, COUNT(*) as annot from Message_hasTag_Tag_T as Message_hasTag_Tag_T group by MessageId;
create or replace TEMP view aggJoin5806906657996276515 as select MessageId as v1, annot from Message_hasCreator_Person_T as Message_hasCreator_Person_T, aggView2957534732491998764 where Message_hasCreator_Person_T.MessageId=aggView2957534732491998764.v1;
create or replace TEMP view aggView1387876897694725025 as select ParentMessageId as v1, COUNT(*) as annot from Comment_replyOf_Message_T as Comment_replyOf_Message_T group by ParentMessageId;
create or replace TEMP view aggJoin3677837428075735390 as select v1, aggJoin5806906657996276515.annot * aggView1387876897694725025.annot as annot from aggJoin5806906657996276515 join aggView1387876897694725025 using(v1);
create or replace TEMP view aggView6454859820467758548 as select v1, SUM(annot) as annot from aggJoin3677837428075735390 group by v1;
create or replace TEMP view aggJoin2389693104784416458 as select annot from Person_likes_Message_T as Person_likes_Message_T, aggView6454859820467758548 where Person_likes_Message_T.MessageId=aggView6454859820467758548.v1;
select SUM(annot) as v9 from aggJoin2389693104784416458;
