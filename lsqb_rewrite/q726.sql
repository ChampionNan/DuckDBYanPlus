create or replace TEMP view aggView4124328066537519550 as select MessageId as v1, COUNT(*) as annot from Message_hasTag_Tag_T as Message_hasTag_Tag_T group by MessageId;
create or replace TEMP view aggJoin6706462716705752605 as select MessageId as v1, annot from Message_hasCreator_Person_T as Message_hasCreator_Person_T, aggView4124328066537519550 where Message_hasCreator_Person_T.MessageId=aggView4124328066537519550.v1;
create or replace TEMP view aggView377702788141675912 as select ParentMessageId as v1, COUNT(*) as annot from Comment_replyOf_Message_T as Comment_replyOf_Message_T group by ParentMessageId;
create or replace TEMP view aggJoin6357611887696307285 as select v1, aggJoin6706462716705752605.annot * aggView377702788141675912.annot as annot from aggJoin6706462716705752605 join aggView377702788141675912 using(v1);
create or replace TEMP view aggView7793419908801391609 as select v1, SUM(annot) as annot from aggJoin6357611887696307285 group by v1;
create or replace TEMP view aggJoin329712502191685599 as select annot from Person_likes_Message_T as Person_likes_Message_T, aggView7793419908801391609 where Person_likes_Message_T.MessageId=aggView7793419908801391609.v1;
select SUM(annot) as v9 from aggJoin329712502191685599;
