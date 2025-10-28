create or replace TEMP view aggView3264539425848991175 as select MessageId as v1, COUNT(*) as annot, TagId as v2 from Message_hasTag_Tag_T as Message_hasTag_Tag_T group by MessageId,TagId;
create or replace TEMP view aggJoin3561446869697631731 as select CommentId as v3, v2, annot from Comment_replyOf_Message_T as Comment_replyOf_Message_T, aggView3264539425848991175 where Comment_replyOf_Message_T.ParentMessageId=aggView3264539425848991175.v1;
create or replace TEMP view aggView4339944571411841911 as select CommentId as v3, COUNT(*) as annot, TagId as v6 from Comment_hasTag_Tag as cht group by CommentId,TagId;
create or replace TEMP view aggJoin5451644001104042621 as select v2, aggJoin3561446869697631731.annot * aggView4339944571411841911.annot as annot, v6 from aggJoin3561446869697631731 join aggView4339944571411841911 using(v3) where (v2 < v6);
select SUM(annot) as v7 from aggJoin5451644001104042621;
