create or replace TEMP view aggView727706616926588788 as select CommentId as v3, COUNT(*) as annot, TagId as v6 from Comment_hasTag_Tag as cht group by CommentId,TagId;
create or replace TEMP view aggJoin6467524601263708132 as select ParentMessageId as v1, v6, annot from Comment_replyOf_Message_T as Comment_replyOf_Message_T, aggView727706616926588788 where Comment_replyOf_Message_T.CommentId=aggView727706616926588788.v3;
create or replace TEMP view aggView1406086673979917694 as select v1, SUM(annot) as annot, v6 from aggJoin6467524601263708132 group by v1,v6;
create or replace TEMP view aggJoin1004224167391312134 as select TagId as v2, v6, annot from Message_hasTag_Tag_T as Message_hasTag_Tag_T, aggView1406086673979917694 where Message_hasTag_Tag_T.MessageId=aggView1406086673979917694.v1 and TagId<v6;
select SUM(annot) as v7 from aggJoin1004224167391312134;
