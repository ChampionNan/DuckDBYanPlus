create or replace TEMP view aggView3192924842814039435 as select CommentId as v3, COUNT(*) as annot, TagId as v8 from Comment_hasTag_Tag as cht2 group by CommentId,TagId;
create or replace TEMP view aggJoin553500597113591165 as select ParentMessageId as v1, v8, annot from Comment_replyOf_Message_T as Comment_replyOf_Message_T, aggView3192924842814039435 where Comment_replyOf_Message_T.CommentId=aggView3192924842814039435.v3;
create or replace TEMP view aggView1154368546987504810 as select v1, SUM(annot) as annot, v8 from aggJoin553500597113591165 group by v1,v8;
create or replace TEMP view aggJoin7366817943848827175 as select TagId as v2, v8, annot from Message_hasTag_Tag_T as Message_hasTag_Tag_T, aggView1154368546987504810 where Message_hasTag_Tag_T.MessageId=aggView1154368546987504810.v1 and TagId<v8;
create or replace TEMP view aggView5213158301950306349 as select v2, SUM(annot) as annot from aggJoin7366817943848827175 group by v2;
create or replace TEMP view aggJoin8741513210454917654 as select TagId as v2, annot from Comment_hasTag_Tag as cht1, aggView5213158301950306349 where cht1.TagId=aggView5213158301950306349.v2;
select SUM(annot) as v9 from aggJoin8741513210454917654;
