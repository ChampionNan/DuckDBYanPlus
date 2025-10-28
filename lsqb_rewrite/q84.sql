create or replace TEMP view aggView6010171111193560176 as select CommentId as v3, COUNT(*) as annot, TagId as v8 from Comment_hasTag_Tag as cht2 group by CommentId,TagId;
create or replace TEMP view aggJoin8153658318501324156 as select ParentMessageId as v1, v8, annot from Comment_replyOf_Message_T as Comment_replyOf_Message_T, aggView6010171111193560176 where Comment_replyOf_Message_T.CommentId=aggView6010171111193560176.v3;
create or replace TEMP view aggView5005635730071721186 as select TagId as v2, COUNT(*) as annot from Comment_hasTag_Tag as cht1 group by TagId;
create or replace TEMP view aggJoin1489631561210996428 as select MessageId as v1, TagId as v2, annot from Message_hasTag_Tag_T as Message_hasTag_Tag_T, aggView5005635730071721186 where Message_hasTag_Tag_T.TagId=aggView5005635730071721186.v2;
create or replace TEMP view aggView4565531189619719203 as select v1, SUM(annot) as annot, v8 from aggJoin8153658318501324156 group by v1,v8;
create or replace TEMP view aggJoin6535428922108802568 as select v2, aggJoin1489631561210996428.annot * aggView4565531189619719203.annot as annot, v8 from aggJoin1489631561210996428 join aggView4565531189619719203 using(v1) where (v2 < v8);
select SUM(annot) as v9 from aggJoin6535428922108802568;
