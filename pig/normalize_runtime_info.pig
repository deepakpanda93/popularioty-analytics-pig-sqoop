%default INPUTFILE 'mr_both_combined.txt'
%default OUTFOLDER '.'


runtime = LOAD '$INPUTFILE' as (src_type:chararray, src_id:chararray, dest_type:chararray, dest_id:chararray, wo:int, event:int, ne:int, discjs:int, discpolicy:int, discfilter:int);

/*Calculate all the maximium values for each count*/
B = GROUP runtime all;
max = FOREACH B GENERATE MAX(runtime.wo) as two,   MAX(runtime.event) as tevent, MAX(runtime.ne) as tne , MAX(runtime.discjs) as tdiscjs, MAX(runtime.discpolicy) as tdiscpolicy, MAX
(runtime.discfilter) as tdiscfilter, 1 as constant:int;


/*separate internal from webobjects*/
internal = filter runtime by src_type == 'service_object_stream';
webobjects = filter runtime by src_type == 'webobject';

/* In the future more filtering is needed here to remove services too...*/



/*split soid and stream propperly*/
i_so = FOREACH internal GENERATE flatten(STRSPLIT(src_id,'#!'))  as (src_id: chararray, src_stream:chararray) ,flatten(STRSPLIT(dest_id,'#!')) as (dest_id: chararray, dest_stream:chararray), wo as wo:int, event as event:int, ne as ne:int , discjs as discjs:int , discpolicy as discpolicy:int , discfilter as discfilter:int;

gr = GROUP i_so by (src_id, src_stream);

add_so =  FOREACH gr GENERATE FLATTEN(group), SUM($1.event) as tot_events:long,SUM($1.ne) as tot_ne:long, SUM($1.discjs) as tot_discjs:long, SUM($1.discpolicy) as tot_discpolicy:long, SUM($1.discfilter) as tot_discfilter:long,  COUNT($1) as count:long;

internal_so =  FOREACH add_so GENERATE src_id, src_stream, tot_events/count as event:long,  tot_ne/count as ne:long, tot_discjs/count as discjs:long, tot_discpolicy/count as discpolicy:long, tot_discfilter/count as discfilter:long, 1 as constant:int ;

webobject_so = FOREACH webobjects GENERATE flatten(STRSPLIT(dest_id,'#!')) as (dest_wo_id: chararray, dest_wo_stream:chararray), wo as wo_wo:int, 1 as constant:int;

/*Join with maximimum to achieve normalized data*/
f = JOIN max by constant, internal_so by constant; 
g = JOIN max by constant, webobject_so by constant; 


activity = FOREACH f GENERATE src_id as act_so_id: chararray, src_stream as act_stream: chararray, 
					( (10-(discjs*10/tdiscjs) is null? 10: 10-(discjs*10/tdiscjs) )+ 
				          (10-(discpolicy*10/tdiscpolicy)is null? 10: 10-(discpolicy*10/tdiscpolicy)) + 
					  (10-(discfilter*10/tdiscfilter) is null? 10: 10-(discfilter*10/tdiscfilter))
					  
					)/3 as activity:float;


h = JOIN g  by (dest_wo_id, dest_wo_stream) FULL, internal_so by (src_id, src_stream);

/* Normalize the data according to the SO producing the MAximium amount of updates per category, then web object updates, and events are weighted with twice the value assigned to non events...*/
popularity = FOREACH h GENERATE (src_id is null?dest_wo_id:src_id) as src_id , (src_stream is null? dest_wo_stream: src_stream) as src_stream, 
					(  
						((20*wo_wo/two) is null? 0: (20*wo_wo/two))  +  ((20*event/tevent) is null? 0: (20*event/tevent) )  +  ( (10*ne/tne) is null? 0: (10*ne/tne) )
					)/5 as popularity;


k = JOIN activity by (act_so_id,act_stream) FULL, popularity by (src_id,src_stream);
/*
 Here... it is ok to rely on the key src_id, and src_stream from now on in spite of the outer join, because if there is activity, there needs to be popularity!
service object data
*/
mix = FOREACH k GENERATE  CONCAT(CONCAT(src_id,'#!'),src_stream) as id, 'service_object_stream' as type,'runtime' as runtime,  (popularity is null?0:popularity) as popularity, (activity is null? 0: activity) as activity;
store mix INTO '$OUTFOLDER/stream_popularity_and_activity';


k_no_null = FOREACH k generate (activity is null? 0 : activity) as activity,src_id ,src_stream, (popularity is null? 0 : popularity) as popularity;

groupped_by_so_id = GROUP k_no_null BY src_id;


so_total =  FOREACH groupped_by_so_id GENERATE group, 'service_object' as type, 'runtime' as runtime, SUM(k_no_null.popularity)/COUNT(k_no_null) as tot_popularity:long,SUM(k_no_null.activity)/COUNT(k_no_null) as tot_activity:long;
store so_total INTO '$OUTFOLDER/so_popularity_and_activity';



/*

 TODO  : JOIN with some user import from the idm Database and generate entries to increase reputation of users based on their SOs...

*/

/*totals = FOREACH B GENERATE SUM(internal_so.wo) as two,   SUM(internal_so.event) as tevent, SUM(internal_so.ne) as tne , SUM(internal_so.discjs) as tdiscjs, SUM(internal_so.discpolicy) as tdiscpolicy, SUM(internal_so.discfilter) as tdiscfilter, 1 as constant:int;

max = FOREACH B GENERATE MAX(internal_so.wo) as two,   MAX(internal_so.event) as tevent, MAX(internal_so.ne) as tne , MAX(internal_so.discjs) as tdiscjs, MAX(internal_so.discpolicy) as tdiscpolicy, MAX(internal_so.discfilter) as tdiscfilter, 1 as constant:int;

f = JOIN totals by constant, max by constant, internal_so by constant; 
f = JOIN max by constant, internal_so by constant using 'skewed'; 
f = JOIN totals by constant, max by constant, internal_so by constant; */

