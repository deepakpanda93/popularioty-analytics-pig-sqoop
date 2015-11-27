%default INPUTFILE '/user/hadoop/raw_data/output_mr_runtime/part-r-00000'
%default OUTFOLDER '.'


/* TODO 

	

	Generate one unified file for all of them source type popularity/activity ... check feedback MR job for formatting.

*/
runtime = LOAD '$INPUTFILE' as (src_type:chararray, src_id:chararray, dest_type:chararray, dest_id:chararray, 
wo:int, event:int, ne:int, discjs:int,  
discpolicy:int, discfilter:int, 
servicepop:int, serviceactok:int, 
serviceactwrong:int, so_subsc: int);

B = GROUP runtime all;

/* get the max and sums of all values for every row in the data */
max = FOREACH B GENERATE 
MAX(runtime.wo) as two, MAX(runtime.event) as tevent,  MAX(runtime.ne) as tne ,MAX(runtime.discjs) as tdiscjs, 
MAX(runtime.discpolicy) as tdiscpolicy, MAX (runtime.discfilter) as tdiscfilter,
MAX (runtime.servicepop) as tservicepop, MAX (runtime.serviceactok) as tserviceactok, 
MAX (runtime.serviceactwrong) as tserviceactwrong, MAX (runtime.so_subsc) as tso_subsck, 
 1 as constant:int;

/*sum = FOREACH B GENERATE
SUM(runtime.wo) as swo, SUM(runtime.event) as sevent,  SUM(runtime.ne) as sne ,SUM(runtime.discjs) as sdiscjs, 
SUM(runtime.discpolicy) as sdiscpolicy, SUM (runtime.discfilter) as sdiscfilter,
SUM (runtime.servicepop) as sservicepop, SUM (runtime.serviceactok) as sserviceactok, 
SUM (runtime.serviceactwrong) as sserviceactwrong, SUM (runtime.so_subsc) as sso_subsck, 1 as constant:int;*/

/*separate internal so communications from services and from webobjects*/

/* ================================================================================ */
/* ================================================================================ */
/* ================================================================================ */
/* 				APPLICATIONS PART				  	    */
/* ================================================================================ */
/* ================================================================================ */
/* ================================================================================ */
/* ================================================================================ */


services_io = filter runtime by src_type == 'service_instance' or  dest_type == 'service_instance';
services_out_gr = GROUP services_io  by (src_type,src_id);
services_in_gr = GROUP services_io  by (dest_type,dest_id);

sum_services_in = FOREACH services_in_gr GENERATE  FLATTEN(group) as (dest_type,dest_id),
SUM($1.servicepop) as servicepop_in, 1 as constant:int;

sum_services_out = FOREACH services_out_gr GENERATE  FLATTEN(group) as  (src_type,src_id),
SUM($1.servicepop) as servicepop_out, 1 as constant:int;

service_io = JOIN sum_services_in by (dest_id, dest_type)  FULL, sum_services_out by (src_id, src_type);
service_all = FOREACH service_io GENERATE 
	(dest_id is null ? src_id : dest_id) as service_id,
	(dest_type is null? src_type : dest_type) as type, 
	(servicepop_out is null? 0: servicepop_out)+ (servicepop_in is null? 0: servicepop_in) as messages, 
	1 as constant:int;

/*get rid of unknowns finally. If we did it before, we would have lost info*/

service_messages_count= filter service_all by type != 'unknown';

group_services = GROUP service_messages_count all;

max_services = FOREACH group_services GENERATE 
MAX(service_messages_count.messages) as maxmessages, 1 as constant:int;

services_count_max = JOIN service_messages_count by (constant), max_services by (constant);
services_popularity = FOREACH services_count_max GENERATE service_id, 'service_instance' as type, 'popularity' as reptype,  (1 + (messages*9/maxmessages)) as rating, messages as count;

store services_popularity INTO '$OUTFOLDER/service_popularity';

service_io_act_filter = FILTER services_io by serviceactwrong != 0 or serviceactok != 0;

service_activity = FOREACH service_io_act_filter GENERATE src_id as service_id, 'service_instance' as type, 'activity' as reptype,serviceactok as ok, serviceactwrong as wrong;

store service_activity INTO '$OUTFOLDER/service_activity';

/* activity is in service_activity */


/* ================================================================================ */
/* ================================================================================ */
/* ================================================================================ */
/* 				SERVIOTICY PART				  	    */
/* ================================================================================ */
/* ================================================================================ */
/* ================================================================================ */
/* ================================================================================ */
/* 	PROCESS PIPELINES 			*/


sos = filter runtime by src_type == 'service_object_stream';
webobjects = filter runtime by src_type == 'webobject';
internal = filter sos by dest_type == 'service_object_stream';


/*split soid and stream propperly*/

i_so = FOREACH internal GENERATE 
	flatten(STRSPLIT(src_id,'#!'))  as (src_id: chararray, src_stream:chararray) ,
	flatten(STRSPLIT(dest_id,'#!')) as (dest_id: chararray, dest_stream:chararray), 
	wo as wo:int, event as event:int, ne as ne:int , discjs as discjs:int , 
	discpolicy as discpolicy:int , discfilter as discfilter:int;


/* generate sum and count different streams added for compositions i.e. internal*/

gr = GROUP i_so by (src_id, src_stream);


add_so =  FOREACH gr GENERATE FLATTEN(group) as (src_id, src_stream), 
	SUM($1.event) as tot_events:long,SUM($1.ne) as tot_ne:long, SUM($1.discjs) as tot_discjs:long,
	SUM($1.discpolicy) as tot_discpolicy:long, SUM($1.discfilter) as tot_discfilter:long,  COUNT($1) as count:long,1 as constant:int ;

activity_so = FOREACH add_so GENERATE CONCAT(CONCAT(src_id, '#!'),src_stream) as stream_oid, 'service_object_stream' as type, 'activity' as reptype, 
	 tot_events as tot_events:long  , tot_ne as tot_ne:long,  tot_discjs as tot_discjs:long,
	tot_discpolicy as tot_discpolicy:long,  tot_discfilter as tot_discfilter:long;


store activity_so INTO '$OUTFOLDER/streams_activity';

/* add_so is activity for pipelines*/ 



/*internal_so =  FOREACH add_so GENERATE src_id, src_stream, tot_events/count as event:long,  tot_ne/count as ne:long, tot_discjs/count as discjs:long, tot_discpolicy/count as discpolicy:long, tot_discfilter/count as discfilter:long, 1 as constant:int ;

f = JOIN max by constant, internal_so by constant/*, sum by constant*/;  */

/* ================================================================================ */
/* 	PROCESS WEBOBJECTS 			*/

webobject_so = FOREACH webobjects GENERATE flatten(STRSPLIT(dest_id,'#!')) as (dest_wo_id: chararray, dest_wo_stream:chararray), wo as wo_wo:int, 1 as constant:int;



/* ================================================================================ */
/*		PROCESS subscriptions i.e. destination user			*/

subscriptions  = filter sos by dest_type != 'service_object_stream';

sub_so = FOREACH subscriptions GENERATE 
	flatten(STRSPLIT(src_id,'#!'))  as (src_id: chararray, src_stream:chararray) ,
	 dest_type as dest_type :chararray, dest_id as dest_id:chararray, 
	so_subsc as so_subsc:int;

subs_gr = GROUP sub_so by (src_id, src_stream);

subscriptions_so =  FOREACH subs_gr GENERATE FLATTEN(group), 
	SUM($1.so_subsc) as tot_so_subc:long,  COUNT($1) as count:long;

/* ================================================================================ */
/* ================================================================================ */
/*		 MERGE SOs FOR POPULARITY			*/


subs_wo = JOIN subscriptions_so by (src_id,src_stream) FULL, webobject_so by (dest_wo_id,dest_wo_stream);
sub_wo_uni = FOREACH subs_wo GENERATE (src_id is null? dest_wo_id: src_id) as src_id_t:chararray, 
(src_stream is null? dest_wo_stream: src_stream) as src_stream_t:chararray,  
(wo_wo is null? 0:wo_wo )+( tot_so_subc is null? 0: tot_so_subc) as total;

subs_wo_pipes = JOIN sub_wo_uni by (src_id_t,src_stream_t) FULL, add_so by (src_id,src_stream);
subs_wo_pipes_uni = FOREACH subs_wo_pipes GENERATE (src_id_t is null?  src_id:src_id_t) as src_id_f:chararray, 
(src_stream_t is null?  src_stream:src_stream_t) as src_stream_f:chararray, 
(total is null? 0:total) + (tot_events is null ? 0: tot_events )+ ( tot_ne is null ? 0: tot_ne ) as total, 1 as constant;

C = GROUP subs_wo_pipes_uni all;

max_pop_so = FOREACH C GENERATE 
MAX(subs_wo_pipes_uni.total) as max_pop, 1 as constant;

sos_count_max = JOIN subs_wo_pipes_uni by (constant), max_pop_so by (constant);

popularity_so = FOREACH sos_count_max 
		 GENERATE CONCAT(CONCAT(src_id_f, '#!'),src_stream_f) as stream_oid, 'service_object_stream' as type, 'popularity' as reptype, 
		 1+ (total*9/max_pop) as pop:float, total;

store popularity_so INTO '$OUTFOLDER/streams_popularity';


/* ================================================================================ */
/* ================================================================================ */
/* ================================================================================ */

