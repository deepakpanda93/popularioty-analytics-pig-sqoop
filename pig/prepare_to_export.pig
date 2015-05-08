%default INPUTFILE 'input.txt'
%default OUTFOLDER '.'

A = LOAD '$INPUTFILE' using PigStorage('\t') AS (type:chararray, id:chararray, json:chararray);
FINALREP = FILTER A BY type=='reputation';
SUBREP = FILTER A BY type!='reputation';
JSONFINAL = FOREACH FINALREP GENERATE CONCAT(CONCAT(id,','),json);
JSONSUB = FOREACH SUBREP GENERATE CONCAT(CONCAT(id,','),json);
store JSONSUB INTO '$OUTFOLDER_SUB';
store JSONFINAL INTO '$OUTFOLDER_AGG';

