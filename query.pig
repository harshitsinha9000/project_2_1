REGISTER '/usr/local/pig/lib/piggybank.jar';

DEFINE XPATH org.apache.pig.piggybank.evaluation.xml.XPath();

dataset = load '/flume_import/StatewiseDistrictwisePhysicalProgress.xml' using org.apache.pig.piggybank.storage.XMLLoader('row') as (x:chararray);

b = foreach dataset generate XPATH(x,'row/State_Name') as State_Name,XPATH(x,'row/District_Name') as District_Name,
XPATH(x,'row/Project_Objectives_IHHL_BPL') as (Project_Objectives_IHHL_BPL:int),
XPATH(x,'row/Project_Objectives_IHHL_APL') as (Project_Objectives_IHHL_APL:int),
XPATH(x,'row/Project_Objectives_IHHL_TOTAL') as (Project_Objectives_IHHL_TOTAL:int),
XPATH(x,'row/Project_Objectives_SCW') as Project_Objectives_SCW,
XPATH(x,'row/Project_Objectives_School_Toilets') as Project_Objectives_School_Toilets,
XPATH(x,'row/Project_Objectives_Anganwadi_Toilets') as Project_Objectives_Anganwadi_Toilets,
XPATH(x,'row/Project_Objectives_RSM') as Project_Objectives_RSM,
XPATH(x,'row/Project_Objectives_PC')  as Project_Objectives_PC,
XPATH(x,'row/Project_Performance_IHHL_BPL') as (Project_Performance_IHHL_BPL:int),
XPATH(x,'row/Project_Performance_IHHL_APL') as (Project_Performance_IHHL_APL:int),
XPATH(x,'row/Project_Performance_IHHL_TOTAL') as (Project_Performance_IHHL_TOTAL:int),
XPATH(x,'row/Project_Performance_SCW') as Project_Performance_SCW,
XPATH(x,'row/Project_Performance_School_Toilets') as Project_Performance_School_Toilets,
XPATH(x,'row/Project_Performance_Anganwadi_Toilets') as Project_Performance_Anganwadi_Toilets,
XPATH(x,'row/Project_Performance_RSM') as Project_Performance_RSM,
XPATH(x,'row/Project_Performance_PC')  as Project_Performance_PC;


--Query 1 (100% objective in BPL)

district =  FOREACH (filter b by Project_Objectives_IHHL_BPL  >= Project_Performance_IHHL_BPL) generate District_Name;

--Query 2 (80 % using udf)
REGISTER 'compute_bpl.py' USING streaming_python AS myudf1

bpl_80 = FOREACH b GENERATE myudf1.compute_bpl(Project_Objectives_IHHL_BPL,Project_Performance_IHHL_BPL,District_Name)


