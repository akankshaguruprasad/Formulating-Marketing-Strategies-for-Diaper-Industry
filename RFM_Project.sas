PROC IMPORT OUT= customer_panel_groc DATAFILE= "h:/diapers/diapers_PANEL_GR_1114_1165.xlsx" 
            DBMS=xlsx REPLACE;
     SHEET="diapers_PANEL_GR_1114_1165"; 
     GETNAMES=YES;
RUN;

proc print data=customer_panel_groc(obs=5);run;

PROC IMPORT OUT= customer_panel_drug DATAFILE= "h:/diapers/diapers_PANEL_DR_1114_1165.xlsx" 
            DBMS=xlsx REPLACE;
     SHEET="diapers_PANEL_DR_1114_1165"; 
     GETNAMES=YES;
RUN;

proc print data=customer_panel_drug(obs=5);run;

PROC IMPORT OUT= customer_panel_mass DATAFILE= "h:/diapers/diapers_PANEL_MA_1114_1165.xlsx" 
            DBMS=xlsx REPLACE;
     SHEET="diapers_PANEL_MA_1114_1165"; 
     GETNAMES=YES;
RUN;

data customer_panel;
set customer_panel_groc customer_panel_drug customer_panel_mass;
run;

proc print data=customer_panel(obs=5);run;

PROC IMPORT OUT= metadata 
            DATAFILE= "H:\diapers\prod_diaper" 
            DBMS=EXCEL REPLACE;
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

data metadata;
set metadata;
VEND = put(input(VEND,best5.),z5.);
ITEM = put(input(ITEM,best5.),z5.);
COLUPC=input(cats(SY,GE,VEND,ITEM),best14.);
run;

proc sort data=customer_panel out=customer_panel;
by COLUPC;run;

proc sort data=metadata out=metadata;
by COLUPC;run;

data cus_merged;
merge customer_panel(in=x) metadata(in=y);
by COLUPC;
if x=1 and y=1;
run;

data cus_merged;
set cus_merged;
if L5 in('HUGGIES','PAMPERS','PRIVATE LABEL','LUVS','DRYPERS','FITTI') then Brand=L5;
else Brand='Other';
run;

proc print data = cus_merged(obs = 5);run; 

Data RFM_Data; set cus_merged; 
Drop L1 L2 L3 L4 L5 L9 Level UPC SY GE VEND ITEM _STUBSPEC_1828RC VOL_EQ PRODUCT_TYPE STAGE_PHASE FLAVOR_SCENT WEIGHT_OF_BABY COLOR THICKNESS USER_INFO;
run;
proc print data = RFM_Data (obs=10);run;

data RFM_Data; set RFM_Data; if Brand ^='HUGGIES' then delete; run;

proc sql;
	create table RFM_Monetary as
	select Sum(DOLLARS)as Total_Price, PANID
	from RFM_Data
	group by PANID
order by PANID;
quit;

proc sql;
	create table RFM_Recency as
	select MAX(WEEK) as Recency, PANID
	from RFM_Data
	group by PANID
order by PANID;
quit;

proc sql;
	create table RFM_Frequency as
	select Sum(UNITS) as Frequency, PANID
	from RFM_Data
	group by PANID
order by PANID;
quit;

proc rank data=RFM_Recency out=RFM_R ties=low groups=5;var Recency;ranks rnkR;run;
proc rank data=RFM_Monetary out=RFM_M ties=low groups=5;var Total_Price;ranks rnkM;run;

proc print data=RFM_F (obs=10);run;
proc freq data=RFM_F;table rnkF;run;

data rankingRFM;
merge RFM_Recency  RFM_Monetary;
by PANID;run;

proc print data= rankingRFM (obs =20);run;
proc corr data = rankingRFM;
var Recency Frequency Total_Price ;run;

data rank;
merge RFM_R RFM_M;
by PANID;run;

proc print data = rank (obs=10);run;

data rank;set rank;
rank = catx('-',rnkR,rnkM);
run;

proc print data = rank (obs=10);run;

proc sql;
	create table RFM as
	select PANID, Recency, Total_Price, rnkR,rnkM,rank 
	from rank
	order by PANID;
quit;

proc print data = RFM(obs = 10);run;

proc freq data = RFM; table rank;run;

data Cutomertype; set RFM;
if rnkR=0 then Customer_Type = "CustAtRisk";
else if 0<rnkR<=3 and 0<=rnkM<3 then Customer_Type = "Potential";
else if rnkR=3 and (rnkM=0 or rnkM=1) then Customer_Type = "Potential";
else if rnkR=4 and (rnkM=0 or rnkM=1) then Customer_Type = "Potential";
else Customer_Type = "Loyal";
run;

proc print data = Cutomertype(obs = 10); run;

proc freq data = Cutomertype;table Customer_Type;run;
