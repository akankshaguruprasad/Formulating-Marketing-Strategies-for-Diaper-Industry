DATA diaper_groc;
INFILE 'H:\diapers\diapers_groc_1114_1165' FIRSTOBS=2;
INPUT IRI_KEY WEEK SY $ GE $ VEND $ ITEM $ UNITS DOLLARS F $ D PR;
run;

DATA diaper_drug;
INFILE 'H:\diapers\diapers_drug_1114_1165' FIRSTOBS=2;
INPUT IRI_KEY WEEK SY $ GE $ VEND $ ITEM $ UNITS DOLLARS F $ D PR;
run;

data diaper;
set diaper_groc diaper_drug;
run;

PROC IMPORT OUT= metadata 
            DATAFILE= "H:\diapers\prod_diaper" 
            DBMS=EXCEL REPLACE;
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

data diaper;
set diaper;
VEND = put(input(VEND,best5.),z5.);
ITEM = put(input(ITEM,best5.),z5.);
COLUPC=input(cats(SY,GE,VEND,ITEM),best14.);
SY = put(input(SY,best2.),z2.);
GE = put(input(GE,best2.),z2.);
UPC=catx('-',SY,GE,VEND,ITEM);
run;

proc sort data=diaper out=diaper;
by UPC;run;

proc print data = diaper (obs = 5);run;

proc sort data=metadata out=metadata;
by UPC;run;

proc print data = metadata (obs = 5);run;

data merged;
merge diaper (in=x) metadata(in=y);
by UPC;
if x=1 and y=1;
run;

proc print data = merged (obs = 5);run;

proc sql;
 create table Q1 as
    select L5,sum(DOLLARS)as brand_sales
    from merged
    group by L5
	order by brand_sales desc;
 quit;

 proc print data = Q1 (obs = 6); run;

 data merged;
set merged;
if L5 in('HUGGIES','PAMPERS','PRIVATE LABEL','LUVS','DRYPERS','FITTI') then Brand=L5;
else Brand='Other';
run;

proc print data = merged (obs = 50);run;

proc freq data = merged; table Brand;run;

proc sql;
create table q2 as
select Brand, Sum(dollars) as Total_sales
from merged
group by Brand
order by Total_sales desc;
quit;

proc print data=q2(obs=10);run;

data merged;
set merged;
price=dollars/(units*vol_eq);
if F eq 'NONE' then Feat=0; else Feat=1;
if D eq 0 then Disp=0; else Disp=1;
run;

proc contents data=merged;run;
proc print data=merged(obs=5);run;

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

proc print data=customer_panel_mass(obs=5);run;

data customer_panel;
set customer_panel_groc customer_panel_drug customer_panel_mass;
run;

proc print data=customer_panel(obs=5);run;

proc sql;
create table info_table as
select IRI_KEY,WEEK,Brand, round(Avg(price),0.01) as Avg_Price, round(Max(Disp),0.01) as Display, round(Max(Feat),0.01) as Feature, Max(PR) as PriceReduction
from merged
where IRI_KEY in 
      (Select unique(IRI_KEY) 
      from customer_panel)  
group by IRI_KEY,WEEK,Brand
order by IRI_KEY,WEEK,Brand;
quit;

proc print data=info_table(obs=10);run;

/*Decision mapping*/

data metadata;
set metadata;
VEND = put(input(VEND,best5.),z5.);
ITEM = put(input(ITEM,best5.),z5.);
COLUPC=input(cats(SY,GE,VEND,ITEM),best14.);
run;

proc print data=metadata(obs=5);run;

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
proc freq data = cus_merged;table Brand;run;

proc sql;
create table cus_merged_2 as
select panid,week,iri_key,Brand, count(*) as count
from cus_merged
group by panid,week,iri_key,Brand;
quit;

proc print data=cus_merged_2(obs=10);run;

proc sql;
create table MNL_data as
select PANID,info_table.IRI_KEY, info_table.WEEK, info_table.Brand, Avg_Price, Display, Feature, PriceReduction, 
case when info_table.Brand=cus_merged_2.Brand then 1 else 0 end as decision,
catx("-",PANID,info_table.IRI_KEY, info_table.WEEK) as panelid
from info_table, cus_merged_2
where info_table.IRI_KEY=cus_merged_2.IRI_KEY and info_table.WEEK=cus_merged_2.WEEK ;
quit;

proc print data=MNL_data(obs=20);run;

proc sql;
create table remove_table as
select panid,week,iri_key, count(*) as count
from MNL_data
group by panid,week,iri_key
having count ne 7
order by panid,week,iri_key;
quit;

proc sort data=MNL_data out=MNL_data;
by panid week iri_key;
run;

data MNL_data;
merge MNL_data(in=x) remove_table(in=y);
by panid week iri_key;
if x=1 and y=0;
run;

proc print data=MNL_data(obs=20);run;

PROC IMPORT OUT= customer_details 
            DATAFILE= "h:/diapers/ads demo3.csv"
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC print data=customer_details(obs=10);run;

proc sort data=MNL_data out=MNL_data;
by panid;
run;

proc sort data=customer_details out=customer_details;
by panelist_id;
run;

data MNL_data;
merge MNL_data(in=x) customer_details(rename=(panelist_id=panid) in=y);
by panid;
if x=1;
run;


proc sort data=MNL_data out=MNL_data;
by panelid;
run;

proc print data=MNL_data(obs=20);run;

data MNL_data_v2;
set MNL_data;
by panelid;
if first.panelid then pid+1;
if Brand = 'HUGGIES' then B1=1; else B1=0;
if Brand = 'PAMPERS' then B2=1;else B2=0;
if Brand = 'PRIVATE LABEL' then B3=1;else B3=0;
if Brand = 'LUVS' then B4=1;else B4=0;
if Brand = 'DRYPERS' then B5=1;else B5=0;
if Brand = 'FITTI' then B6=1;else B6=0;
D1=Display*B1;
D2=Display*B2;
D3=Display*B3;
D4=Display*B4;
D5=Display*B5;
D6=Display*B6;
F1=Feature*B1;
F2=Feature*B2;
F3=Feature*B3;
F4=Feature*B4;
F5=Feature*B5;
F6=Feature*B6;
Price1=Avg_Price*B1;
Price2=Avg_Price*B2;
Price3=Avg_Price*B3;
Price4=Avg_Price*B4;
Price5=Avg_Price*B5;
Price6=Avg_Price*B6;
Fam1=Family_Size*B1;
Fam2=Family_Size*B2;
Fam3=Family_Size*B3;
Fam4=Family_Size*B4;
Fam5=Family_Size*B5;
Fam6=Family_size*B6;
Inc1=Combined_Pre_Tax_Income_of_HH*B1;
Inc2=Combined_Pre_Tax_Income_of_HH*B2;
Inc3=Combined_Pre_Tax_Income_of_HH*B3;
Inc4=Combined_Pre_Tax_Income_of_HH*B4;
Inc5=Combined_Pre_Tax_Income_of_HH*B5;
Inc6=Combined_Pre_Tax_Income_of_HH*B6;
Age1=HH_AGE*B1;
Age2=HH_AGE*B2;
Age3=HH_AGE*B3;
Age4=HH_AGE*B4;
Age5=HH_AGE*B5;
Age6=HH_AGE*B6;
PR_D=PriceReduction*Display;
PR_Feat=PriceReduction*Feature;
run;

PROC print data=MNL_data_v2(obs=10);run;

proc mdc data=MNL_data_v2; 
   model decision =B1-B6 Avg_Price Display Feature PriceReduction Inc1-Inc6 Age1-Age6/ 
            type=clogit 
            nchoice=7
            optmethod=qn 
            covest=hess; 
   id pid; 
   output out=mnl_outp p=prob xbeta=beta pred=pred;
run;


data mnl_outp_v2(keep=price_own_elast disp_own_elast feat_own_elast price_cross_elast disp_cross_elast feat_cross_elast PR_own_elast PR_cross_elast Brand prob Avg_Price Display Feature PriceReduction);
set mnl_outp;
price_own_elast = (1-prob)*Avg_Price*(-0.3037);
disp_own_elast = (1-prob)*Display*0.7551;
feat_own_elast = (1-prob)*Feature*0.4842;
PR_own_elast = (1-prob)*PriceReduction*0.2149;
price_cross_elast = -prob*Avg_Price*-0.3037;
disp_cross_elast = -prob*Display*0.7551;
feat_cross_elast = -prob*Feature*0.4842;
PR_cross_elast = -prob*PriceReduction*0.2149;
run;


proc means data=mnl_outp_v2 mean MAXDEC=2;
var Avg_Price Display Feature price_own_elast disp_own_elast feat_own_elast price_cross_elast disp_cross_elast feat_cross_elast PR_own_elast PR_cross_elast;
class Brand;
output out=elasticity(where=(_TYPE_=1)) mean(Avg_Price Display Feature price_own_elast disp_own_elast feat_own_elast price_cross_elast disp_cross_elast feat_cross_elast PR_own_elast PR_cross_elast)= Avg_Price Display Feature price_own_elast disp_own_elast feat_own_elast price_cross_elast disp_cross_elast feat_cross_elast PR_own_elast PR_cross_elast;
run;


proc transpose data=elasticity(drop=_TYPE_ _FREQ_) out=elastic name=Result;
    id Brand;
run;

proc print data=elastic ROUND;run;

