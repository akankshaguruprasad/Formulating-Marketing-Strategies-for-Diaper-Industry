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

proc sort data=metadata out=metadata;
by UPC;run;

data merged;
merge diaper (in=x) metadata(in=y);
by UPC;
if x=1 and y=1;
run;

data merged;
set merged;
if L5 in('HUGGIES','PAMPERS','PRIVATE LABEL','LUVS','DRYPERS','FITTI') then Brand=L5;
else Brand='Other';
run;

data merged;
set merged;
price=dollars/(units*vol_eq);
if F eq 'NONE' then Feat=0; else Feat=1;
if D eq 0 then Disp=0; else Disp=1;
run;

data merged;
set merged;

data Panel; set merged; if Brand ^='HUGGIES' then delete; run;

data panel; set panel; 
if F = 'NONE' then Feature = 0;
if F = 'A+' then Feature = 1;
if F = 'A' then Feature = 1;
if F = 'B' then Feature = 1;
if F = 'C' then Feature = 1;
run;

proc freq data = Panel; table F;run;
proc print data = Panel(obs = 5);run;
proc sql;
	create table regression_avg as
	select IRI_KEY, WEEK, avg(price) as avg_price, avg(Disp) as avg_D, avg(Feat) as avg_F, avg(PR)as avg_PR
	from Panel
	group by IRI_KEY, WEEK
order by IRI_KEY, WEEK;
quit;

proc sql;
	create table regression_interaction as
	select IRI_KEY, WEEK, avg_price, avg_D, avg_F, avg_PR, avg_D*avg_price as avg_price_D,
	avg_F*avg_price as avg_price_F, avg_PR*avg_price as avg_price_PR
	from regression;
quit;

proc sql;
	create table regression_sum as 
	select IRI_KEY, WEEK, sum(DOLLARS) as total_dollar_sale
	from Panel
	group by IRI_KEY, WEEK
order by IRI_KEY, WEEK;
quit;

DATA regression_int;
MERGE regression_interaction regression_sum;
BY IRI_KEY WEEK;run;

proc panel data = regression_int;
ID IRI_KEY WEEK;
model total_dollar_sale = avg_price avg_D avg_F avg_PR avg_price_D avg_price_F/rantwo;
run;

proc print data = panel(obs = 10);run;

PROC IMPORT OUT= diaper_storedet DATAFILE= "h:/diapers/Delivery_Stores.xlsx" 
            DBMS=xlsx REPLACE;
     SHEET="Delivery_Stores"; 
     GETNAMES=YES;
RUN;
proc sort DATA = diaper_storedet;
BY IRI_KEY;
run;

proc sort DATA = panel;
BY IRI_KEY;
run;
proc print data = diaper_storedet (obs = 10);run;

data diaperStore;
   merge panel diaper_storedet;
   by IRI_KEY;
run;

proc print data = diaperStore(obs =10);run;
data StoreLA; set diaperStore;
if Market_Name ^= 'LOS ANGELES' then delete;
if F ^= 'A' and F^= 'B' then delete;run;

proc ttest data = StoreLA sides = L; var dollars; class F; run;

data StoreNY; set diaperStore;
if Market_Name ^= 'NEW YORK' then delete;
if F ^= 'A' and F^= 'B' then delete;run;

proc ttest data = StoreNY sides = U; var dollars; class F; run;
