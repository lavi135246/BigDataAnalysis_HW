Table_csv = LOAD '{1987,1988,1989,1990,1991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008}.csv' 
	USING PigStorage(',') AS (
    Year:chararray,
    Month:int,
    DayofMonth:chararray,
    DayOfWeek:chararray,
    DepTime:chararray,
    CRSDepTime:chararray,
    ArrTime:chararray,
    CRSArrTime:chararray,
    UniqueCarrier:chararray,
    FlightNum:chararray,
    TailNum:chararray,
    ActualElapsedTime:chararray,
    CRSElapsedTime:chararray,
    AirTime:chararray,
    ArrDelay:int,
    DepDelay:int,
    Origin:chararray,
    Dest:chararray,
    Distance:chararray,
    TaxiIn:chararray,
    TaxiOut:chararray,
    Cancelled:chararray,
    CancellationCode:chararray,
    Diverted:chararray,
    CarrierDelay:int,
    WeatherDelay:int,
    NASDelay:int,
    SecurityDelay:int,
    LateAircraftDelay:int);

-- Q1
B = GROUP Table_csv BY Month;
C = FOREACH B{
	C1 = FOREACH Table_csv GENERATE (ArrDelay+DepDelay);
	GENERATE group, C1;
}
--average = FOREACH C GENERATE group, AVG(C1.$0);
--maximum = FOREACH C GENERATE group, MAX(C1.$0);
ans = FOREACH C GENERATE group, AVG(C1.$0), MAX(C1.$0);
ans1 = ORDER ans BY group ASC;


-- Q2
B = FILTER Table_csv BY (WeatherDelay > 0);
C = GROUP B ALL;
--D = FOREACH C GENERATE group, COUNT(B);
--E = FOREACH C GENERATE group, AVG(B.WeatherDelay);
ans2 = FOREACH C GENERATE group, COUNT(B), AVG(B.WeatherDelay);



-- Q3
B = GROUP Table_csv BY (Year, Month);
C = FOREACH B GENERATE group, COUNT(Table_csv);
ans3 = ORDER C BY $1 ASC;



-- Q4
B = GROUP Table_csv BY Origin;
average = FOREACH B GENERATE group, AVG(Table_csv.DepDelay);
order_avg = ORDER average BY $1 DESC;

-- with largest average: FMN, CYS, OGD, BFF, PIR

B = FILTER Table_csv BY ((Origin=='FMN') OR (Origin=='CYS') OR (Origin=='OGD') OR (Origin=='BFF') OR (Origin=='PIR'));
C = GROUP B BY Origin;
ans4 = FOREACH C{
	ca_cnt = FILTER B BY (CarrierDelay>0);
	wh_cnt = FILTER B BY (WeatherDelay>0);
	na_cnt = FILTER B BY (NASDelay>0);
	se_cnt = FILTER B BY (SecurityDelay>0);
	la_cnt = FILTER B BY (LateAircraftDelay>0);
	GENERATE group, COUNT(ca_cnt),COUNT(wh_cnt),COUNT(na_cnt),COUNT(se_cnt),COUNT(la_cnt);
};


-- Other
B = GROUP Table_csv BY Origin;
average = FOREACH B GENERATE group, COUNT(Table_csv);
order_avg = ORDER average BY $1 DESC;
top_airport = LIMIT order_avg 5;

-- find top 5 frequent airport
-- delay
B = FILTER Table_csv BY ((Origin=='ORD') OR (Origin=='ATL') OR (Origin=='DFW') OR (Origin=='LAX') OR (Origin=='PHX'));
C = GROUP B BY Origin;
ans5 = FOREACH C{
	ca_cnt = FILTER B BY (CarrierDelay>0);
	wh_cnt = FILTER B BY (WeatherDelay>0);
	na_cnt = FILTER B BY (NASDelay>0);
	se_cnt = FILTER B BY (SecurityDelay>0);
	la_cnt = FILTER B BY (LateAircraftDelay>0);
	GENERATE group, COUNT(ca_cnt),COUNT(wh_cnt),COUNT(na_cnt),COUNT(se_cnt),COUNT(la_cnt);
};

ans6 = FOREACH C GENERATE group, AVG(B.DepDelay);

-- destination not very sure. Maybe try group by Origin and Dest??

------------- NOT WORKING --------------
-- ans6 = FOREACH C{
--	destination = GROUP B BY Dest;
--	dest_cnt = FOREACH destination GENERATE COUNT(B);
--	sorted = ORDER dest_cnt BY DESC;
--	top_n = LIMIT sorted 2;
--	GENERATE group, FLATTEN(top_n);
-- };

C = GROUP B BY (Origin, Dest);
D = FOREACH C GENERATE group, COUNT(B);
E = ORDER D BY $1 DESC;
F = LIMIT E 10;
G = FILTER Table_csv BY (((Origin=='ORD')AND(Dest=='MSP')) 
						OR ((Origin=='LAX')AND((Dest=='PHX')OR(Dest=='LAS')OR(Dest=='SFO'))) 
						OR ((Origin=='PHX')AND(Dest=='LAX'))
						);
H = GROUP G BY (Origin, Dest);
ans7 = FOREACH H GENERATE group, AVG(G.DepDelay);
ans8 = FOREACH H{
	ca_cnt = FILTER G BY (CarrierDelay>0);
	wh_cnt = FILTER G BY (WeatherDelay>0);
	na_cnt = FILTER G BY (NASDelay>0);
	se_cnt = FILTER G BY (SecurityDelay>0);
	la_cnt = FILTER G BY (LateAircraftDelay>0);
	GENERATE group, COUNT(ca_cnt),COUNT(wh_cnt),COUNT(na_cnt),COUNT(se_cnt),COUNT(la_cnt);
};