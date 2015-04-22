-- this query takes the templatelogic files, matches them with clicks and sales, to come up with a funnel formation based on various dimensions, specific to the nature/type of the funnel : onlineretail / funnelflighttravel / funnelflighthotel

%default templatelogic_files 's3://viz-raw-data/templatelogic/2015/04/09/' -- load just one day data.
%default csv_dir 's3://viz-db-dump/' -- the csv directory for files containing the mappings
%default udf_path 's3://viz-emr-jars/udfs.jar' -- the path for the udfs
%default piggy_path 's3://viz-emr-jars/piggy_sreenath.jar' -- the piggy path
%default jar_path 's3://viz-emr-jars/' -- the path containing all the jars.
%default click_files 's3://viz-raw-data/click/2015/04/09/' -- the click files, the same day, although
%default sale_files 's3://viz-db-dump/hourly/2015/04/09/{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23}/salesrep/' -- sales one day files.
%default output_dir_online 's3://viz-temp/sumit/output/finalOutput0409/online124/' -- where the output goes
%default output_dir_hotel 's3://viz-temp/sumit/output/finalOutput0409/hotel124/' -- where the output goes
%default output_dir_flight 's3://viz-temp/sumit/output/finalOutput0409/flight124/' -- where the output goes

register $udf_path; -- to use the built udfs
register $piggy_path; -- to use the inbuilt udfs

-- to read the json data
register $jar_path/json-simple-1.1.jar;
register $jar_path/elephant-bird-core-4.5.jar;
register $jar_path/elephant-bird-pig-4.5.jar;
register $jar_path/elephant-bird-hadoop-compat-4.5.jar;

-- pig mapred settings
set mapred.child.java.opts -Xmx8000m
set mapred.reduce.slowstart.completed.maps 0.9
set mapred.map.tasks.speculative.execution false
SET default_parallel 20
SET pig.tmpfilecompression TRUE
SET pig.tmpfilecompression.codec gz
SET mapred.compress.map.output TRUE      --To make Compression true between Map and Reduce
SET mapred.map.output.compression.codec com.hadoop.compression.lzo.LzopCodec
SET mapred.output.compress TRUE
SET mapred.output.compression.codec org.apache.hadoop.io.compress.GzipCodec;

define SegmentId2SegmentName com.vizury.udfs.common.TRANSLATE('$csv_dir', 'segidtosegname.csv');
define BannerId2BannerClass com.vizury.udfs.common.TRANSLATE('$csv_dir', 'bannerIdtobannerclass.csv');

define BannerId2AdvId com.vizury.udfs.common.TRANSLATE('$csv_dir', 'banneridtoadvid.csv');
define AdvId2Advname com.vizury.udfs.common.TRANSLATE('$csv_dir', 'advidtoadvname.csv');

define AdvName2FunnelName com.vizury.udfs.common.TRANSLATE('$csv_dir', 'AdvNameToCampaignType.csv');

define TS_GMT_2_millisec com.vizury.udfs.common.TIMESTAMP_TO_MILLISEC('GMT', 'yyyy-MM-dd HH:mm:ss');

define length             org.apache.pig.piggybank.evaluation.string.LENGTH();

templatelogic_data = LOAD '$templatelogic_files' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

tlogic_req0 = foreach templatelogic_data {
						currTime = CurrentTime();
  						year = (chararray)GetYear(currTime);
  						month = (chararray)GetMonth(currTime);
  						day = (chararray)GetDay(currTime);
						advid = BannerId2AdvId($0#'_bid') ;
						advname = AdvId2Advname(advid) ;
						productcustomization = TRIM($0#'_pc');
						categorydefaultproduct = TRIM($0#'_cdp');
						subcategory1 = TRIM($0#'_sc1');
						subcategory2 = TRIM($0#'_sc2');
						-- citycustomization = (( (($0#'_city') matches '') or (($0#'_city' matches 'null')) or ((length($0#'_city')<=2)) ) ? 'NonCust' : 'CityCust');
						-- hotelcustomization = (($0#'_isDef' == 0) ? 'NonCust' : 'HotelCust');
					generate TRIM($0#'_ver') as version_no,
						 TRIM($0#'_ip') as ip,
						 TRIM($0#'_c') as cookie:chararray,
						 TRIM($0#'_ts') as timestamp,
						 BannerId2BannerClass($0#'_bid') as bannerclass, -- write it : mapping
						 AdvId2Advname(advid) as campaign, -- write it : mapping
						 AdvName2FunnelName(advname) as funnelname,
						 TRIM($0#'_iid') as imprid:chararray, -- write it : clean
						 SegmentId2SegmentName($0#'_sid') as segname, -- write it : mapping
						 TRIM($0#'_pid') as productid,
						 TRIM($0#'_cid') as category_id,
						 TRIM($0#'_pc') as productcustomization, -- # of products customized
						 TRIM($0#'_cdp') as categorydefaultproduct, -- cat customized ?
						 TRIM($0#'_sc1') as subcategory1, -- subcat customized?
						 TRIM($0#'_sc2') as subcategory2, -- subsubcat customized ? 
						 TRIM($0#'_ppc') as priorityprodcust, -- vague 							 TRIM($0#'_pcc') as prioritycatcust, -- vague
						 TRIM($0#'_psc1') as prioritysubcat1cust, -- vague
						 TRIM($0#'_psc2') as prioritysubcat2cust, -- vague
						 TRIM($0#'_dp') as defaultproduct,
						 -- (productcustomization matches '0' ? 'prod_no'  : 'prod_yes') as isProdCust:chararray, -- yes/no
						 -- (categorydefaultproduct matches'0' ? 'cat_no' : 'cat_yes') as isCatCust : chararray, -- yes/no
						 -- (subcategory1 matches '0' ? 'subcat_no' : 'subcat_yes') as isSubCatCust:chararray, 							 (subcategory2 matches '0' ? 'subsubcat_no' : 'subsubcat_yes') as isSubSubCatCust:chararray,
						 TRIM($0#'_isDef') as isdefaulthotel,
						 TRIM($0#'_city') as city_shown,
						 TRIM($0#'_pinfo') as pinfo,
						 TRIM($0#'_lshid') as HotelId,
						 -- TRIM($0#'_cust_0') as customization_hotel,
						 TRIM($0#'_srh_0') as HotelBasedOnDepDate,
						 TRIM($0#'_srh_1') as HotelBasedOnDepDateNext,
						 TRIM($0#'_srh_-1') as HotelBasedOnDepDatePrev,
						 TRIM($0#'_ph_0') as PriorityBasedHotelDepDate,
						 TRIM($0#'_ph_1') as PriorityBasedHotelDepDateNext,
						 TRIM($0#'_ph_-1') as PriorityBasedHotelDepDatePrev,
						 --(citycustomization  MATCHES 'CityCust' ? 'CityCust' : (hotelcustomization MATCHES 'HotelCust' ? 'HotelCust' : 'NonCus')) as customization_hotel,
						 TRIM($0#'_cust_0')  as customization_hotel,
						 TRIM($0#'_src') as source, -- source city
						 TRIM($0#'_dest') as destination, -- destination city
						 TRIM($0#'_ddt') as depdate,
						 TRIM($0#'_trt') as trt, -- vague
						 TRIM($0#'_slf') as slf, -- vague
						 TRIM($0#'_pla') as isPlaceShown, -- helps us know cust'ion based on place shown
						 TRIM($0#'_pri') as isPriceShown, -- helps us know cust'ion based on price shown
						 TRIM($0#'_ins') as ins, -- vague
						 TRIM($0#'_ind') as ind, -- vague
						 TRIM($0#'_oid') as offerid, 
						 TRIM($0#'_sec') as sectors,
						 TRIM($0#'_dt1') as dateMinus1, -- others : date-1
						 TRIM($0#'_dt2') as dateMinus2, -- others : date-2
						 TRIM($0#'_dt3') as date0, -- others : date
						 TRIM($0#'_dt4') as datePlus1, -- others : date+1 
						 TRIM($0#'_dt5') as datePlus2, -- others : date+2
						 TRIM($0#'_rdt') as returndate, -- return date
						 (($0#'_pla' == 1 and $0#'_pri' == 0) ? 'OnlyPlaceShown' : (($0#'_pla' == 1 and $0#'_pri' == 1) ? 'PlacePriceShown' : 'NoCust')) as customization_flight ,
						CONCAT(CONCAT(CONCAT(year, '-'), CONCAT(month, '-')),day) as datecur;
			};

tlogic_req = FILTER tlogic_req0 by length(imprid)>2;

SPLIT tlogic_req into tlogic_online IF funnelname matches 'E-Commerce' , tlogic_hotel IF funnelname matches 'Travel-Hotel', tlogic_flight IF funnelname matches 'Travel-Flight' ;


clickdata = load '$click_files' using PigStorage('\t') as (cookie:chararray, clickiid:chararray, click_ts:chararray);

imprclick = join tlogic_hotel by (imprid) LEFT OUTER , clickdata by (clickiid);

raw_sales     = load '$sale_files' using PigStorage('\t') as (d1, scookie:chararray, d3, d4, d5, d6, click_ts:chararray, sale_ts:chararray);

salesWith_ms  = foreach raw_sales generate scookie as scookie:chararray, click_ts as click_ts:chararray, TS_GMT_2_millisec(click_ts) as click_ms:long, TS_GMT_2_millisec(sale_ts) as sale_ms:long; -- write it : math 

ics = join imprclick by (tlogic_hotel::cookie,clickdata::click_ts)  LEFT OUTER , salesWith_ms by (scookie,click_ts); 

-- ics_city_grpd = group ics by citycustomization;
-- ics_hotel_grpd = group ics by hotelcustomization;

-- customizing ics

-- logic for N day sales
-- oneDaySales   = filter salesWith_ms by sale_ms < 86400000L + click_ms;	
-- fiveDaySales   = filter salesWith_ms by sale_ms < 432000000L + click_ms;	
-- tenDaySales   = filter salesWith_ms by sale_ms < 864000000L + click_ms;	
-- twentyDaySales   = filter salesWith_ms by sale_ms < 1728000000L + click_ms;

-- please help me with the logic below

ics_grp = group ics by (funnelname,version_no,campaign,datecur,segname,bannerclass,customization_hotel,HotelBasedOnDepDate,
HotelBasedOnDepDateNext,HotelBasedOnDepDatePrev,offerid,PriorityBasedHotelDepDate,PriorityBasedHotelDepDateNext,
PriorityBasedHotelDepDatePrev,HotelId,city_shown);	

ics_grp_sales = foreach ics_grp {
						isOneDaySales = filter ics by sale_ms < click_ms + 86400000L;
						--distODS = DISTINCT isOneDaySales;
						isFiveDaySales = filter ics by sale_ms < click_ms + 432000000L;
						--distFDS = DISTINCT isFiveDaySales;
						isTenDaySales = filter ics by sale_ms < click_ms + 864000000L;
						--distTDS = DISTINCT isTenDaySales;
						twentyDaySales = filter ics by sale_ms < click_ms + 1728000000L;
						--distTWDS = twentyDaySales;
						clicks = ics.clickiid;
						sales = ics.scookie;
						-- distclicks = ics::clickdata::clickiid;
						-- distsales = DISTINCT ics::salesWith_ms::;

						generate -- group::funnelname as funnelname,
								 -- group::version_no as ver_no,
								 -- group::campaign as campaign,
								 -- group::date as date,
								 -- group::segname as segmentname,
								 -- group::bannerclass as bannerclass,
								 -- group::Customization as Customization,
								 -- group::HotelBasedOnDepDate as HotelBasedOnDepDate,
								 -- group::HotelBasedOnDepDateNext as HotelBasedOnDepDateNext,
								 -- group::HotelBasedOnDepDatePrev as HotelBasedOnDepDatePrev,
								 -- group::offerid as offername,
								 -- group::PriorityBasedHotelDepDate as PriorityBasedHotelDepDate,
								 -- group::PriorityBasedHotelDepDateNext as PriorityBasedHotelDepDateNext,
								 -- group::PriorityBasedHotelDepDatePrev as PriorityBasedHotelDepDatePrev,
								 -- group::HotelId as HotelId,
								 flatten(group) as (funnelname,version_no,campaign,datecur,segname,bannerclass,customization_hotel,HotelBasedOnDepDate,HotelBasedOnDepDateNext,HotelBasedOnDepDatePrev,offerid,PriorityBasedHotelDepDate,
								 PriorityBasedHotelDepDateNext,PriorityBasedHotelDepDatePrev,HotelId,city_shown),
								 -- please specify something from Impressions table to have a count of Impr
								 COUNT(ics) as Impression,
								 COUNT(ics.clickiid) as click,
								 COUNT(ics.scookie) as sales,
								 COUNT(isOneDaySales) as SalesOneDay,
								 COUNT(isFiveDaySales) as SalesFiveDay,
								 COUNT(isTenDaySales) as SalesTenDay,
								 COUNT(twentyDaySales) as SalesTwentyDay;
						};
			
ics_dump_hotel = foreach ics_grp_sales generate 'TravelHotel' as FunnelName:chararray,
					version_no as VersionNo:chararray,campaign as campaign:chararray,
					datecur as date:chararray,
					segname as SegmentName:chararray,bannerclass as BannerClass:chararray,
					customization_hotel as Customization:chararray,
					HotelBasedOnDepDate as HotelBasedOnDepDate:int,HotelBasedOnDepDateNext as HotelBasedOnDepDateNext:int,
					HotelBasedOnDepDatePrev as HotelBasedOnDepDatePrev:int,offerid as OfferNumber:chararray,
					PriorityBasedHotelDepDate as PriorityBasedHotelDepDate:int,PriorityBasedHotelDepDateNext as PriorityBasedHotelDepDateNext:int,
					PriorityBasedHotelDepDatePrev as PriorityBasedHotelDepDatePrev:int,HotelId as HotelId:chararray,
					city_shown as cityShown:chararray,
					Impression as Impression:int,
					click as Click:int,sales as Sales:int,SalesOneDay as SalesOneDay:int,SalesFiveDay as SalesFiveDay:int,
					SalesTenDay as SalesTenDays:int,SalesTwentyDay as SalesTwentyDays:int;

store ics_dump_hotel into '$output_dir_hotel' using PigStorage('\t');

imprclick = join tlogic_flight by (imprid) LEFT OUTER , clickdata by (clickiid);

ics = join imprclick by (tlogic_flight::cookie,clickdata::click_ts) LEFT OUTER, salesWith_ms by (scookie,click_ts); 

-- customizing ics

-- logic for N day sales
-- oneDaySales   = filter salesWith_ms by sale_ms < 86400000L + click_ms;	
-- fiveDaySales   = filter salesWith_ms by sale_ms < 432000000L + click_ms;	
-- tenDaySales   = filter salesWith_ms by sale_ms < 864000000L + click_ms;	
-- twentyDaySales   = filter salesWith_ms by sale_ms < 1728000000L + click_ms;

ics_grp = group ics by (funnelname,version_no,campaign,datecur,segname,bannerclass,sectors,isPlaceShown,isPriceShown,customization_flight,offerid,dateMinus1,dateMinus2,date0,datePlus1,datePlus2);	

ics_grp_sales = foreach ics_grp {
						isOneDaySales = filter ics by sale_ms < click_ms + 86400000L;
						-- distODS = DISTINCT isOneDaySales;
						isFiveDaySales = filter ics by sale_ms < click_ms + 432000000L;
						-- distFDS = DISTINCT isFiveDaySales;
						isTenDaySales = filter ics by sale_ms < click_ms + 864000000L;
						-- distTDS = DISTINCT isTenDaySales;
						twentyDaySales = filter ics by sale_ms < click_ms + 1728000000L;
						-- distTWDS = twentyDaySales;
						clicks = ics.clickiid;
						sales = ics.scookie;

						generate -- group::funnelname as funnelname,
								 -- group::version_no as ver_no,
								 -- group::campaign as campaign,
								 -- group::datecur as currentdate,
								 -- group::segname as segmentname,
								 -- group::sectors as sectors,
								 -- group::isPlaceShown as isPlaceShown,
								 -- group::isPriceShown as isPriceShown,
								 -- group::customization as customizationType,
								 -- group::offerid as offerid,
								 -- group::dateMinus1 as date1,
								 -- group::dateMinus2 as date2,
								 -- group::date0 as date3,
								 -- group::datePlus1 as date4,
								 -- group::datePlus2 as date5,
								 flatten(group) as (funnelname,version_no,campaign,datecur,segname,bannerclass,sectors,isPlaceShown,isPriceShown,customization_flight,offerid,dateMinus1,dateMinus2,date0,datePlus1,datePlus2),
								 -- please specify something from Impressions table to have a count of Impr
								 COUNT(ics) as Impression,
								 COUNT(clicks) as click,
								 COUNT(sales) as sales,
								 COUNT(isOneDaySales) as SalesOneDay,
								 COUNT(isFiveDaySales) as SalesFiveDay,
								 COUNT(isTenDaySales) as SalesTenDay,
								 COUNT(twentyDaySales) as SalesTwentyDay;
						};

ics_dump_flight = foreach ics_grp_sales generate 'TravelFlight' as FunnelName:chararray,
					version_no as VersionNo:chararray,campaign as campaign:chararray,
					datecur as date:chararray,
					segname as SegmentName:chararray,bannerclass as BannerClass:chararray,
					sectors as Sectors:chararray, isPlaceShown as isPlaceShown:chararray,
					isPriceShown as isPriceShown:chararray,customization_flight as CustomizationType:chararray,
					offerid as OfferId:chararray,dateMinus2 as date1:int,dateMinus1 as date2:int,date0 as date3:int,
					datePlus1 as date4:int,datePlus2 as date5:int,Impression as Impression:int,
					click as Click:int,sales as Sales:int,SalesOneDay as SalesOneDay:int,SalesFiveDay as SalesFiveDay:int,
					SalesTenDay as SalesTenDays:int,SalesTwentyDay as SalesTwentyDays:int;

			
store ics_dump_flight into '$output_dir_flight' using PigStorage('\t');

imprclick = join tlogic_online by (imprid) LEFT OUTER , clickdata by (clickiid);

-- sales converted click_ts / sales_ts into msecs.

-- now we have imprs,clks,sales for all the three funnels together
ics = join imprclick by (tlogic_online::cookie,clickdata::click_ts)  LEFT OUTER , salesWith_ms by (scookie,click_ts); 

-- customizing ics

-- logic for N day sales
-- oneDaySales   = filter salesWith_ms by sale_ms < 86400000L + click_ms;	
-- fiveDaySales   = filter salesWith_ms by sale_ms < 432000000L + click_ms;	
-- tenDaySales   = filter salesWith_ms by sale_ms < 864000000L + click_ms;	
-- twentyDaySales   = filter salesWith_ms by sale_ms < 1728000000L + click_ms;    

ics_grp = group ics by (funnelname,version_no,campaign,datecur,segname,defaultproduct,categorydefaultproduct,productcustomization,bannerclass,subcategory1,subcategory2,productid);	

ics_grp_sales = foreach ics_grp {
						isOneDaySales = filter ics by sale_ms < click_ms + 86400000L;
						-- distODS = DISTINCT isOneDaySales;
						isFiveDaySales = filter ics by sale_ms < click_ms + 432000000L;
						-- distFDS = DISTINCT isFiveDaySales;
						isTenDaySales = filter ics by sale_ms < click_ms + 864000000L;
						-- distTDS = DISTINCT isTenDaySales;
						twentyDaySales = filter ics by sale_ms < click_ms + 1728000000L;
						-- distTWDS = twentyDaySales;
						-- clicks = ics.clickiid;
						-- sales = ics.scookie;

						generate -- group::funnelname as funnelname,
								 -- group::version_no as ver_no,
								 -- group::campaign as campaign,
								 -- group::datecur as datecur,
								 -- group::segname as segmentname,
								 -- group::defaultproduct as defaultproduct,
								 -- group::categorydefaultproduct as categorydefaultproduct,
								 -- group::isProdCust as isProductCust,
								 -- group::isCatCust as SubCategory1,
								 -- group::isSubCatCust as SubCategory2,
								 flatten(group) as (funnelname,version_no,campaign,datecur,segname,defaultproduct,categorydefaultproduct,productcustomization,bannerclass,subcategory1,subcategory2,productid),
								 -- please specify something from Impressions table to have a count of Impr
								 COUNT(ics) as Impression,
								 COUNT(ics.clickiid) as click,
								 COUNT(ics.scookie) as sales,
								 COUNT(isOneDaySales) as SalesOneDay,
								 COUNT(isFiveDaySales) as SalesFiveDay,
								 COUNT(isTenDaySales) as SalesTenDay,
								 COUNT(twentyDaySales) as SalesTwentyDay;
						};

ics_dump_online = foreach ics_grp_sales generate 'OnlineRetail' as FunnelName:chararray,
					version_no as VersionNo:chararray,campaign as campaign:chararray,
					datecur as date:chararray,
					segname as SegmentName:chararray,defaultproduct as DefaultProds:int,categorydefaultproduct as CatDefProds:int,
					productcustomization as ProdCusts:int,bannerclass as BannerClass:chararray,subcategory1 as SubCategory1:int,
					subcategory2 as SubCategory2:int,productid as ProductId:chararray, Impression as Impression:int,
					click as Click:int,sales as Sales:int,SalesOneDay as SalesOneDay:int,SalesFiveDay as SalesFiveDay:int,
					SalesTenDay as SalesTenDays:int,SalesTwentyDay as SalesTwentyDays:int;
			
store ics_dump_online into '$output_dir_online' using PigStorage('\t');
