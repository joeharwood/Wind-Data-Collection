library(RODBC)
require(lubridate)
require(ggplot2)
require(hexbin)
require(dplyr)
library(splines)
#options(scipen=999)

###ADD NEW LINE
##########################################################################################################
######                 Move old historic data to a new folder                 ############################
##########################################################################################################

root_dir <- "S:/OandT/OptRisk/Energy_Requirements/19 - Wind Energy/WPFS/Wind farm models/Historic_data/"

files <- list.files(path=root_dir, pattern="*.RDS", full.names=TRUE, recursive=FALSE)
files_name_only <- list.files(path=root_dir, pattern="*.RDS", recursive=FALSE)

date <- file.info(files[1])$ctime

old_files_date <- format(as.POSIXct(date, format='%Y/%m/%d %H:%M:%S'),format='%Y%m%d')

dir.create(paste0(root_dir, old_files_date))

for (i in 1 : length(files_name_only)){
  file.rename(from=files[i],
            to=paste0(root_dir, old_files_date, "/",files_name_only[i]))
}

##########################################################################################################
##########         Collect new historic data            ##################################################
##########################################################################################################


root_dir <- "S:/OandT/OptRisk/Energy_Requirements/19 - Wind Energy/WPFS/Wind farm models"

# check for 32 bit set up
ifelse(.Machine$sizeof.pointer == 4, "Congrats you are running 32bit", "Caution - Its a trap 64bit")

#set up connection args to DEAF
connection_args <-  list("DEAFP", "tde", "control1")

query <- "select unique(md.GEN_ID) 
from vgen_metered_data md
inner join wind_generator wg on md.gen_id = wg.gen_id
where (wg.end_date is null or wg.end_date > Sysdate);"

#set up connection to DEAF
wind_farm_list <- tbl_df(RODBC_query(connection_args, query))

#set up connection to NED

NED <- odbcDriverConnect("Driver={Oracle in OraClient11g_home1};Dbq=NEDP;Uid=caplinj;Pwd=caplinj;")


for (i in wind_farm_list) {
  tryCatch(
    {#create sql
    #change GEN_ID as appropriate
    GEN_ID_NUMBER <- i
    
    ##  Identify location id and station id, if it exists
    ##  If station id does not exist then best we have for wind speed is the most recent forecast
    qlocation <- "select l.loc_id, s.station_id
    from gen_frcstr_loc_map l left join spf_frcstr_loc_station_map s
    on l.loc_id = s.loc_id
    where l.gen_id = GEN_ID_NUMBER"
    qlocation <- gsub("GEN_ID_NUMBER", GEN_ID_NUMBER, qlocation)
    location <- sqlQuery(EFS, qlocation)
    locid <- location$LOC_ID
    stid <- location$STATION_ID
    
    query_wind_gen <- "SELECT GEN_NAME, capacity from wind_generator where GEN_ID = GEN_ID_NUMBER;"
    
    query_wind_name <- gsub("GEN_ID_NUMBER", GEN_ID_NUMBER, query_wind_gen)
    
    #obtain all metering for a GEN_ID
    gen_name_capacity <- sqlQuery(EFS, query_wind_name)
    
    gen_name_BMU <- gen_name_capacity$GEN_NAME
    capacity  <- gen_name_capacity$CAPACITY
    
    if (!is.na(stid)) {
      query_windspeed <- "select gdate, gtime, 0.514444*ws as ws from vactweather 
      where stnum = stid and gdate >= 20110101
      order by gdate, gtime"
      query_windspeed <- gsub("stid", stid, query_windspeed)
      wind <- tbl_df(sqlQuery(EFS, query_windspeed))
      wind <- wind %>% mutate(datetime = ymd_hm(10000*GDATE + GTIME, tz = 'GMT')) %>%
        select(datetime, WS)
    } else {
      query_windspeed <- "select target_datetime, frcst_datetime, F_WS_MEAN as WS
      from weather_frcst_data where loc_id = locid 
      and target_datetime > frcst_datetime and target_datetime < frcst_datetime + 1/4
      order by target_datetime"
      query_windspeed <- gsub("locid", locid, query_windspeed)
      wind <- tbl_df(sqlQuery(EFS, query_windspeed, as.is = 1:2))
      wind <- wind  %>% 
        mutate(datetime =  ymd_hms(TARGET_DATETIME, tz = "GMT"), FRCST_DATETIME =  ymd_hms(FRCST_DATETIME, tz = "GMT")) 
      wind_latest <- wind %>% group_by(datetime) %>% summarise(FRCST_DATETIME = max(FRCST_DATETIME))
      wind <- inner_join(wind, wind_latest, by = c("datetime", "FRCST_DATETIME")) %>%
        select(datetime, WS)
    }
    
    ### taken from line 84  "and target_datetime >= to_date('20-APR-18', 'DD-MON-YY')"
    
    #obtain all metering for a GEN_ID
    query_wind_gen <- "select METERED_DATETIME as datetime, GEN_MW
    from  GEN_METERED_DATA 
    where GEN_ID = GEN_ID_NUMBER
    and METERED_DATETIME  >= to_date('01-01-2011', 'DD-MM-YYYY');"
    
    query_wind_gen <- gsub("GEN_ID_NUMBER", GEN_ID_NUMBER, query_wind_gen)
    
    wind_gen <- tbl_df(sqlQuery(EFS, query_wind_gen, as.is = 1))
    wind_gen <- wind_gen %>% mutate(datetime = ymd_hms(DATETIME, tz = "GMT"), GEN_MW = pmax(GEN_MW, 0)) %>%
      select(datetime, GEN_MW)
    
    ##  Combine wind and generation
    
    metered <- inner_join(wind, wind_gen, by = "datetime") %>% mutate(gen_id = GEN_ID_NUMBER, capacity = capacity)
    
    ##  If there's duplicated data: I know there's duplicated data in vactweather
    metered <- metered %>% mutate(dummy = 1:nrow(metered))
    metered_pick <- metered %>% group_by(datetime) %>% summarise (dummy = max(dummy)) 
    metered <- inner_join(metered, metered_pick, by = c("datetime", "dummy")) %>% select(-dummy)
    
    # use diff function to compare consecutive gen_mw and remove if 0, unless it's at maximum capacity
    metered <- metered[c(TRUE, (abs(diff(metered$GEN_MW))>0 | (metered$GEN_MW == capacity)[-1])), ]
    
    ##Acquire BOA data
    query_BOA <- "select gt.gmt_prd_dtm as datetime, bm.bmu_id
    from baar_bidoffer_acceptance bo inner join bm_units bm
    on bo.bmu_id = bm.bmu_id
    inner join general_date_times gt on gt.sett_date = bo.sett_date and gt.sett_period = bo.sett_period
    where bm.fuel_i = 'WIND' and bm.bmu_id = 'gen_name_BMU' and gt.gmt_prd_dtm between bm.datetime_from and bm.datetime_to
    and gt.gmt_prd_dtm >= to_date('01-01-2011', 'DD-MM-YYYY')
    and qab < 0;"
    
    query_BOA <- gsub("gen_name_BMU", gen_name_BMU, query_BOA)
    query_BOA <- gsub("\n", " ", query_BOA)
    
    #obtain all metering for a GEN_ID
    BOA_data <- tbl_df(sqlQuery(NED, query_BOA, as.is = 1))
    if (nrow(BOA_data) > 0) {
      BOA_data <- BOA_data %>% mutate(datetime = ymd_hms(DATETIME, tz = "GMT") + minutes(30)) %>% select(datetime)
      # join- remove all entries that have a datetime in the BOA table
      metered <- anti_join(metered, BOA_data, by = "datetime")
      
      
    #obtain all metering for a GEN_ID
    query_NED_constraint <- "select 
    bmu_id, CST.CST_GROUP, CST.CVOL, CST.VWA_PB, GDT.GMT_PRD_DTM
    from VW_CST_BID_UNITS CST
    inner join GENERAL_DATE_TIMES GDT
    on CST.SETT_DATE=GDT.SETT_DATE and
    CST.SETT_PERIOD=GDT.SETT_PERIOD
    where CST.bmu_id IN (select u.bmu_id 
    from bmu_mapping_lookups m 
    inner join bm_units u
    on u.bmu_id = m.ngc_bmu_id
    where (u.fuel_i = 'WIND' and u.bmu_id <> 'RSHLW-1' )
    or  ngc_bmu_id in ('CREG00030', 'CREG00085', 'CREG00086', 'CREG00087')
    and u.datetime_from <= SYSDATE)
    and bmu_id = 'gen_name_BMU';"
    
    query_NED_constraint <- gsub("gen_name_BMU", gen_name_BMU, query_NED_constraint)
      
    NED_constraint <- tbl_df(sqlQuery(NED, query_NED_constraint, as.is = c(1)))
    
    
    NED_constraint %>% mutate(Datetime = as.POSIXct(GMT_PRD_DTM, format = "YYYY-MM-DD HH:MM:SS"))
    NED_constraint %>% mutate(Datetime = ymd_hms(GMT_PRD_DTM))
    
    #check for no constraints
    if (nrow(NED_constraint) > 0) {
      NED_constraint <- NED_constraint %>% mutate(datetime = ymd_hms(GMT_PRD_DTM, tz = "GMT") + minutes(30)) %>% select(datetime)
      # join- remove all entries that have a datetime in the BOA table
      metered <- anti_join(metered, NED_constraint, by = "datetime")
    }
    }   

    saveRDS(metered, paste0(root_dir, "/Historic_data/wind_id_", GEN_ID_NUMBER, ".RDS"))
    print(i)
  }, error = function(e){cat("Gen_ID", i , "not extracted", conditionMessage(e), "\n")})
}


        