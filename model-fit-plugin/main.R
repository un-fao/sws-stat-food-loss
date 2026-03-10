library(mgcv)
#install.packages("nimble")
library(nimble)
library(readxl)
library(magrittr)
library(abind)
library(ggthemes)
library(openxlsx)
library(RColorBrewer)
library(doParallel)
library(coda)
library(data.table)
library(ggh4x)
library(raster) 
library(ggrepel)
#library(qs)
library(float)
library(gridExtra)
#library(tidyverse)
library(dplyr)
library(tidyr)
library(tibble)
library(ggplot2)
library(readr)
library(stringr)
#library(sf) #the package rgdal has been removed from CRAN in October 2023, it is in the raster package
#and is needed for shapefile. We can use sf package instead, or terra, but i wasn't able to install them so
#I am not going to run the shapefile part of the code since it is needed to only to save the image region_map.png

library(nimble)
library(float)
library(qs2)

library(future)
library(future.apply)

library(faosws)
library(faoswsUtil)

library(countrycode) #####Still needs to be added to renv.lock


if (faosws::CheckDebug()) {
    library(faoswsModules)
    SETT <- ReadSettings("sws.yml")
    GetTestEnvironment(baseUrl = SETT[["server"]], token = SETT[["token"]])
}


R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")

#####################
#Prepare_model_data

load(file.path(R_SWS_SHARE_PATH,'Bayesian_food_loss/Inputs/input_data2.RData'))
load(file.path(R_SWS_SHARE_PATH,'Bayesian_food_loss/Inputs/FAOCrops2.RData'))
load(file.path(R_SWS_SHARE_PATH,'Bayesian_food_loss/Inputs/CropCalendar2.RData'))


years_out = 1991:2024


# Read in some required inputs.
#load("Inputs/input_data.RData")
#load("Inputs/FAOCrops.RData")
#load("Inputs/CropCalendar.RData")


# Change spelling of meats & animal products.
fbsTree1%<>%mutate(food_group=case_match(gfli_basket,
                                         "Meat & Animals Products"~"Meats & Animal Products",
                                         .default=gfli_basket))

# Split roots & tubers and oil-bearing crops.
fbsTree1$food_group[fbsTree1$foodgroupname=="2907"] <- "Roots & Tubers"
fbsTree1$food_group[fbsTree1$foodgroupname=="2913"] <- "Oil-Bearing Crops"

# Make a factor of the food group.
fbsTree1$basket_num <- as.numeric(as.factor(fbsTree1$food_group))

# Join with FAO crop information.
fbsTree1%<>%left_join(dplyr::select(FAOCrops,crop,measureditemcpc))


#########################
#First of all we obtain the countries groupings data table which we call M49
#########################
############################
# Codelist
############################
areaList = as.data.table(GetCodeList("lossWaste", "loss_sdg", "geographicAreaM49"))
setnames(areaList, tolower(names(areaList)))
areaList[, code := as.character(code)]

#name_map contains code, name, type_l
name_map = areaList[, .(
    code   = as.character(code),
    name   = description,
    type_l = tolower(type)
)]
setkey(name_map, code)

#here I extract just code and type_l so we can see what kind of node represents a certain code
type_lookup = name_map[, .(code, type_l)]
setkey(type_lookup, code)
#type_l values are "country", "region", "intermediate region", "subregion".
########################
# Trees / edges
########################
# M49 world tree (root = "1")
tree_m49 = GetCodeTree("lossWaste", "loss_sdg", "geographicAreaM49", "1")
ed_m49   = as.data.table(adjacent2edge(tree_m49))
setnames(ed_m49, c("parent","child"))


# SDG world tree (root = "1.04")
tree_sdg = GetCodeTree("lossWaste", "loss_sdg", "geographicAreaM49", "1.04")
ed_sdg   = as.data.table(adjacent2edge(tree_sdg))
setnames(ed_sdg, c("parent","child"))
#ed_sdg[, `:=`(parent = as.character(parent), child = as.character(child))]

# Parent maps 
parent_m49 = ed_m49[, .(parent = parent[1]), by = child]
setkey(parent_m49, child)

parent_sdg = ed_sdg[, .(parent = parent[1]), by = child]
setkey(parent_sdg, child)

# SDG regions under 1.04
sdg_region_codes = ed_sdg[parent == "1.04", unique(child)]
# [1] "747"  "62"   "753"  "513"  "9.04" "202"  "419"

# 
# Countries (REAL leaves in BOTH trees)
# 
# "country" according to codelist AND present as a child in both trees
dt = name_map[type_l == "country", .(m49_country_code = code, country = name)]
dt = dt[m49_country_code %chin% parent_m49$child]
dt = dt[m49_country_code %chin% parent_sdg$child]

# 
# Get sdg_region_code for each country (climb in SDG tree to one of the 7 region codes "419"  "202"  "62"   "747"  "513"  "753"  "9.04")
# 
dt[, sdg_region_code := parent_sdg[m49_country_code, parent]]

#Keep climbing up the SDG tree until your code is one of the 7 SDG region codes under 1.04
# I use 8 as a safe upper bound ( I think 4 will be sufficient)
for (i in 1:8) {
    dt[!is.na(sdg_region_code) & !(sdg_region_code %chin% sdg_region_codes), #sdg_region_code is NOT one of the 7 SDG region codes,keep climbing, replacing it with its parent
       sdg_region_code := parent_sdg[sdg_region_code, parent]]
}
dt[!(sdg_region_code %chin% sdg_region_codes), sdg_region_code := NA_character_]


# sdg_region_name (label of the 7 SDG regions under 1.04)
# name_map is keyed by code already (setkey(name_map, code))
dt[, sdg_region_name := name_map[sdg_region_code, name]]

# sanity check
dt[!is.na(sdg_region_code) & is.na(sdg_region_name), .N]#normally should be 0

# 
#  Get M49 subregion + region for each country 
#
# immediate M49 parent
dt[, m49_parent1 := parent_m49[m49_country_code, parent]]

#  build m49_subregion_code (climb until type == "sub-region")
dt[, cur := m49_parent1]
for (i in 1:8) {
    dt[!is.na(cur) & type_lookup[cur, type_l] != "sub-region",
       cur := parent_m49[cur, parent]]
}
dt[, m49_subregion_code := cur]
dt[, cur := NULL]


# remember intermediate region if present at parent1
dt[, m49_intermediate_code := fifelse(type_lookup[m49_parent1, type_l] == "intermediate region",
                                      m49_parent1, NA_character_)]

# climb to first ancestor that is an "intermediate region" (if it exists)
dt[, cur := m49_parent1]
for (i in 1:12) {
    dt[!is.na(cur) & type_lookup[cur, type_l] != "intermediate region",
       cur := parent_m49[cur, parent]]
}
dt[, m49_intermediate_code := fifelse(!is.na(cur) & type_lookup[cur, type_l] == "intermediate region",
                                      cur, NA_character_)]
dt[, cur := NULL]

# climb from subregion to first ancestor that is a "region"
dt[, cur := m49_subregion_code]
for (i in 1:8) {
    dt[!is.na(cur) & type_lookup[cur, type_l] != "region",
       cur := parent_m49[cur, parent]]
}
dt[, m49_region_code := cur]
dt[, cur := NULL]

##############################
# Build SDG L1 (hybrid buckets)
#    - Asia (142) from SDG Asia regions 62,753
#    - Europe (150) vs Northern America (21) from SDG 513 using M49
#    - Northern Africa and Western Asia (747) together at L1
#    - Oceania (9) from SDG 9.04, but Aus/NZ moved to Northern America (21) if you want that rule
#    - LAC (419), SSA (202)
#############################
dt[, sdg_subregion_l1_code := fcase(
    sdg_region_code %chin% c("62","753"), "142",                # Asia, with "62" = Central and Southern Asia (SDG region); "753" = Eastern and South-Eastern Asia (SDG region) 
    sdg_region_code == "513" & m49_region_code == "150", "150", # Europe
    sdg_region_code == "513", "21",                             # Northern America (rest of 513)
    
    sdg_region_code == "747", "747",                            # NAfrica + WAsia together at L1
    
    sdg_region_code == "9.04" & m49_subregion_code == "53", "21",  # Aus/NZ -> Northern America (your rule)
    sdg_region_code == "9.04", "543",        
    
    sdg_region_code == "419", "419",                            # LAC
    sdg_region_code == "202", "202",                            # Sub-Saharan Africa
    
    default = NA_character_
)]

# 
# Build SDG L2 (Asia must be true subregions like 34/35/30/143, etc.)
#
dt[, sdg_subregion_l2_code := fcase(
    # Asia: split by M49 subregions (34/35/30/143)
    sdg_subregion_l1_code == "142", m49_subregion_code,
    
    # Europe: split by M49 Europe subregions (151/154/155/39)
    sdg_subregion_l1_code == "150", m49_subregion_code,
    
    # 747: split into 15 vs 145 (via M49 subregion)
    sdg_subregion_l1_code == "747", m49_subregion_code,
    
    # Northern America bucket: 21 or 53
    sdg_subregion_l1_code == "21" & m49_subregion_code == "53", "53",
    sdg_subregion_l1_code == "21", "21",
    
    # Oceania bucket (L1=543): use M49 subregions under it (54/57/61)
    sdg_subregion_l1_code == "543", m49_subregion_code,
    
    # LAC: use intermediate region (5/13/29); 
    sdg_subregion_l1_code == "419", m49_intermediate_code,
    
    # Sub-Saharan Africa: use intermediate region (11/14/17/18); 
    sdg_subregion_l1_code == "202", m49_intermediate_code,
    
    default = NA_character_
)]

#  group-level fallback: only if every single country in that L1 group fails to get an intermediate region,
# then collapse L2 to the L1 code (419 or 202).
if (dt[sdg_subregion_l1_code == "419", all(is.na(m49_intermediate_code))]) {
    dt[sdg_subregion_l1_code == "419", sdg_subregion_l2_code := "419"]
}
if (dt[sdg_subregion_l1_code == "202", all(is.na(m49_intermediate_code))]) {
    dt[sdg_subregion_l1_code == "202", sdg_subregion_l2_code := "202"]
}


#  Optional guards to prevent accidental wrong codes in L2 (so if you see NA something is wrong)
# Keep Asia L2 only among these M49 subregions (Central/Eastern/SE/Southern Asia)
dt[sdg_subregion_l1_code == "142" & !(sdg_subregion_l2_code %chin% c("143","30","35","34")),
   sdg_subregion_l2_code := NA_character_]

# Keep Europe L2 only among these M49 subregions
dt[sdg_subregion_l1_code == "150" & !(sdg_subregion_l2_code %chin% c("151","154","155","39")),
   sdg_subregion_l2_code := NA_character_]

# Keep 747 L2 only Northern Africa or Western Asia
dt[sdg_subregion_l1_code == "747" & !(sdg_subregion_l2_code %chin% c("15","145")),
   sdg_subregion_l2_code := NA_character_]

# 
# Attach names
# 
dt = merge(dt, name_map[, .(code, sdg_subregion_l1 = name)],
           by.x="sdg_subregion_l1_code", by.y="code", all.x=TRUE)

dt = merge(dt, name_map[, .(code, sdg_subregion_l2 = name)],
           by.x="sdg_subregion_l2_code", by.y="code", all.x=TRUE)

# 
#  Final output
# 
final = dt[, .(
    m49_country_code, country,
    sdg_region_code, sdg_region_name,
    sdg_subregion_l1_code, sdg_subregion_l1,
    sdg_subregion_l2_code, sdg_subregion_l2
)][order(country)]

final[]

#Adding iso3 using the mapping datatable
#m49_fs_iso_mapping = as.data.table(ReadDatatable("m49_fs_iso_mapping", readOnly = FALSE))
# keep only rows that actually have an m49+iso3
#map_iso3 = m49_fs_iso_mapping[
#  !is.na(m49) & m49 != "" & !is.na(iso3) & iso3 != "",
#  .(m49_country_code = as.character(m49), iso3)
#]

#keep unique (m49,iso3) pairs
#map_iso3 = unique(map_iso3, by = c("m49_country_code", "iso3"))


#Mapping M49 to iso3 using countrycode since the m49_fs_iso_mapping datatable is not updated.
final_with_iso3 = copy(final)
final_with_iso3[, iso3c := countrycode(as.integer(m49_country_code), "un", "iso3c")]
#problem_codes = c(58, 158, 200, 230, 412, 530, 582, 680, 736, 810, 836, 890, 891)

#data.frame(
#  m49 = problem_codes,
#  name = countrycode(problem_codes, "un", "country.name")
#)

#Here I associate the iso codes from the file of the original country groupings since
#countrycode package did not have one for those M49 codes
final_with_iso3[m49_country_code=="158", iso3c := "TWN"]
final_with_iso3[m49_country_code=="412", iso3c := ""]
final_with_iso3[m49_country_code=="680", iso3c := "XSQ"]
final_with_iso3[m49_country_code=="836", iso3c := ""]
#I could not find iso3 for those countries in the original country groupings file and neither did the package:
countries_to_remove = c("58","200","230","530","582","736","810","890","891")
###Remove the other countries not present in the original countries groupings 
final_with_iso3[, m49_country_code := as.character(m49_country_code)]

final_with_iso3 = final_with_iso3[!(m49_country_code %in% countries_to_remove)]

setnames(final_with_iso3, "iso3c", "iso3")
M49 = copy(final_with_iso3)


setDT(M49)

M49 = M49[!is.na(iso3) & iso3!=""]

# aliases used by the original model code
M49[, region_l1 := sdg_subregion_l1]
M49[, region_l2 := sdg_subregion_l2]
M49[, l1_num := as.numeric(as.factor(region_l1))]
M49[, l2_num := as.numeric(as.factor(region_l2))]


# the only "M49 numeric" key we need (it matches geographicaream49 in original model_data)
M49[, m49_numeric := as.numeric(m49_country_code)]


write_excel_csv(M49,file=file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss/M49.csv"))

#we need to consider only the production
from_elem = "5510"
# Build key for production
key_prod = DatasetKey(
    domain  = "agriculture",
    dataset = "aproduction",
    dimensions = list(
        Dimension(name = "geographicAreaM49", keys = as.character(M49$m49_numeric)),
        Dimension(name = "measuredItemCPC",   keys = as.character(unique(FAOCrops$measureditemcpc))), #use the CPC items included in the model_data
        Dimension(name = "measuredElement",   keys = from_elem),
        Dimension(name = "timePointYears",    keys = as.character(years_out))
    )
)

prod_dt = as.data.table(GetData(key_prod, flags = FALSE, omitna = FALSE))

prod_support = prod_dt[, .(
    m49_numeric     = as.integer(geographicAreaM49),
    year            = as.integer(timePointYears),
    measureditemcpc = as.character(measuredItemCPC)
)]

# Prepare harvest calendar ------------------------------------------------

# Take the median harvesting end month.
crop_calendar <- CropCalendar%>%group_by(geographicaream49,measureditemcpc)%>%
    dplyr::summarise(harvesting_month_onset=median(harvesting_month_onset,na.rm=T),
                     harvesting_month_end=median(harvesting_month_end,na.rm=T))

# Create a data frame with a row for every product and country, into which
# we will fill and impute harvesting end months.
harvest_calendar_data <- tibble(measureditemcpc=rep(fbsTree1$measureditemcpc,nrow(M49)),
                                food_group=rep(fbsTree1$food_group,nrow(M49)),
                                iso3=sort(rep(M49$iso3,nrow(fbsTree1))))%>%
    left_join(distinct(select(fbsTree1,food_group,gfli_basket)))%>%
    left_join(dplyr::select(M49, iso3, m49_country_code, region_l2, sdg_region_name), by = "iso3") %>%
    mutate(geographicaream49=as.numeric(m49_country_code))%>%
    left_join(crop_calendar)

# Function to impute the harvesting end months.
impute_month <- function(x,y){
    if(!all(is.na(x$harvesting_month_end))){
        x$harvesting_month_end[is.na(x$harvesting_month_end)] <- median(x$harvesting_month_end,na.rm=T)
    }
    return(x)
}

# Impute missing harvesting end months.
harvest_calendar_imputed <- harvest_calendar_data%>%
    group_by(measureditemcpc,region_l2)%>%
    {print(mean(is.na(.$harvesting_month_end)))
        group_modify(.,impute_month)}%>%
    group_by(measureditemcpc,sdg_region_name)%>%
    {print(mean(is.na(.$harvesting_month_end)))
        group_modify(.,impute_month)}%>%
    group_by(food_group,iso3)%>%
    {print(mean(is.na(.$harvesting_month_end)))
        group_modify(.,impute_month)}%>%
    group_by(iso3)%>%
    {print(mean(is.na(.$harvesting_month_end)))
        group_modify(.,impute_month)}%>%
    ungroup()%>%
    mutate(iso3=factor(iso3,levels=M49$iso3))%>%
    select(iso3,measureditemcpc,harvesting_month_onset,harvesting_month_end)%>%
    mutate(harvesting_month_onset=floor(harvesting_month_onset),
           harvesting_month_end=ceiling(harvesting_month_end))
mean(is.na(harvest_calendar_imputed$harvesting_month_end))#0 means everything was filled


# ------------------------------------------------------------
# Prepare GDP from SWS datatable (gdp_bayesian)
# ------------------------------------------------------------
DT_gdp = as.data.table(ReadDatatable("gdp_bayesian", readOnly = TRUE))
if (is.null(DT_gdp) || !nrow(DT_gdp)) stop("gdp_bayesian is empty or not found.")

# Expecting columns we wrote: isocode, year, gdp_sm, gdp_percap
# Normalize names:
setnames(DT_gdp, tolower(names(DT_gdp)))
# We can ensure that the required columns exist
req_gdp = c("isocode","year","gdp_sm","gdp_percap")
if (!all(req_gdp %in% names(DT_gdp))) {
    stop("gdp_bayesian must contain: ", paste(req_gdp, collapse = ", "))
}

GDP_full = DT_gdp[
    isocode %in% M49$iso3 & year >= 1991,
    .(
        iso3 = as.character(isocode),
        year = as.numeric(year),
        sm_GDP_percap = as.numeric(gdp_sm),      # keep SAME name used later
        GDP_percap    = as.numeric(gdp_percap)   # keep SAME name used later
    )
]

GDP_full = as_tibble(GDP_full) %>%
    left_join(select(M49, iso3, region_l1, region_l2, country, m49_numeric), by = "iso3")

# (optional sanity check)
# stopifnot(!anyDuplicated(GDP_full %>% select(iso3, year)))




# Create a plot of smoothed GDP for each country.
# pdf("GDP_fit.pdf",width=5,height=3.5)
# for(i in 1:nrow(M49)){
#   plot_out <- ggplot(GDP_full%>%filter(country==M49$country[i]))+
#     geom_line(aes(x=year,y=exp(sm_GDP_percap)))+
#     geom_point(aes(x=year,y=GDP_percap))+
#     scale_y_continuous(labels=scales::label_comma())+
#     labs(x="Year",y="GDP per capita, PPP (current international $)",title=M49$country[i])
#   plot(plot_out)
# }
# dev.off()

GDP_full%<>%ungroup()

# Prepare climate data ----------------------------------------------------

# ------------------------------------------------------------
# Prepare climate data from SWS datatables
# temperature_bayesian: isocode, year, month, temperature_c
# rainfall_bayesian:    isocode, year, month, rainfall_mm
# ------------------------------------------------------------
DT_temp = as.data.table(ReadDatatable("temperature_bayesian", readOnly = TRUE))
DT_rain = as.data.table(ReadDatatable("rainfall_bayesian", readOnly = TRUE))

if (is.null(DT_temp) || !nrow(DT_temp)) stop("temperature_bayesian is empty or not found.")
if (is.null(DT_rain) || !nrow(DT_rain)) stop("rainfall_bayesian is empty or not found.")

setnames(DT_temp, tolower(names(DT_temp)))
setnames(DT_rain, tolower(names(DT_rain)))

req_temp = c("isocode","year","month","temperature_c")
req_rain = c("isocode","year","month","rainfall_mm")

if (!all(req_temp %in% names(DT_temp))) stop("temperature_bayesian must contain: ", paste(req_temp, collapse=", "))
if (!all(req_rain %in% names(DT_rain))) stop("rainfall_bayesian must contain: ", paste(req_rain, collapse=", "))

# Filter to the modelling years 
year_min = 1991
year_max = 2024

monthly_temp_data = as_tibble(
    DT_temp[
        isocode %in% M49$iso3 & year >= year_min & year <= year_max,
        .(iso3 = as.character(isocode),
          year = as.numeric(year),
          month = as.numeric(month),
          temperature_c = as.numeric(temperature_c))
    ]
) %>%
    group_by(iso3, month) %>%
    mutate(temperature_mean = mean(temperature_c, na.rm = TRUE)) %>%
    ungroup()

monthly_rain_data = as_tibble(
    DT_rain[
        isocode %in% M49$iso3 & year >= year_min & year <= year_max,
        .(iso3 = as.character(isocode),
          year = as.numeric(year),
          month = as.numeric(month),
          rainfall_mm = as.numeric(rainfall_mm))
    ]
) %>%
    group_by(iso3, month) %>%
    mutate(rainfall_mean = mean(rainfall_mm, na.rm = TRUE)) %>%
    ungroup()

monthly_weather_full = expand_grid(
    year  = year_min:year_max,
    iso3  = M49$iso3,
    month = 1:12
) %>%
    left_join(select(M49, iso3, region_l2), by = "iso3") %>%
    left_join(monthly_temp_data, by = c("iso3","year","month")) %>%
    left_join(monthly_rain_data, by = c("iso3","year","month")) %>%
    group_by(region_l2, year, month) %>%
    group_modify(function(x, y) {
        # fill missing by subregional means (same logic as the old code)
        x$rainfall_mm[is.na(x$rainfall_mm)] = mean(x$rainfall_mm, na.rm = TRUE)
        x$rainfall_mean[is.na(x$rainfall_mean)] = mean(x$rainfall_mean, na.rm = TRUE)
        x$temperature_c[is.na(x$temperature_c)] = mean(x$temperature_c, na.rm = TRUE)
        x$temperature_mean[is.na(x$temperature_mean)] = mean(x$temperature_mean, na.rm = TRUE)
        x
    }) %>%
    ungroup() %>%
    select(-region_l2)



# Prepare logistics performance index data ----------------------------

# ------------------------------------------------------------
# Prepare LPI from SWS datatable (lpi_bayesian)
# expecting: isocode, year, lpi_sm, lpi_raw
# ------------------------------------------------------------
DT_lpi = as.data.table(ReadDatatable("lpi_bayesian", readOnly = TRUE))
if (is.null(DT_lpi) || !nrow(DT_lpi)) stop("lpi_bayesian is empty or not found.")

setnames(DT_lpi, tolower(names(DT_lpi)))
req_lpi = c("isocode","year","lpi_sm","lpi_raw")
if (!all(req_lpi %in% names(DT_lpi))) {
    stop("lpi_bayesian must contain: ", paste(req_lpi, collapse = ", "))
}

LPI_full = DT_lpi[
    isocode %in% M49$iso3 & year >= 1991 & year <= 2024,
    .(
        iso3   = as.character(isocode),
        year   = as.numeric(year),
        sm_lpi = as.numeric(lpi_sm),
        lpi    = as.numeric(lpi_raw)
    )
]

LPI_full = as_tibble(LPI_full) %>%
    left_join(select(M49, iso3, country), by = "iso3") %>%
    # (optional) keep GDP columns if you still use them for plotting
    left_join(select(GDP_full, iso3, year, GDP_percap, sm_GDP_percap), by = c("iso3","year"))
# Plot smoothed LPI for each country.
# pdf("LPI_fit.pdf",width=5,height=3.5)
# for(i in 1:nrow(M49)){
#   plot_out <- ggplot(LPI_full%>%filter(country==M49$country[i]))+
#     geom_line(aes(x=year,y=sm_lpi))+
#     geom_point(aes(x=year,y=lpi))+
#     scale_y_continuous(labels=scales::label_comma())+
#     labs(x="Year",y="Logistics performance indicator (LPI)",title=M49$country[i])
#   plot(plot_out)
# }
# dev.off()

# Prepare model training data ---------------------------------------------

# Set up the model training data frame.
# Keep weather covariate values for comparison with the new ones.
model_data <- Data_Use_train0%>%select(geographicaream49,timepointyears,measureditemcpc,
                                       fsc_location,loss_per_clean,tag_datacollection,
                                       foodgroupname,basket_sofa_wu,
                                       temperature_c,temperature_mean,
                                       rainfall_mm,rainfall_mean,url,solr_id)%>%
    left_join(select(fbsTree1,measureditemcpc,gfli_basket,food_group))%>%#,store_min,store_max,requires_fridge,perish_class,perish_score,fridge_or_cool))%>%
    dplyr::rename(harvest_temp=temperature_c,harvest_temp_mean=temperature_mean,
                  harvest_rain=rainfall_mm,harvest_rain_mean=rainfall_mean,
                  year=timepointyears,
                  loss_percentage=loss_per_clean)%>%
    mutate(m49_numeric=as.numeric(geographicaream49))

# Label official SUA data.
model_data$tag_datacollection[model_data$tag_datacollection=="SWS_2"] <- "SUA"

# Relabel China's M49 code.
model_data$m49_numeric[model_data$m49_numeric==1248] <- 156

#Filter the data 
setDT(model_data)

setDT(model_data)
model_data[, `:=`(
    m49_numeric     = as.integer(m49_numeric),
    year            = as.integer(year),
    measureditemcpc = as.character(measureditemcpc)
)]
#keep only rows in model_data whose (m49_numeric, year, measureditemcpc) also appear in prod_support
setkey(prod_support, m49_numeric, year, measureditemcpc)
prod_support = unique(prod_support)
model_data = model_data[prod_support, on = .(m49_numeric, year, measureditemcpc), nomatch = 0]
#with nomatch = 0 we drop non-matching rows


# Join the model training data with new weather variables (matched by harvesting end month),
# GDP data, and LPI data.
model_data %<>%
    left_join(
        dplyr::select(M49, m49_numeric, iso3, region_l1, region_l2),
        by = "m49_numeric"
    ) %>%
    left_join(harvest_calendar_imputed, by = c("iso3","measureditemcpc")) %>%
    left_join(dplyr::select(GDP_full, iso3, year, GDP_percap, sm_GDP_percap), by = c("iso3","year")) %>%
    left_join(dplyr::select(LPI_full, iso3, year, sm_lpi), by = c("iso3","year")) %>%
    left_join(dplyr::select(FAOCrops, crop, measureditemcpc), by = "measureditemcpc") %>%
    left_join(monthly_weather_full, by = c("harvesting_month_end"="month","year","iso3")) %>%
    filter(year >= 1991) %>%
    group_by(iso3) %>%
    ungroup() %>%
    mutate(
        stage = case_match(fsc_location, "sws_total" ~ "primaryproduct",
                           "trader" ~ "wholesale", .default = fsc_location),
        method = case_match(tag_datacollection,
                            "SUA" ~ "Supply utilization accounts",
                            "Survey" ~ "Survey",
                            "Modelled estimates" ~ "Modelled estimates",
                            "Modelled Estimates" ~ "Modelled estimates",
                            "Literature Review" ~ "Literature review",
                            .default = "Other")
    )

# Label National Accounts data as primary product.
model_data$stage[model_data$tag_datacollection=="National Accounts"] <- "primaryproduct" 

# Relabel primary product as farm for modelling, preserving
# primary product in stage_original.
model_data$stage_original <- model_data$stage
model_data$stage[model_data$stage=="primaryproduct"] <- "farm"

# # Comparsions of new versus old weather covariate values.
# ggplot(model_data,aes(x=harvest_rain_mean,y=rainfall_mean))+
#   geom_abline()+
#   geom_point()+
#   labs(x="From old code",y="New implementation",title="Rainfall (mm)")
# 
# ggplot(model_data,aes(x=harvest_temp_mean,y=temperature_mean))+
#   geom_abline()+
#   geom_point()+
#   labs(x="From old code",y="New implementation",title="Temperature (C)")


# Plot the data source distribution before reducing low-variance time series.
model_data$datasource <- model_data$solr_id
model_data$datasource[model_data$datasource=="Statistical Working System"] <- "SWS"
model_data$datasource[model_data$datasource=="Aphlis"] <- "APHLIS"
model_data$datasource[grepl("ers.usda.gov/data-products/food", model_data$url, ignore.case = TRUE)] <- "USDA"

table(model_data$datasource)/nrow(model_data)

# ggplot(model_data%>%
#          mutate(datasource=factor(case_match(datasource,"APHLIS"~"APHLIS",
#                                              "USDA"~"USDA",
#                                              "SWS"~"SWS",
#                                              .default="Other"),
#                                   levels=rev(c("SWS","APHLIS","Other","USDA"))))%>%
#          group_by(datasource)%>%
#          dplyr::summarise(count=n()),
#        aes(x="",y=count,fill=datasource))+
#   geom_col(position = position_stack())+
#   coord_polar(theta="y")+
#   scale_fill_few(name="Data source")+
#   theme_void()

# Function to reduce low-variance data point time series.
carry_forward_remove <- function(x,y){ 
    x$year_min <- x$year
    x$year_max <- x$year
    x$year_model <- x$year
    if(nrow(x)>1){
        # If data source is APHLIS, take the mean loss percentage and keep as one data point.
        if(all(x$datasource=="APHLIS")){
            x$year_min <- min(x$year)
            x$year_max <- max(x$year)
            x$loss_percentage <- mean(x$loss_percentage)
            x <- filter(x,year==floor(median(x$year)))
            x$year_model <- NA
        }else{
            # If there are more than 2 values that are equal to the nearest 0.1% loss, 
            # take the mean loss percentage and keep as one data point.
            # This step reduces the USDA data a lot.
            unique_rounded_percentages <- unique(round(x$loss_percentage,3))
            unique_years <- unique(x$year)
            matches_counts <- table(round(x$loss_percentage,3))
            if(max(matches_counts)>=3){ # This will catch USDA data carry-forwards as well. 
                matches_counts <- matches_counts[matches_counts>=3]
                x0 <- filter(x,!round(loss_percentage,3)%in%as.numeric(names(matches_counts)))
                for(i in 1:length(matches_counts)){
                    v <- filter(x,round(loss_percentage,3)==as.numeric(names(matches_counts[i])))
                    v$year_min <- min(v$year)
                    v$year_max <- max(v$year)
                    v$loss_percentage <- mean(v$loss_percentage)
                    # We label the reduced data point with the labels (e.g. source) from the data point with the median year.
                    v <- filter(v,year==floor(median(year)))
                    v$year_model <- NA
                    x0 <- rbind(v,x0)
                }
                x <- x0
            }else{
                # Finally, if there is a set of more than 2 values with a range of less than 0.1%,
                # take the mean loss percentage and keep as one data point.
                # This step won't remove many but catches cases where similar values cross a
                # rounding discontinuity in the previous step.
                lp_range <- range(x$loss_percentage)[2]-range(x$loss_percentage)[1]
                if(nrow(x)>=3&lp_range<=1e-3){
                    x$year_min <- min(x$year)
                    x$year_max <- max(x$year)
                    x$loss_percentage <- mean(x$loss_percentage)
                    x <- filter(x,year==floor(median(year)))
                    x$year_model <- NA
                }
            }
        }
    }
    return(x)
}

# Average duplicates
model_data%<>%group_by(iso3,year,crop,stage,datasource,tag_datacollection,solr_id,
                       fsc_location)%>%
    group_modify(function(x,y){
        if(nrow(x)>1&y$datasource%in%c("USDA","APHLIS")){
            x$loss_percentage <- mean(x$loss_percentage,na.rm=T)
            x <- distinct(x)
        }
        return(x)
    })

# Reduce low-variance time series.
model_data <- model_data%>%
    group_by(iso3,crop,
             stage)%>%
    group_modify(carry_forward_remove)%>%
    group_by(iso3,crop,tag_datacollection,
             stage)%>%
    group_modify(carry_forward_remove)%>%
    group_by(iso3,crop,solr_id,
             stage)%>%
    group_modify(carry_forward_remove)


table(model_data$datasource)/nrow(model_data)
table(model_data$tag_datacollection)

# Remove a few specific India data points as instructed by Carola.
model_data%<>%filter(!(year==2001&iso3=="IND"))

# Store data points with NA covariate values (due to old country codes etc.).
excluded_data <- model_data%>%filter(is.na(iso3)|is.na(year)|is.na(rainfall_mean)|
                                         is.na(temperature_mean)|is.na(sm_GDP_percap)|is.na(sm_lpi))

# Remove data points with NA covariate values.
model_data%<>%filter(!is.na(iso3),!is.na(year),!is.na(rainfall_mean),
                     !is.na(temperature_mean),!is.na(sm_GDP_percap),
                     !is.na(sm_lpi))%>%
    ungroup()%>%
    mutate(across(where(is.character), as.factor),
           stage=relevel(stage,ref="wholesupplychain"),
           method=relevel(method,ref="Supply utilization accounts"))

# Make SWS the first source in the data source effect.
model_data$source <- factor(model_data$datasource,levels=c("SWS",levels(model_data$datasource)[levels(model_data$datasource)!="SWS"]))

# Make a lot of variables factors with carefully-specified levels.
model_data%<>%mutate(
    #we can keep iso3 but it is optional
    iso3=factor(iso3,levels=sort(unique(model_data$iso3))),
    #model country id = M49 numeric (what Nimble will use)
    country_m49 =factor(m49_numeric, levels = sort(unique(m49_numeric))), 
    region_l2=factor(region_l2,levels=sort(unique(M49$region_l2))),
    region_l1=factor(region_l1,levels=sort(unique(M49$region_l1))),
    measureditemcpc=factor(measureditemcpc,levels=sort(unique(model_data$measureditemcpc))),
    basket_country=factor(paste(food_group,m49_numeric),levels=sort(unique(paste(food_group,m49_numeric)))),
    crop_country=factor(paste(measureditemcpc,m49_numeric),levels=sort(unique(paste(measureditemcpc,m49_numeric))))
)

# Model training data row index.
model_data$row <- 1:nrow(model_data)

# Plot new data source distribution.
# ggplot(model_data%>%
#          mutate(datasource=factor(case_match(datasource,"APHLIS"~"APHLIS",
#                                              "USDA"~"USDA",
#                                              "SWS"~"SWS",
#                                              .default="Other"),
#                                   levels=rev(c("SWS","APHLIS","Other","USDA"))))%>%
#          group_by(datasource)%>%
#          summarise(count=n()),
#        aes(x="",y=count,fill=datasource))+
#   geom_col(position = position_stack())+
#   coord_polar(theta="y")+
#   scale_fill_few(name="Data source")+
#   theme_void()

# Apply windsorisation.
windsorise <- function(x,n_sd=3){
    upper_threshold <- median(x)+n_sd*sd(x)
    x[x>upper_threshold] <- upper_threshold
    return(x)
}

model_data%<>%mutate(loss_percentage_original=loss_percentage)%>%
    group_by(food_group)%>%
    group_modify(function(x,y){
        x$loss_percentage <- windsorise(x$loss_percentage,n_sd=3)
        return(x)
    })%>%
    group_by(stage_original)%>%
    group_modify(function(x,y){
        x$loss_percentage <- windsorise(x$loss_percentage,n_sd=3)
        return(x)
    })

# See how the windsorisation has changed the data.
ggplot(filter(model_data,(loss_percentage_original-loss_percentage)>0))+
    geom_point(aes(x=loss_percentage_original,y=loss_percentage,colour=food_group))
ggplot(filter(model_data,(loss_percentage_original-loss_percentage)>0))+
    geom_point(aes(x=loss_percentage_original,y=loss_percentage,colour=stage_original))

model_data%>%group_by(stage_original)%>%dplyr::summarise(mean((loss_percentage_original-loss_percentage)>0.01))
model_data%>%group_by(food_group)%>%dplyr::summarise(mean((loss_percentage_original-loss_percentage)>0.01))

# See distribution of data by method, stage, food group.
table(model_data$method)
table(model_data$method)/nrow(model_data)

table(model_data$stage_original)
table(model_data$stage_original)/nrow(model_data)



###Saving the model data and everything that will be used for predictions phase
save_dir_model_data <- file.path(R_SWS_SHARE_PATH, "Bayesian_food_loss", "Model_data")
dir.create(save_dir_model_data, recursive = TRUE, showWarnings = FALSE)

qs2::qs_save(model_data, file.path(save_dir_model_data, "model_data.qs2"))

pred_inputs <- list(
    fbsTree1 = fbsTree1,
    M49 = M49,
    harvest_calendar_imputed = harvest_calendar_imputed,
    GDP_full = GDP_full,
    LPI_full = LPI_full,
    monthly_weather_full = monthly_weather_full,
    FAOCrops = FAOCrops,
    prod_support = prod_support  #we need this to filter out the invalid combinations in the predictions phase as well
)
qs2::qs_save(pred_inputs, file.path(save_dir_model_data, "prediction_inputs.qs2"))




# Faster test model for quick experimentation -----------------------------

# test_data <- model_data%>%
#   mutate(harvest_rain_mean=as.numeric(scale(log(rainfall_mean+1))),
#          harvest_temp_mean=as.numeric(scale(temperature_mean)),
#          sm_GDP_percap=as.numeric(scale(sm_GDP_percap)),
#          sm_lpi=as.numeric(scale(sm_lpi)),
#          year=as.numeric(scale(year)),
#          url=as.factor(url))

# test_model <- bam(cloglog(loss_percentage)~
#                     year+
#                     harvest_rain_mean+
#                     harvest_temp_mean+
#                     sm_GDP_percap+
#                     sm_lpi+
#                     stage+
#                     method+
#                     s(region_l1,bs="re")+
#                     s(region_l1,by=year,bs="re")+
#                     s(region_l1,by=harvest_rain_mean,bs="re")+
#                     s(region_l1,by=harvest_temp_mean,bs="re")+
#                     s(region_l1,by=sm_GDP_percap,bs="re")+
#                     s(region_l1,by=sm_lpi,bs="re")+
#                     s(region_l2,bs="re")+
#                     s(region_l2,by=year,bs="re")+
#                     s(region_l2,by=harvest_rain_mean,bs="re")+
#                     s(region_l2,by=harvest_temp_mean,bs="re")+
#                     s(region_l2,by=sm_GDP_percap,bs="re")+
#                     s(region_l2,by=sm_lpi,bs="re")+
#                     s(food_group,bs="re")+
#                     s(food_group,by=year,bs="re")+
#                     s(food_group,by=harvest_rain_mean,bs="re")+
#                     s(food_group,by=harvest_temp_mean,bs="re")+
#                     s(food_group,by=sm_GDP_percap,bs="re")+
#                     s(food_group,by=sm_lpi,bs="re")+
#                     s(iso3,bs="re")+
#                     s(iso3,by=year,bs="re")+
#                     s(crop,bs="re")+
#                     s(crop,by=year,bs="re")+
#                     # s(iso3,food_group,bs="re")+
#                     # s(iso3,food_group,by=year,bs="re")+
#                     # s(iso3,crop,bs="re")+
#                     # s(iso3,crop,by=year,bs="re")+
#                     s(solr_id,bs="re"),
#                   data=test_data)
# summary(test_model)
# 


# Some paper plots --------------------------------------------------------
save_plots_dir <- file.path(R_SWS_SHARE_PATH, "Bayesian_food_loss", "Plots")
dir.create(save_plots_dir, recursive = TRUE, showWarnings = FALSE)
# GDP and LPI examples.
jamaica_gdp <- ggplot(GDP_full%>%filter(country=="Jamaica"))+
    geom_line(aes(x=year,y=exp(sm_GDP_percap)))+
    geom_point(aes(x=year,y=GDP_percap),colour="grey15")+
    scale_y_log10(labels=scales::label_comma())+
    theme_minimal()+
    theme(plot.title = element_text(size=unit(11,"pt")))+
    labs(x=NULL,y="Current international $",title="(a) GDP per capita, PPP")
lith_lpi <- ggplot(LPI_full%>%filter(country=="Lithuania"))+
    geom_line(aes(x=year,y=sm_lpi))+
    geom_point(aes(x=year,y=lpi),colour="grey15")+
    scale_y_continuous(labels=scales::label_comma())+
    theme_minimal()+
    theme(plot.title = element_text(size=unit(11,"pt")))+
    labs(x=NULL,y="Index value",title="(b) Logistics performance index")
ggsave(gridExtra::arrangeGrob(jamaica_gdp,lith_lpi,nrow=1),file=file.path(save_plots_dir,"covariates_example.png"),width=8,height=3.25,dpi=600)



# Region and subregion map.

# Load the shapefile.
#shape_file <- shapefile(file.path(R_SWS_SHARE_PATH,"food-loss-test/Covariates/Map shapefiles/BNDA.shp"))




###Commented from here:

# shp_path <- file.path(R_SWS_SHARE_PATH, "food-loss-test/Covariates/Map shapefiles/BNDA.shp")
# 
# shape_file <- sf::st_read(shp_path, quiet = TRUE)
# 
# # convert sf -> sp object so your existing shape_file@data + fortify() code still works
# shape_file <- as(shape_file, "Spatial")
# 
# 
# # Join map and partition by iso3 code.
# map_data <- data.frame(id=rownames(shape_file@data), NAME=shape_file@data$ROMNAM,
#                        ISO3=shape_file@data$ISO3CD, stringsAsFactors=F)%>%
#   left_join(dplyr::select(M49,iso3,region_l2,region_l1),by=c("ISO3"="iso3"))
# map_df <- fortify(shape_file)
# map_df <- left_join(map_df,map_data, by="id")
# 
# # Establish the region, subregion hierarchy.
# region_hierarchy <- select(M49,region_l1, region_l2,l1_num)%>%distinct()%>%
#   arrange(region_l1,region_l2)%>%
#   group_by(l1_num)%>%group_modify(function(x,y){
#     x$l2_num <- 1:nrow(x)
#     return(x)
#   })
# 
# # Generate color palette for regions.
# l1_palette <- brewer.pal(length(unique(region_hierarchy$region_l1)), "GnBu")
# pastel_base_colors <- c(
#   "#A8C5DB",  # soft blue
#   "#F7C8A3",  # peach
#   "#B7D6D7",  # dusty aqua
#   "#F4A6B5",  # muted pink
#   "#BFD8B8",  # sage green (less neon)
#   "#D6CDEA",  # soft lavender
#   "#FAF1B0"   # muted lemon
# )
# 
# l1_palette <- c(
#   "#6B8BA4",  # steel blue
#   
#   "#7FA5A5",  # slate teal
#   
#   "#7C9C75",  # olive green
#   "#D29060",  # warm tan
#   
# )
# 
# l1_palette <- c(
#   "#C46C7B",  # dusty rose
#   "#C4B24F",   # mustard
#   "#7FBFA8",  # dusty aqua
#   "#5DAFAD",  # deeper cyan
#   "#3996B3",  # ocean blue
#   "#A291C6",  # dusky purple
#   "#17445E"   # navy blue
# )
# 
# 
# names(l1_palette) <- unique(region_hierarchy$region_l1)
# 
# # Shades for subregions.
# region_hierarchy%<>%group_by(l1_num)%>%
#   group_modify(function(x,y){
#     x$colour <- colorRampPalette(c("white", l1_palette[y$l1_num]))(max(x$l2_num)+1)[-1]
#     return(x)
#   })
# 
# # Generate shades for L2 within each L1
# map_df%<>%left_join(dplyr::select(region_hierarchy,region_l2,region_l1,colour))%>%
#   mutate(region_l2=factor(region_l2,levels=region_hierarchy$region_l2))
# 
# colour_map <- setNames(region_hierarchy$colour, region_hierarchy$region_l2)
# 
# # Plot the map.
# region_map <- ggplot(map_df, aes(x=long, y=lat, group=group))+
#   geom_polygon(aes(fill=region_l2))+
#   scale_fill_manual(name=NULL,values=colour_map,na.value = "white",
#                     guide=guide_legend(nrow=11),
#                     na.translate = F)+
#   coord_fixed(ylim=c(-56.5,83.62295),xlim=c(-179.9,179.9),expand = FALSE)+
#   theme_void(base_size = 7)+
#   #geom_path(colour="grey10", linewidth=0.1)+
#   theme(legend.key.size = unit(0.6,"lines"),
#         panel.grid.major = element_blank(),
#         panel.grid.minor = element_blank(),
#         panel.background = element_rect(fill = "white",
#                                         colour = "white"),
#         axis.line = element_blank(),
#         plot.background = element_rect(fill="white",colour="white"),
#         plot.margin = margin(t=2.5,r=5,b=1.75,l=5))
# ggsave(region_map,file=file.path(R_SWS_SHARE_PATH,"food-loss-test/Plots/region_map.png"),width=180,height=52,units="mm",dpi=600)
# 
# # Write country grouping table.
# write_excel_csv(select(M49,country,iso3,sdg_region_name,
#                        sdg_subregion_l1,region_l1,sdg_subregion_l2),
#                 file=file.path(R_SWS_SHARE_PATH,"food-loss-test/Tables/country_grouping_table.csv"))
# 
# 
# 
# 
# 
# 



# 
# #Lyuba's checks code
# # how many unique observed combinations do you have?
# obs <- model_data |>
#   dplyr::distinct(iso3, crop, year, stage)
# 
# n_obs <- nrow(obs)
# 
# # how many combos exist if it were complete?
# n_full <- length(unique(model_data$iso3)) *
#   length(unique(model_data$crop)) *
#   length(unique(model_data$year)) *
#   length(unique(model_data$stage))
# 
# coverage <- n_obs / n_full
# coverage
# 
# #Counts per group (by counry)
# country_n <- model_data |> dplyr::count(iso3, name="n")
# summary(country_n$n)
# country_n |> dplyr::arrange(n) |> head(20)
# 
# 
# #by crop/product
# crop_n <- model_data |> dplyr::count(crop, name="n")
# summary(crop_n$n)
# crop_n |> dplyr::arrange(n) |> head(20)
# 
# 
# #By year
# year_n <- model_data |> dplyr::count(year, name="n")
# year_n |> dplyr::arrange(year)
# 
# 
# #by (country/crop) interaction 
# cc_n <- model_data |> dplyr::count(iso3, crop, name="n")
# summary(cc_n$n)
# cc_n |> dplyr::arrange(n) |> head(30)
# 
# 
# 




# Half-cauchy distribution function.
dhalf_cauchy <- nimbleFunction(run=function(x=double(0),scale=double(0),log=integer(0)){
    returnType(double(0))
    if(x<=0){
        return(-Inf)
    }else{
        return(log(2)-log(pi)+log(scale)-log(x^2+scale^2))
    }
})

# Half-cauchy simulation function.
rhalf_cauchy <- nimbleFunction(run = function(n = integer(0), scale = double(0)) {
    returnType(double(0))
    Z <- rnorm(1)  # Standard normal random variable.
    W <- rnorm(1)  # Standard normal random variable.
    X <- Z/W  # Cauchy distributed variable (location=0, scale=1).
    # Take the absolute value to get half-Cauchy and scale.
    return(abs(X)*scale)
})

P <- 6 # Number of parameters
pred_meta <- list(
    P = P,
    year_seq = 1991:2024
)
qs2::qs_save(pred_meta, file.path(save_dir_model_data, "prediction_meta.qs2"))


loss_code <- nimbleCode({
    for(i in 1:N){
        # Normal distribution for the cloglog loss percentage, 
        # with mean mu and standard deviation sigma_y.
        y[i] ~ dnorm(mu[i],sd=sigma_y)
        # Mean of the Normal.
        mu[i] <- 
            # Intercepts. 
            a1[1] + a2[region_l1[i],1] + a3[region_l2[i],1]*sigma_a3[1] + 
            a4[country[i],1]*sigma_a4[1] + 
            b1[basket[i],1] + b2[crop[i],1]*sigma_b2[1] + b3[basket_country[i],1]*sigma_b3[1] + b4[crop_country[i],1]*sigma_b4[1] + 
            # Year trends.
            (a1[2] + a2[region_l1[i],2] + a3[region_l2[i],2]*sigma_a3[2] + 
                 a4[country[i],2]*sigma_a4[2] + 
                 b1[basket[i],2] + b2[crop[i],2]*sigma_b2[2] + b3[basket_country[i],2]*sigma_b3[2] + b4[crop_country[i],2]*sigma_b4[2])*year[i] + 
            # Covariate effects.
            (a1[3] + a2[region_l1[i],3] + a3[region_l2[i],3]*sigma_a3[3] + b1[basket[i],3])*rain[i] +
            (a1[4] + a2[region_l1[i],4] + a3[region_l2[i],4]*sigma_a3[4] + b1[basket[i],4])*temp[i] +
            (a1[5] + a2[region_l1[i],5] + a3[region_l2[i],5]*sigma_a3[5] + b1[basket[i],5])*gdp[i] +
            (a1[6] + a2[region_l1[i],6] + a3[region_l2[i],6]*sigma_a3[6] + b1[basket[i],6])*lpi[i] +
            # Other effects.
            c1[stage[i]] + c2[method[i]] + c3[source[i]]*sigma_c3
    }
    
    for(p in 1:6){
        # Global effects.
        a1[p] ~ dnorm(0,sd=10)
        # Level 1 subregion effects.
        for(r in 1:N_l1) a2[r,p] ~ dnorm(0,sd=sigma_a2[p])
        # Level 2 subregion effects.
        for(r in 1:N_l2) a3[r,p] ~ dnorm(0,sd=1)
        # Basket effects.
        for(g in 1:N_basket) b1[g,p] ~ dnorm(0,sd=sigma_b1[p])
    }
    
    for(p in 1:2){
        # Country effects.
        for(c in 1:N_country) a4[c,p] ~ dnorm(0,sd=1) 
        # Crop effects.
        for(i in 1:N_crop){
            b2[i,p] ~ dnorm(0,sd=1)
        }
        # Basket-country interaction effects.
        for(i in 1:N_basket_country){
            b3[i,p] ~ dnorm(0,sd=1)
        }
        # Crop-country interaction effects.
        for(i in 1:N_crop_country){
            b4[i,p] ~ dnorm(0,sd=1)
        }
    }
    
    # Stage effects:
    # set whole supply chain as the baseline.
    c1[1] <- 0
    for(s in 2:N_stage){
        c1[s] ~ dnorm(0,sd=10)
    }
    
    # Data collection method effects:
    # set SWS as the baseline.
    c2[1] <- 0
    for(m in 2:N_method){
        c2[m] ~ dnorm(0,sd=1)
    }
    
    # Data source effect. Set SWS = 0.
    c3[1] <- 0
    for(s in 2:N_source){
        c3[s] ~ dnorm(0,sd=1)
    }
    
    # Hyper-parameters.
    sigma_y ~ dhalf_cauchy(1)
    sigma_c3 ~ dhalf_cauchy(1)
    
    for(p in 1:P){
        sigma_a2[p] ~ dhalf_cauchy(1)
        sigma_a3[p] ~ dhalf_cauchy(0.5)
        sigma_b1[p] ~ dhalf_cauchy(1)
    }
    for(p in 1:2){
        sigma_a4[p] ~ dhalf_cauchy(0.5)
        sigma_b2[p] ~ dhalf_cauchy(1)
        sigma_b3[p] ~ dhalf_cauchy(0.5)
        sigma_b4[p] ~ dhalf_cauchy(0.5)
    }
    
})

# Constants for modelling (e.g. number of data points, number of regions).
N <- nrow(model_data)
N_l1 <- length(levels(model_data$region_l1))
N_l2 <- length(levels(model_data$region_l2))
N_basket <- length(levels(model_data$food_group))
N_stage <- length(levels(model_data$stage))
N_method <- length(levels(model_data$method))
N_country <- length(levels(model_data$country_m49))
N_crop <- length(levels(model_data$crop))
N_crop_country <- length(levels(model_data$crop_country))
N_basket_country <- length(levels(model_data$basket_country))
N_source <- length(levels(model_data$source))

# NIMBLE takes two types of data: constants and data. Constants are things 
# like the number of data points or covariate values. Data are things
# that have a probability distribution in the model e.g. the transformed
# loss percentage.

loss_constants <- list(N=N,P=P,N_l1=N_l1,N_l2=N_l2,N_basket=N_basket,
                       N_stage=N_stage,N_method=N_method,
                       N_country=N_country,N_crop=N_crop,
                       N_crop_country=N_crop_country,
                       N_basket_country=N_basket_country,
                       N_source=N_source,
                       region_l1=as.numeric(model_data$region_l1),
                       region_l2=as.numeric(model_data$region_l2),
                       country=as.numeric(model_data$country_m49),
                       basket=as.numeric(model_data$food_group),
                       stage=as.numeric(model_data$stage),
                       method=as.numeric(model_data$method),
                       source=as.numeric(model_data$source),
                       crop=as.numeric(model_data$crop),
                       basket_country=as.numeric(model_data$basket_country),
                       crop_country=as.numeric(model_data$crop_country),
                       year=model_data$year,
                       rain=log(model_data$rainfall_mean+1),
                       temp=model_data$temperature_mean,
                       gdp=model_data$sm_GDP_percap,
                       lpi=model_data$sm_lpi
)

loss_data <- list(y=cloglog(model_data$loss_percentage))

# Random initial value generator function for the MCMC.
loss_init_fun <- function(seed,test_rows){
    set.seed(seed)
    inits <- list(a1=rnorm(P,0,0.1),
                  a2=matrix(rnorm(N_l1*P,0,0.01),nrow=N_l1),
                  a3=matrix(rnorm(N_l2*P,0,0.01),nrow=N_l2),
                  a4=array(rnorm(N_country*2,0,0.01),dim=c(N_country,2)),
                  b1=matrix(rnorm(N_basket*P,0,0.01),nrow=N_basket),
                  b2=matrix(rnorm(N_crop*2,0,0.01),nrow=N_crop),
                  b3=matrix(rnorm(N_basket_country*2,0,0.01),nrow=N_basket_country),
                  b4=matrix(rnorm(N_crop_country*2,0,0.01),nrow=N_crop_country),
                  c1=rnorm(N_stage),c2=rnorm(N_method),c3=rnorm(N_source),
                  sigma_a2=runif(P,0,1),     
                  sigma_a3=runif(P,0,1),     
                  sigma_a4=runif(2,0,1),
                  sigma_b1=runif(P,0,1),
                  sigma_b2=runif(2,0,1),
                  sigma_b3=runif(2,0,1),
                  sigma_b4=runif(2,0,1),
                  sigma_y=runif(1,0,1),
                  sigma_c3=runif(1,0,1),
                  y=rep(NA,N))
    inits$y[test_rows] <- rnorm(length(test_rows),0,1)
    return(inits)
}

# Function to run the model so that multiple chains (or k-folds) 
# can be run in parallel.

# The function allows us to fit the model in a cross-validation mode,
# test_row_list is the list of test data point rows for each chain/fold.
# If test_row_list is length 0, then it fits the model to the full
# data set.

loss_model_run_function <- function(x,code,constants,data,inits_list,mcmc_seeds,test_row_list,n_iter,n_burnin,n_thin){
    library(nimble)
    
    censored_data <- data
    # Set test data points as NA so the model doesn't see them.
    censored_data$y[test_row_list[[x]]] <- NA
    
    # Scale covariates based on observed data points (not test).
    scale_train_covariates <- function(u,test_rows){
        if(length(test_rows>0)){
            return((u-mean(u[-test_rows]))/sd(u[-test_rows]))
        }else{
            return((u-mean(u))/sd(u))
        }
    }
    
    # Scale all covariates.
    constants$year <- scale_train_covariates(constants$year,test_row_list[[x]])
    constants$rain <- scale_train_covariates(constants$rain,test_row_list[[x]])
    constants$temp <- scale_train_covariates(constants$temp,test_row_list[[x]])
    constants$gdp <- scale_train_covariates(constants$gdp,test_row_list[[x]])
    constants$lpi <- scale_train_covariates(constants$lpi,test_row_list[[x]])
    
    # Build the model.
    model <- nimbleModel(code,constants,censored_data,inits_list[[x]])
    
    # Compile the model.
    compiled_model <- compileNimble(model)
    
    # Choose which parameters to save samples for.
    mcmc_config <- configureMCMC(model,monitors=c("a1","a2","a3","a4",
                                                  "b1","b2","b3","b4","c1","c2","c3",
                                                  "sigma_a2","sigma_a3","sigma_a4",
                                                  "sigma_b1","sigma_b2","sigma_b3","sigma_b4",
                                                  "sigma_c3","sigma_y"),monitors2 = c("mu"),
                                 useConjugacy = FALSE)
    
    all_nodes <- model$getNodeNames()
    predictive_nodes <- all_nodes[model$predictiveNodeIDs]
    
    # Replace default variance parameter samplers with slice samplers.
    sigma_nodes <- all_nodes[grepl("^sigma",all_nodes)|grepl("^tau",all_nodes)]
    mcmc_config$removeSamplers(sigma_nodes)
    for(i in 1:length(sigma_nodes)){
        mcmc_config$addSampler(sigma_nodes[i],"slice")
    }
    
    # Use an automated-factor slice sampler for the global, regional, and food group effects.
    af_nodes <- setdiff(all_nodes[grepl("^a1",all_nodes)|grepl("^a2",all_nodes)|grepl("^b1",all_nodes)],
                        predictive_nodes)
    mcmc_config$removeSamplers(af_nodes)
    mcmc_config$addSampler(af_nodes,"AF_slice")
    
    # Build the MCMC.
    mcmc <- buildMCMC(mcmc_config)
    
    # Compile the MCMC.
    compiled_mcmc <- compileNimble(mcmc)
    
    # Run the MCMC.
    samples <- runMCMC(compiled_mcmc,niter=n_iter,nburnin = n_burnin,inits = inits_list[[x]],
                       samplesAsCodaMCMC = TRUE,nchains=1,thin=n_thin,thin2=n_thin,setSeed = mcmc_seeds[x]) #sampleAsCodaMCMC does carry over
    #the original thinning and so in the saved chains it will print thinning = 1
    
    return(samples)
}





#Fit model
library(parallel)
# Run the model -----------------------------------------------------------
# ---- Core / worker sanity check -----------------------------------------
cap_workers <- function(requested) {
    logical_cores  <- tryCatch(parallel::detectCores(logical = TRUE),  error = function(e) NA)
    physical_cores <- tryCatch(parallel::detectCores(logical = FALSE), error = function(e) NA)
    
    vals <- na.omit(c(as.integer(logical_cores), as.integer(physical_cores)))
    usable <- if (length(vals) > 0) min(vals) else 1L
    
    if (requested > usable) {
        warning(sprintf(
            "⚠ Requested %d workers but only %d cores detected. Using %d instead.",
            requested, usable, usable
        ))
        requested <- usable
    }
    as.integer(requested)
}

# Choose the number of chains to run the MCMC for.

n_chains_fit <- 4

# Choose how many workers to use (cap to machine capacity)
n_workers <- cap_workers(n_chains_fit)

set.seed(34095831)
# Choose the initial value generator seeds and MCMC seeds.
init_seeds_fit <- rbinom(n_chains_fit,1000000,runif(n_chains_fit,0,1))
mcmc_seeds_fit <- rbinom(n_chains_fit,1000000,runif(n_chains_fit,0,1))

# Generate initial values for each chain.
inits_list_fit <- lapply(1:n_chains_fit,function(x)loss_init_fun(init_seeds_fit[x],integer(0)))


RNGkind("L'Ecuyer-CMRG")
set.seed(34095831)
# Set up the parallel computing cluster (use n_workers, not n_chains_fit)
par_cluster <- makeCluster(n_workers)
parallel::clusterSetRNGStream(par_cluster, iseed = 34095831)


# 1) capture master lib paths
main_libpaths <- .libPaths()

# 2) send them to workers and set them there
clusterExport(par_cluster, "main_libpaths")
clusterEvalQ(par_cluster, { .libPaths(main_libpaths); NULL })

# 3) now nimble will load on workers
clusterEvalQ(par_cluster, { library(nimble); NULL })


# Export helper functions needed on workers
clusterExport(par_cluster, c("dhalf_cauchy", "rhalf_cauchy"))

# Run the full model for 200k iterations.
# Expected time 12 hours.
system.time({
    fit_samples_list<- parLapply(cl = par_cluster, X = 1:n_chains_fit, 
                                 fun = loss_model_run_function,
                                 code=loss_code, constants=loss_constants,
                                 data=loss_data,inits_list=inits_list_fit,
                                 mcmc_seeds=mcmc_seeds_fit,
                                 test_row_list = replicate(n_chains_fit, integer(0), simplify = FALSE),
                                 n_iter=200000,n_burnin=100000,n_thin=50) 
    #n_iter=20,n_burnin=10,n_thin=1) 
    
})

# Stop the parallel cluster. 
stopCluster(par_cluster)



# Put the results in an mcmc.list format.
fit_samples_list <- list(samples=as.mcmc.list(lapply(fit_samples_list,function(x)x$samples)),
                         samples2=as.mcmc.list(lapply(fit_samples_list,function(x)x$samples2)))

# Saving samples is recommended.
#save(fit_samples_list,file="Saved models/fit_samples_list_23_05_25.RData")



# Combine the samples from multiple chains together into a matrix.
fit_combined_samples <- do.call("rbind",fit_samples_list$samples)
n_sim_fit <- nrow(fit_combined_samples)

save_dir <- file.path(R_SWS_SHARE_PATH, "Bayesian_food_loss", "Saved_models", "mcmc_outputs_2026")
dir.create(save_dir, recursive = TRUE, showWarnings = FALSE)

# 1) Save ALL posterior draws (parameters) as a matrix (fast for prediction)
qs2::qs_save(
    fit_combined_samples,
    file = file.path(save_dir, "fit_combined_samples.qs2")
)
#fit_combined_samples <- qs2::qs_read("fit_combined_samples.qs2")
#class(fit_combined_samples)
#str(fit_combined_samples, max.level = 2)
#dim(fit_combined_samples)
# 2) (Optional) Save the chain-wise coda objects too (for diagnostics later)
qs2::qs_save(
    fit_samples_list,
    file = file.path(save_dir, "fit_samples_list_coda.qs2")
)
#fit_samples_list_coda <- qs2::qs_read("fit_samples_list_coda.qs2")
#class(fit_samples_list_coda)
#str(fit_samples_list_coda, max.level = 2)
#dim(fit_samples_list_coda)
# 3) Save factor level mappings used in the fit (CRITICAL if you rebuild model_data later)
levels_fit <- list(
    country_m49 = levels(model_data$country_m49), #M49 used by the model
    iso3 = levels(model_data$iso3), #it is optional to keep it
    region_l1 = levels(model_data$region_l1),
    region_l2 = levels(model_data$region_l2),
    food_group = levels(model_data$food_group),
    stage = levels(model_data$stage),
    method = levels(model_data$method),
    source = levels(model_data$source),
    crop = levels(model_data$crop),
    basket_country = levels(model_data$basket_country),
    crop_country   = levels(model_data$crop_country)
)
qs2::qs_save(levels_fit, file.path(save_dir, "levels_fit.qs2"))

# 4) Save scaling constants used for covariates in the fit/prediction code
# (the prediction code scales using means/sds of model_data)
scale_fit <- list(
    year_mean = mean(model_data$year), year_sd = sd(model_data$year),
    rain_mean = mean(log(model_data$rainfall_mean + 1)),
    rain_sd   = sd(log(model_data$rainfall_mean + 1)),
    temp_mean = mean(model_data$temperature_mean), temp_sd = sd(model_data$temperature_mean),
    gdp_mean  = mean(model_data$sm_GDP_percap),    gdp_sd  = sd(model_data$sm_GDP_percap),
    lpi_mean  = mean(model_data$sm_lpi),           lpi_sd  = sd(model_data$sm_lpi)
)
qs2::qs_save(scale_fit, file.path(save_dir, "scale_fit.qs2"))


#############
#############
# ONCE WE SAVE ALL THE INPUTS FOR THE PREDICTIONS PLUGIN,
# THE REST OF THE CODE FOR ASSESSING THE MCMC CONVERGENCE AND SUMMARY PLOTS
# IS RECOMMENDED TO BE EXECUTED LOCALLY, OUTSIDE OF SWS
############
############


# 
# # Check convergence of the chains.
# uni_psrf <- lapply(colnames(fit_samples_list$samples[[1]]),
#                    function(x)gelman.diag(fit_samples_list$samples[,x],
#                                           multivariate = FALSE,autoburnin=FALSE,transform=TRUE))
# 
# uni_psrf_point_estimates <- lapply(uni_psrf,function(x)x$psrf[1])%>%unlist()
# mean(uni_psrf_point_estimates<=1.05,na.rm=T)
# # Summarise and plot covariate effects ------------------------------------
# 
# # Function to summarise posteriors (tail probability is like a p-value).
# my_summary <- function(x){
#   out <- c(quantile(x,c(0.025,0.5,0.975)),2*min(mean(x<0),mean(x>0)))
#   names(out) <- c("lower_95","median","upper_95","tail_probability")
#   return(out)
# }
# 
# # Nicer stage names for plot labels.
# stage_names <- case_match(levels(model_data$stage),
#                           "wholesupplychain"~"Whole supply chain",
#                           "farm"~"Primary product/\nFarm",
#                           "primaryproduct"~"Primary product",
#                           "processing"~"Processing",
#                           "storage"~"Storage",
#                           #"trader"~"Trader",
#                           "transport"~"Transport",
#                           "wholesale"~"Wholesale")
# 
# # How samples are processed is described in detail for the stage effect
# # but the sample applies elsewhere.
# 
# ## Stage and method effects -------------------------------------
# 
# # Stage effect.
# 
# # Find the columns of the samples matrix corresponding to this effect.
# c1_samples <- fit_combined_samples[,grepl("^c1\\[",colnames(fit_combined_samples))]%>%
#   # Put them into an array.
#   array(dim=c(n_sim_fit,N_stage))
# # Apply the my_summary function across the samples.
# c1_quantiles <- apply(c1_samples,c(2),my_summary)
# 
# dimnames(c1_quantiles) <- list(statistic=c("lower_95","median","upper_95","tail_probability"),
#                                stage=stage_names)
# 
# # Coerce to a data frame and put into wide format.
# c1_quantiles%<>%as.table()%>%as.data.frame()%>%
#   pivot_wider(names_from=statistic,values_from=Freq)
# 
# # Method effect.
# 
# method_names <- case_match(levels(model_data$method),"Supply utilization accounts"~"Supply utilization\naccounts",
#                            .default=levels(model_data$method))
# 
# c2_samples <- fit_combined_samples[,grepl("^c2\\[",colnames(fit_combined_samples))]
# colnames(c2_samples) <- method_names
# c2_quantiles <- apply(c2_samples,c(2),my_summary)
# 
# 
# dimnames(c2_quantiles) <- list(statistic=c("lower_95","median","upper_95","tail_probability"),
#                                method=method_names)
# 
# c2_quantiles%<>%as.table()%>%as.data.frame()%>%
#   pivot_wider(names_from=statistic,values_from=Freq)
# 
# # Combined stage and method effect plot for the paper.
# stage_method_plot <- ggplot(full_join(c1_quantiles%>%dplyr::rename(var=stage)%>%mutate(type="(a) Supply chain stages"),
#                                       c2_quantiles%>%dplyr::rename(var=method)%>%mutate(type="(b) Data collection methods"))%>%
#                               mutate(var=factor(var,levels=c("Whole supply chain","Primary product/\nFarm","Storage","Transport",
#                                                              "Processing","Wholesale","Trader","Supply utilization\naccounts",
#                                                              "Literature review","Modelled estimates","National accounts",
#                                                              "Survey","Other"))),
#                             aes(x=var,ymin=lower_95,y=median,ymax=upper_95))+
#   facet_wrap(~type,scales="free")+
#   geom_hline(yintercept=0,linetype=2)+
#   geom_errorbar(position = position_dodge(width=1),colour="gray15")+
#   geom_point(position = position_dodge(width=1),colour="gray15")+
#   theme_minimal()+
#   theme(axis.text.x = element_text(angle = 45, hjust = 1),
#         strip.text = element_text(hjust=0,size=unit(11,"pt")))+
#   labs(y="Effect on cloglog(loss percentage)",x=NULL)
# ggsave(stage_method_plot,file=file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss/Plots/stage_method_plot.png"),width=8,height=3.75,dpi=600)
# 
# 
# ##  Global covariate effects -----------------------------------------------
# 
# a1_samples <- fit_combined_samples[,grepl("^a1\\[",colnames(fit_combined_samples))]%>%
#   array(dim=c(n_sim_fit,P))
# a1_quantiles <- apply(a1_samples,2,my_summary)
# 
# dimnames(a1_quantiles) <- list(statistic=c("lower_95","median","upper_95","tail_probability"),
#                                coefficient=c("Intercept","Year","Rain","Temperature","GDP","LPI"))
# 
# a1_quantiles%<>%as.table()%>%as.data.frame()%>%
#   pivot_wider(names_from=statistic,values_from=Freq)
# 
# # Plot global covariate effects.
# ggplot(a1_quantiles,aes(x=coefficient,ymin=lower_95,y=median,ymax=upper_95))+
#   geom_hline(yintercept=0,linetype=2)+
#   geom_errorbar(position = position_dodge(width=1))+
#   geom_point(position = position_dodge(width=1))+
#   #geom_text(aes(x=coefficient,y=-1,label=paste("p = ",round(tail_probability,2))))+
#   theme_minimal()+
#   theme(axis.text.x = element_text(angle = 45, hjust = 1))+
#   labs(x="Covariate",y="Global-level model coefficient")
# 
# 
# ## Regional and subregional covariate effects ----------------------------------------------
# 
# a2_samples <- fit_combined_samples[,grepl("^a2\\[",colnames(fit_combined_samples))]%>%
#   array(dim=c(n_sim_fit,N_l1,P))
# a2_quantiles <- apply(a2_samples,c(2,3),my_summary)
# 
# dimnames(a2_quantiles) <- list(statistic=c("lower_95","median","upper_95","tail_probability"),
#                                subregion=levels(model_data$region_l1),
#                                coefficient=c("Intercept","Year","Rain","Temperature","GDP","LPI"))
# 
# a2_quantiles%<>%as.table()%>%as.data.frame()%>%
#   pivot_wider(names_from=statistic,values_from=Freq)
# 
# # Plot regional covariate effects.
# ggplot(a2_quantiles,aes(x=subregion,ymin=lower_95,y=median,ymax=upper_95))+
#   facet_wrap(~coefficient,nrow=2,scales = "free_y")+
#   geom_errorbar(position = position_dodge(width=1))+
#   geom_point(position = position_dodge(width=1))+
#   theme(axis.text.x = element_text(angle = 45, hjust = 1))+
#   labs(x=NULL,y=NULL)
# 
# # Subregional covariate effects.
# a3_samples <- fit_combined_samples[,grepl("^a3\\[",colnames(fit_combined_samples))]%>%
#   array(dim=c(n_sim_fit,N_l2,P))
# 
# 
# ## Food group covariate effects --------------------------------------------
# 
# b1_samples <- fit_combined_samples[,grepl("^b1\\[",colnames(fit_combined_samples))]%>%
#   array(dim=c(n_sim_fit,N_basket,P))
# b1_quantiles <- apply(b1_samples,c(2,3),my_summary)
# 
# dimnames(b1_quantiles) <- list(statistic=c("lower_95","median","upper_95","tail_probability"),
#                                basket=levels(model_data$food_group),
#                                coefficient=c("Intercept","Year","Rain","Temperature","GDP per capita","LPI"))
# 
# b1_quantiles%<>%as.table()%>%as.data.frame()%>%
#   pivot_wider(names_from=statistic,values_from=Freq)
# 
# # Plot the food group covariate effects.
# ggplot(b1_quantiles,aes(x=basket,ymin=lower_95,y=median,ymax=upper_95))+
#   facet_wrap(~coefficient,nrow=2,scales = "free_y")+
#   geom_errorbar(position = position_dodge(width=1))+
#   geom_point(position = position_dodge(width=1))+
#   theme(axis.text.x = element_text(angle = 45, hjust = 1))
# 
# 
# ## Compute total effects by food group (including global effects) ------------------
# 
# # Total covariate effects by region and food group.
# # (currently not used).
# Alpha_samples <- array(NA,dim=c(n_sim_fit,N_l1,N_basket,P))
# for(i in 1:N_l1){
#   for(j in 1:N_basket){
#     for(k in 1:P){
#       Alpha_samples[,i,j,k] <- a1_samples[,k]+a2_samples[,i,k]+b1_samples[,j,k]
#     }
#   }
# }
# 
# Alpha_quantiles <- apply(Alpha_samples,c(2,3,4),my_summary)
# dimnames(Alpha_quantiles) <- list(statistic=c("lower_95","median","upper_95","tail_probability"),
#                                   subregion=levels(model_data$region_l1),
#                                   basket=levels(model_data$food_group),coefficient=c("Intercept","Year","Rain","Temperature","GDP per capita","LPI"))
# 
# Alpha_quantiles%<>%as.table()%>%as.data.frame()%>%
#   pivot_wider(names_from=statistic,values_from=Freq)
# 
# # Function to make only the first letter a capital.
# capitalise_first <- function(text) {
#   text <- tolower(text)
#   if (nchar(text) == 0) return(text)
#   paste0(toupper(substring(text, 1, 1)), substring(text, 2))
# }
# 
# # Total covariate effects by food group.
# Beta_samples <- array(NA,dim=c(n_sim_fit,N_basket,P))
# for(j in 1:N_basket){
#   for(k in 1:P){
#     Beta_samples[,j,k] <- a1_samples[,k]+b1_samples[,j,k]
#   }
# }
# 
# Beta_quantiles <- apply(Beta_samples,c(2,3),my_summary)
# dimnames(Beta_quantiles) <- list(statistic=c("lower_95","median","upper_95","tail_probability"),
#                                  basket=sapply(levels(model_data$food_group),capitalise_first),coefficient=c("Intercept","Year","Rainfall","Temperature","GDP per capita","LPI"))
# 
# Beta_quantiles%<>%as.table()%>%as.data.frame()%>%
#   pivot_wider(names_from=statistic,values_from=Freq)
# 
# # Plot total covariate effects by food group.
# food_group_coefficients <- ggplot(Beta_quantiles,aes(x=basket,ymin=lower_95,y=median,ymax=upper_95))+
#   facet_wrap(~coefficient,nrow=2,scales = "free_y")+
#   geom_hline(yintercept = 0,linetype=2)+
#   geom_errorbar(position = position_dodge(width=1),colour="grey15")+
#   geom_point(position = position_dodge(width=1),colour="grey15")+
#   #geom_point(data=Alpha_quantiles,aes(colour=subregion,shape=subregion))+
#   theme_minimal()+
#   theme(axis.text.x = element_text(angle = 45, hjust = 1))+
#   scale_colour_few(name="Region")+
#   scale_shape_manual(values=1:7)+
#   labs(x=NULL,y="Food group-level model coefficient",shape="Region")
# ggsave(food_group_coefficients,file=file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss/Plots/food_group_coefficients.png"),width=8,height=5,dpi=600)
# 
# 
# ## Data source effect ------------------------------------------------------
# 
# c3_samples <- fit_combined_samples[,grepl("^c3\\[",colnames(fit_combined_samples))]%>%
#   array(dim=c(n_sim_fit,N_source))
# c3_quantiles <- apply(c3_samples,c(2),my_summary)
# 
# dimnames(c3_quantiles) <- list(statistic=c("lower_95","median","upper_95","tail_probability"),
#                                source=levels(model_data$source))
# 
# c3_quantiles%<>%as.table()%>%as.data.frame()%>%
#   pivot_wider(names_from=statistic,values_from=Freq)%>%
#   left_join(select(model_data,source,method,tag_datacollection)%>%distinct())%>%
#   mutate(method=case_match(method,"Supply utilization accounts"~"Supply utilization\naccounts",
#                            .default =method))%>%
#   left_join(mutate(c2_quantiles,c2=median)%>%select(method,c2))%>%
#   mutate(method=factor(method,levels=levels(c2_quantiles$method)))
# 
# # Plot data source effect distribution by data method.
# # ggplot(c3_quantiles,aes(x=method,y=median+c2))+
# #   geom_boxplot(position = position_dodge(width=1))+
# #   geom_jitter(position = position_dodge(width=1))+
# #   theme_minimal()+
# #   theme(axis.text.x = element_text(angle = 45, hjust = 1))+
# #   labs(x="Method",y="Effect on loss percentage within model")
# 
# c3_quantiles%>%group_by(method)%>%summarise(m1=mean(median),m2=mean(median+c2))
# 
# 
# # In-sample model checking ------------------------------------------------
# 
# # Extract samples for the means for in-sample data and variance parameter.
# mu_samples <- do.call("rbind",fit_samples_list$samples2)%>%
#   array(dim=c(n_sim_fit,N))
# sigma_y_samples <- fit_combined_samples[,grepl("^sigma_y",colnames(fit_combined_samples))]
# 
# # Simulate posterior predictive replicates of the loss percentages.
# y_replicates <- icloglog(apply(mu_samples,2,function(x)rnorm(n_sim_fit,x,sigma_y_samples)))
# 
# 
# # Model training data row index. 
# model_data$row <- 1:nrow(model_data)
# 
# # Coerce to a data frame.
# y_replicates_df <- y_replicates%>%as.data.frame.table()%>%dplyr::rename(loss_percentage=Freq)%>%
#   dplyr::rename(sample=Var1,row=Var2)%>%
#   mutate(row=as.numeric(row))%>%
#   left_join(dplyr::rename(select(model_data,row,food_group,stage_original,loss_percentage),loss_percentage_data=loss_percentage))
# # Function to compute quantiles from 0% to 95%.
# compute_quantiles <- function(x,y){
#   return(data.frame(quantiles=seq(0.01,0.90,by=0.01),
#                     values=quantile(x$loss_percentage,seq(0.01,0.90,by=0.01))))
# }
# 
# # Compute replicate quantiles overall and by food group.
# y_replicate_quantiles_df <- full_join(
#   y_replicates_df%>%group_by(sample,food_group)%>%group_modify(compute_quantiles),
#   y_replicates_df%>%group_by(sample)%>%group_modify(compute_quantiles)%>%mutate(food_group="All products")
# )
# 
# # Compute mean and uncertainty intervals for the posterior predictive quantiles.
# y_replicate_quantiles_df%<>%group_by(food_group,quantiles)%>%dplyr::summarise(lower=quantile(values,0.025),
#                                                                               median=median(values),
#                                                                               upper=quantile(values,0.975))
# 
# # Compute the quantiles for the original data.
# y_quantiles_df <- full_join(
#   model_data%>%
#     group_by(food_group)%>%group_modify(compute_quantiles),
#   model_data%>%
#     group_by()%>%group_modify(compute_quantiles)%>%mutate(food_group="All products")
# )
# 
# y_replicate_quantiles_df%<>%left_join(y_quantiles_df)%>%
#   mutate(food_group=sapply(food_group,capitalise_first))
# 
# # # Quantile-quantile plot.
# # by_basket_QQ <- ggplot(y_replicate_quantiles_df,aes(x=values,ymin=lower,y=median,ymax=upper))+
# #   facet_wrap(~food_group,nrow=2)+
# #   geom_abline()+
# #   geom_ribbon(alpha=0.4,aes(colour=NULL))+
# #   geom_point(colour="grey15")+
# #   theme_minimal()+
# #   scale_x_continuous(labels=scales::label_percent(),breaks=c(0,0.2,0.4))+
# #   scale_y_continuous(labels=scales::label_percent(),breaks=c(0,0.2,0.4,0.6))+
# #   scale_color_few()+scale_fill_few()+
# #   guides(fill="none",colour="none")+
# #   labs(x="Observed loss percentage quantiles",y="Simulated loss percentage quantiles")
# 
# #ggsave(by_basket_QQ,file="Output plots/Model checking/quantile-quantile_plots.png",width=8,height=4,dpi=600)
# 
# 
# # How important is each effect --------------------------------------------
# 
# # Just looking at variance parameters out of curiosity.
# sigmas <- colnames(fit_combined_samples[,grepl("^sigma",colnames(fit_combined_samples))])
# round(sapply(sigmas,function(x)my_summary(fit_combined_samples[,x]))[2,],2)
# 
# sigma_a3 <- fit_combined_samples[,grepl("^sigma_a3",colnames(fit_combined_samples))]
# 
# # Posterior predictive model fit variance.
# mu_variance <- apply(mu_samples,1,var)
# 
# # Posterior predictive loss percentage variance.
# y_variance <- mu_variance + sigma_y_samples^2
# 
# 
# # Compute the covariate effects at different levels.
# covar_effects <- array(0,dim=c(n_sim_fit,N,9,5))
# covar_values <- cbind(1,scale(loss_constants$year),scale(loss_constants$rain),
#                       scale(loss_constants$temp),scale(loss_constants$gdp),
#                       scale(loss_constants$lpi))
# for(j in 1:N){
#   for(k in 1:6){
#     covar_effects[,j,k,1] <- a1_samples[,k]*covar_values[j,k] # Global.
#     covar_effects[,j,k,2] <- a2_samples[,loss_constants$region_l1[j],k]*covar_values[j,k] # Regional.
#     covar_effects[,j,k,3] <- a3_samples[,loss_constants$region_l2[j],k]*sigma_a3[,k]*covar_values[j,k] # Subregional.
#     covar_effects[,j,k,4] <- b1_samples[,loss_constants$basket[j],k]*covar_values[j,k] # Food group.
#   }
#   covar_effects[,j,7,1] <- c1_samples[,loss_constants$stage[j]] # Stage effect.
#   covar_effects[,j,8,1] <- c2_samples[,loss_constants$method[j]] # Method effect.
#   for(k in 1:8){
#     covar_effects[,j,k,5] <- rowSums(covar_effects[,j,k,1:4]) # Total effect of each covariate.
#   }
#   for(l in 1:5){
#     covar_effects[,j,9,l] <- rowSums(covar_effects[,j,,l]) # Total effect at each level.
#   }
# }
# 
# 
# # Function to compute pseudo p-values (not currently used).
# pseudo_p_values <- apply(covar_effects,c(2,3),function(x){
#   if(all(x==0)){
#     return(NA)
#   }else{
#     return(2*min(mean(x<0),mean(x>0)))
#   }
# })
# 
# # Compute percentage of variance explained by covariate/model effects
# # at different levels.
# variance_explained <- array(dim=c(n_sim_fit,9,5,2))
# for(k in 1:9){
#   for(l in 1:5){
#     variance_explained[,k,l,1] <- apply(covar_effects[,,k,l],1,var)/y_variance
#     variance_explained[,k,l,2] <- apply(covar_effects[,,k,l],1,var)/mu_variance
#   }
# }
# 
# # Median posterior predictive variance explained.
# variance_explained_medians <- array(apply(variance_explained,c(2,3,4),median),dim=c(9,5,2),dimnames = 
#                                       list(Covariate=c("Intercept","Year","Rainfall","Temperature","GDP per capita","LPI",
#                                                        "Stage","Data method","Total"),
#                                            Level=c("Global","Region","Subregion","Food group","Total"),
#                                            Type=c("All variance","Model fit variance")))
# 
# # Table for the paper.
# variance_explained_medians%<>%as.data.frame.table()%>%dplyr::rename(R2=Freq)%>%
#   mutate(R2=round(R2,3))%>%
#   pivot_wider(names_from = Covariate,values_from = R2)%>%
#   arrange(Type,Level)
# 
# write_excel_csv(variance_explained_medians,file=file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss/Tables/variance_explained.csv"))
# 
# # Variance explained by data source.
# (apply(sapply(1:N,function(j)c3_samples[,loss_constants$source[j]]),1,var)/
#     y_variance)%>%median()
# 
# # Residual variance term.
# median(sigma_y_samples^2/y_variance)
# 
# 
# 
# 
# 
