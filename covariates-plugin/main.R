# json_path = "Covariates_2026/cckp_cru_ts4.09_tas_pr_1901_2024.json"
# # 1) The API URL 
# url = "https://cckpapi.worldbank.org/api/v1/cru-x0.5_timeseries_tas,pr_timeseries_monthly_1901-2024_mean_historical_cru_ts4.09_mean/global_countries?_format=json"
# 
# #pkgs = c("curl", "jsonlite")
# library("curl")
# library("jsonlite")
# dir.create(dirname(json_path), recursive = TRUE, showWarnings = FALSE)
# curl_download(url, destfile = json_path, mode = "wb")
# 
# # Read 
# x = fromJSON(json_path, simplifyVector = FALSE)
# 
# #Quick sanity check: show top-level names
# cat("Top-level fields in JSON:\n")
# print(names(x))
# 
# #Create two separate datatables (for temperature and precipitations)
# 
# #Extract lists by variable
# tas_all = x$data$tas
# pr_all = x$data$pr
# 
# #Identify geocodes available in both
# geos = intersect (names(x$data$tas), names(x$data$pr))
# 
# 


#########################################################
#########################################################
#Temperature and Precipitations
#########################################################
#########################################################
suppressPackageStartupMessages({
    library(faosws)
    library(data.table)
    library(faoswsUtil)
})


if (faosws::CheckDebug()) {
    library(faoswsModules)
    SETT <- ReadSettings("sws.yml")
    GetTestEnvironment(baseUrl = SETT[["server"]], token = SETT[["token"]])
}

library("mgcv")
library("WDI")
library("curl")
library("jsonlite")
library("openxlsx")
library("dplyr")
library("tidyr")
library("tibble")
library("countrycode")


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

start_year = as.numeric(ifelse(
    is.null(swsContext.computationParams$start_year),
    "1991",
    swsContext.computationParams$start_year
))

end_year = as.numeric(ifelse(
    is.null(swsContext.computationParams$end_year),
    "2024",
    swsContext.computationParams$end_year
))

years_out = start_year:end_year



R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")


out_dir <- file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss","Raw_covariates")
dir.create(out_dir, recursive = "TRUE", showWarnings = FALSE)


###############
#Weather covariates
###############

#The API URL 
url <- "https://cckpapi.worldbank.org/api/v1/cru-x0.5_timeseries_tas,pr_timeseries_monthly_1901-2024_mean_historical_cru_ts4.09_mean/global_countries?_format=json"



#Call the API and fetch the response into memory
resp = curl_fetch_memory(url)
# Convert the raw bytes from the HTTP response into a character string
txt  = rawToChar(resp$content)
#Parse the JSON text into an R object (nested list)
# simplifyVector = FALSE keeps the structure as lists 
x    = fromJSON(txt, simplifyVector = FALSE)

#Extract lists by variable
tas_all = x$data$tas
pr_all  = x$data$pr
#Identify geocodes available 
#codes = intersect(names(tas_all), names(pr_all)) #available in both
codes_tas <- names(tas_all)
codes_pr  <- names(pr_all)
#Build a wide table: country code plus one column per month

#Temperature wide table
tas_rows = lapply(codes_tas, function(code) {
    v <- unlist(tas_all[[code]])      # named vector: "YYYY-MM" -> value
    row = as.list(as.numeric(v))     # values as a list (so it can become columns)
    names(row) = names(v)            # column names are the months
    row$code = code                  # add code
    row
})

dt_tas_wide = rbindlist(tas_rows, fill = TRUE, use.names = TRUE)

# put 'code' first, then month columns sorted
tas_month_cols = sort(setdiff(names(dt_tas_wide), "code"))
setcolorder(dt_tas_wide, c("code", tas_month_cols))

dt_tas_wide[1:5, 1:12, with=FALSE]

###################
#Saving the raw temperature data
###################
# month columns look like "1901-01", "1901-02", ...
month_cols = setdiff(names(dt_tas_wide), "code")

min_year = min(as.integer(substr(month_cols, 1, 4)))
max_year = max(as.integer(substr(month_cols, 1, 4)))

out_file_tas = file.path(out_dir, sprintf("temperature_tas_%d_%d.xlsx", min_year, max_year))


write.xlsx(
    x = as.data.frame(dt_tas_wide),
    file = out_file_tas,
    overwrite = TRUE
)


# dt_tas_wide has: code, 1901-01, 1901-02, ...
dt_tas_long = melt(
    dt_tas_wide,
    id.vars = "code",
    variable.name = "year_month",
    value.name = "tas"
)[order(code, year_month)]

# rename if you want exactly "country_code"
setnames(dt_tas_long, "code", "isocode")
setnames(dt_tas_long, "tas", "temperature_c")
setnames(dt_tas_long, "year_month", "time_period")

dt_tas_long[1:20]


# DT_temperature = as.data.table(ReadDatatable("mean_surface_air_temperature", readOnly = FALSE))
# if (is.null(DT_temperature) || !nrow(DT_temperature)) {
#     message("  (empty or not found)"); next
# }

# ch = Changeset("mean_surface_air_temperature", type = "write")
# AddDeletions(ch, DT_temperature)             
# AddInsertions(ch, dt_tas_long) 
# Finalize(ch)
# message("✅ Done ")

#If we need to place Years and months in two separate columns
dt_tas_long_2 = copy(dt_tas_long)
dt_tas_long_2[, time_period := as.character(time_period)]
dt_tas_long_2[, c("year", "month") := tstrsplit(time_period, "-", fixed=TRUE)]
dt_tas_long_2[, `:=`(year = as.integer(year), month = as.integer(month))]
dt_tas_long_2[, time_period := NULL]
setcolorder(dt_tas_long_2, c("isocode", "year", "month", "temperature_c"))

DT_temperature_2 = as.data.table(ReadDatatable("temperature_bayesian", readOnly = FALSE))
if (is.null(DT_temperature_2)) stop("Datatable 'temperature_bayesian' not found.")

ch_2 = Changeset("temperature_bayesian", type = "write")
AddDeletions(ch_2, DT_temperature_2)             
AddInsertions(ch_2, dt_tas_long_2) 
Finalize(ch_2)
message("✅ Done ")


#Precipitations wide table
pr_rows = lapply(codes_pr, function(code) {
    v = unlist(pr_all[[code]])      # named vector: "YYYY-MM" -> value
    row = as.list(as.numeric(v))     # values as a list (so it can become columns)
    names(row) = names(v)            # column names are the months
    row$code = code                  # add code
    row
})

dt_pr_wide = rbindlist(pr_rows, fill = TRUE, use.names = TRUE)

# put 'code' first, then month columns sorted
pr_month_cols = sort(setdiff(names(dt_pr_wide), "code"))
setcolorder(dt_pr_wide, c("code", pr_month_cols))

dt_pr_wide[1:5, 1:12, with=FALSE]



###################
#Saving the raw precipitations data
###################
# month columns look like "1901-01", "1901-02", ...
month_cols = setdiff(names(dt_pr_wide), "code")

min_year = min(as.integer(substr(month_cols, 1, 4)))
max_year = max(as.integer(substr(month_cols, 1, 4)))

out_file_pr = file.path(out_dir, sprintf("precipitations_%d_%d.xlsx", min_year, max_year))


write.xlsx(
    x = as.data.frame(dt_pr_wide),
    file = out_file_pr,
    overwrite = TRUE
)

#If we want the time period in one column of the SWS datatable
dt_pr_long = melt(
    dt_pr_wide,
    id.vars = "code",
    variable.name = "year_month",
    value.name = "pr"
)[order(code, year_month)]

setnames(dt_pr_long, "code", "isocode")
setnames(dt_pr_long, "pr", "rainfall_mm")
setnames(dt_pr_long, "year_month", "time_period")

dt_pr_long[1:20]
# 
# DT_precipitations = as.data.table(ReadDatatable("rainfall_bayesian", readOnly = FALSE))
# if(is.null(DT_precipitations) || !nrow(DT_precipitations)){
#     message("(empty or not found"); next
# }
# 
# ch = Changeset ("rainfall_bayesian", type = "write")
# AddDeletions (ch, DT_temperature)
# AddInsertions(ch, dt_pr_long)
# Finalize(ch)
# message("Done")

#If we need to place Years and months in two separate columns
dt_pr_long_2 = copy(dt_pr_long)
dt_pr_long_2[, time_period := as.character(time_period)]
dt_pr_long_2[, c("year", "month") := tstrsplit(time_period, "-", fixed = TRUE)]
dt_pr_long_2[, `:=`(year = as.integer(year), month = as.integer(month))]
dt_pr_long_2[, time_period := NULL]
setcolorder(dt_pr_long_2, c("isocode", "year", "month", "rainfall_mm"))

DT_precipitations_2 = as.data.table(ReadDatatable("rainfall_bayesian", readOnly = FALSE))
if (is.null(DT_precipitations_2)) stop("Datatable 'rainfall_bayesian' not found.")


ch_2 = Changeset("rainfall_bayesian", type = "write")
AddDeletions(ch_2, DT_precipitations_2)             
AddInsertions(ch_2, dt_pr_long_2) 
Finalize(ch_2)
message("✅ Done ")


# #Only if we want to save them locally
# json_path = "Covariates_2026/cckp_cru_ts4.09_tas_pr_1901_2024.json"
# 
# cat("TAS:", nrow(dt_tas_wide), "rows x", ncol(dt_tas_wide), "cols\n")
# cat("PR :", nrow(dt_pr_wide),  "rows x", ncol(dt_pr_wide),  "cols\n")
# 
# tas_xlsx <- sub("\\.json$", "_tas_wide.xlsx", json_path)
# pr_xlsx  <- sub("\\.json$", "_pr_wide.xlsx",  json_path)
# 
# openxlsx::write.xlsx(dt_tas_wide, tas_xlsx, overwrite = TRUE)
# openxlsx::write.xlsx(dt_pr_wide,  pr_xlsx,  overwrite = TRUE)
# 
# cat("Saved:\n", tas_xlsx, "\n", pr_xlsx, "\n", sep = "")


########################################################
########################################################
# GDP per capita PPP (current international $)
########################################################
########################################################
# Use the modeling country list 
iso_keep = unique(na.omit(M49$iso3))
#First of all we download the data from World bank
WDIsearch("gdp.*capita")

# All countries, all years available (1960 → latest)
gdp_pc_ppp = WDI(
    country   = "all",
    indicator = "NY.GDP.PCAP.PP.CD",
    extra     = TRUE
)

head(gdp_pc_ppp)



ind = "NY.GDP.PCAP.PP.CD"


yrs = sort(unique(as.integer(gdp_pc_ppp$year)))

gdp_pc_ppp_table = gdp_pc_ppp %>%
    mutate(year = as.integer(year)) %>%
    transmute(
        `Country Name`   = country,
        `Country Code`   = if_else(iso3c == "", iso2c, iso3c),
        `Indicator Name` = "GDP per capita, PPP (current international $)",
        `Indicator Code` = ind,
        year,
        value = .data[[ind]]
    ) %>%
    pivot_wider(names_from = year, values_from = value)


gdp_pc_ppp_table = gdp_pc_ppp_table %>%
    select(`Country Name`, `Country Code`, `Indicator Name`, `Indicator Code`,
           all_of(as.character(yrs)))


ncol(gdp_pc_ppp_table)
names(gdp_pc_ppp_table)[1:12]         # should show 4 headers + first years
gdp_pc_ppp_table[1:15, 1:10]

# years columns 
years_cols = setdiff(names(gdp_pc_ppp_table), c("Country Code", "Country Name", "Indicator Name",
                                                "Indicator Code"))

min_year = min(as.integer(years_cols))
max_year = max(as.integer(years_cols))

out_file_gdp_wb = file.path(out_dir, sprintf("gdp_wb_%d_%d.xlsx", min_year, max_year))


write.xlsx(
    x = as.data.frame(gdp_pc_ppp_table),
    file = out_file_gdp_wb,
    overwrite = TRUE
)

#Derive the ISO3 list from World Bank
iso_keep_wb = gdp_pc_ppp %>% 
    dplyr::filter(!is.na(iso3c), iso3c != "", region != "Aggregates") %>%
    dplyr::distinct(iso3c) %>%
    dplyr:: pull(iso3c)



#WB GDP long
WB_GDP = gdp_pc_ppp %>% 
    transmute(iso3 = iso3c,
              year = as.numeric(year),
              wb_gdp = .data[[ind]],
              wb_region = region
    ) %>% 
    filter(!is.na(iso3), iso3 != "", wb_region != "Aggregates") %>% 
    filter(iso3 %in% iso_keep, year >= 1990) %>%   # like in the covariates preparation in the original main
    select(iso3, year, wb_gdp)

# wb_region_map = WB_GDP %>% 
#     group_by(iso3) %>% 
#     summarise(wb_region = first(na.omit(wb_region)), .groups = "drop")


########################################
########################################
base = "https://www.imf.org/external/datamapper/api/v1"

# helper: fetch + parse JSON using curl (wget-like headers)
imf_get_json = function(url, timeout_sec = 60) {
    h <- new_handle()
    handle_setopt(
        h,
        useragent      = "Wget/1.21.3",
        followlocation = TRUE,
        timeout        = timeout_sec
    )
    handle_setheaders(
        h,
        "Accept"  = "application/json, text/plain, */*",
        "Referer" = "https://www.imf.org/external/datamapper/"
    )
    
    res = curl_fetch_memory(url, handle = h)
    txt = rawToChar(res$content)
    
    if (res$status_code != 200) {
        stop("HTTP ", res$status_code, " for ", url, "\n", substr(txt, 1, 500))
    }
    
    fromJSON(txt, simplifyVector = FALSE)
}

#country catalog 
countries_raw = imf_get_json(paste0(base, "/countries"))$countries

get_label = function(x) {
    lab = x$label
    if (is.null(lab) || length(lab) == 0 || is.na(lab)) return(NA_character_)
    as.character(lab[1])
}

countries = tibble(
    iso3    = names(countries_raw),
    country = vapply(countries_raw, get_label, character(1))
) %>% filter(!is.na(country))

#download ALL areas for an indicator (ALL years if years=NULL) 
imf_indicator_all = function(indicator, years = NULL) {
    url = paste0(base, "/", indicator)
    if (!is.null(years)) {
        url = paste0(url, "?periods=", paste(years, collapse = ","))
    }
    
    x = imf_get_json(url)
    vals = x$values[[indicator]]
    
    bind_rows(lapply(names(vals), function(a) {
        ya = vals[[a]]
        tibble(
            iso3  = a,
            year  = as.integer(names(ya)),
            value = as.numeric(unlist(ya, use.names = FALSE))
        )
    }))
}

#Get ALL values for PPPPC (countries + regions + groups) for ALL available years
ppppc_all_areas = imf_indicator_all("PPPPC")

#Keep only countries 
ppppc_countries_long = ppppc_all_areas %>%
    semi_join(countries, by = "iso3") %>%
    left_join(countries, by = "iso3") %>%
    arrange(country, year)

ppppc_countries_long


# IMF PPPPC: long -> wide (years as columns) 
imf_ind = "PPPPC"

imf_ppppc_wide = ppppc_countries_long %>%
    transmute(
        `Country Code`   = iso3,
        `Indicator Name` = "GDP per capita, PPP (current international $) [IMF PPPPC]",
        `Indicator Code` = "PPPPC",
        year             = as.integer(year),
        value            = as.numeric(value)
    ) %>%
    tidyr::pivot_wider(names_from = year, values_from = value) %>%
    dplyr::arrange(`Country Code`)

meta_cols  = c("Country Code","Indicator Name","Indicator Code")
years_cols = setdiff(names(imf_ppppc_wide), meta_cols)

min_year = min(as.integer(years_cols))
max_year = max(as.integer(years_cols))

out_file_imf = file.path(out_dir, sprintf("gdp_imf_ppppc_%d_%d.xlsx", min_year, max_year))
write.xlsx(
    as.data.frame(imf_ppppc_wide), 
    out_file_imf, 
    overwrite = TRUE
)

#IMF GDP long 
IMF_GDP = ppppc_countries_long %>%
    transmute(
        iso3    = iso3,
        year    = as.numeric(year),
        imf_gdp = as.numeric(value)
    ) %>%
    filter(iso3 %in% iso_keep, year %in% years_out)


# Merge
GDP_full = expand_grid(iso3 = iso_keep, year = years_out) %>%
    left_join(IMF_GDP %>% select(iso3, year, imf_gdp), by = c("iso3","year")) %>%
    left_join(WB_GDP  %>% select(iso3, year, wb_gdp),  by = c("iso3","year")) %>%
    left_join(M49, by = "iso3")


missing_some = (GDP_full%>%group_by(country)%>%summarise(n_missing=sum(is.na(imf_gdp)))%>%
                    filter(n_missing>5))$country
#  WB first, fill missing with IMF
GDP_full$GDP_percap = GDP_full$wb_gdp
GDP_full$source = "World Bank"
use_imf = is.na(GDP_full$wb_gdp) & !is.na(GDP_full$imf_gdp)
GDP_full$GDP_percap[use_imf] = GDP_full$imf_gdp[use_imf]
GDP_full$source[use_imf] = "IMF"

#  Smooth (LOG scale)
GDP_full$sm_GDP_percap = NA_real_

# Manual imputation for South Sudan, blends Sudan and South Sudan data pre first SSD obs
first_SSD = (dplyr::filter(GDP_full, iso3 == "SSD", !is.na(GDP_percap)) %>% dplyr::arrange(year))[1, c("year","GDP_percap")]
impute_SSD_years = 1991:first_SSD$year
SSD_weights = seq(0, 1, length = length(impute_SSD_years))
GDP_full$GDP_percap[GDP_full$iso3 == "SSD" & GDP_full$year %in% impute_SSD_years] =
    first_SSD$GDP_percap * SSD_weights + dplyr::filter(GDP_full, iso3 == "SDN", year %in% impute_SSD_years)$GDP_percap * (1 - SSD_weights)



for (iso_i in iso_keep) {
    
    subsetted_df = GDP_full %>%
        filter(iso3 == iso_i,
               !year %in% c(2020, 2021))
    
    n_gdp = sum(!is.na(subsetted_df$GDP_percap))
    
    if (n_gdp > 0) {
        
        gdp_model = gam(
            log(GDP_percap) ~ s(year, k = ceiling(n_gdp / 4), bs = "cr", m = 1),
            data   = subsetted_df,
            method = "REML"
        )
        
        GDP_full$sm_GDP_percap[GDP_full$iso3 == iso_i] <-
            predict(gdp_model,
                    GDP_full[GDP_full$iso3 == iso_i, ],
                    type = "response")
    }
}

# Impute missing smoothed GDP by M49 region_l2
####################################
GDP_full = GDP_full %>%
    group_by(region_l2, year) %>%
    group_modify(function(x, y) {
        mu = mean(x$sm_GDP_percap, na.rm = TRUE)
        if (is.finite(mu)) x$sm_GDP_percap[is.na(x$sm_GDP_percap)] <- mu
        x
    }) %>%
    ungroup()


global_mean = mean(GDP_full$sm_GDP_percap, na.rm = TRUE)
#GDP_full$sm_GDP_percap[is.na(GDP_full$sm_GDP_percap)] <- global_mean

# Final LONG output to save: isocode, year, gdp (= LOG-smoothed)
dt_gdp_long = as.data.table(
    GDP_full %>%
        transmute(
            isocode    = iso3,
            year       = as.integer(year),
            gdp_sm        = as.numeric(sm_GDP_percap),
            gdp_percap = as.numeric(GDP_percap)
        )
)
setcolorder(dt_gdp_long, c("isocode","year","gdp_sm","gdp_percap"))

DT_gdp = as.data.table(ReadDatatable("gdp_bayesian", readOnly = FALSE))
if (is.null(DT_gdp)) stop("Datatable 'gdp_bayesian' not found.")

ch_gdp = Changeset("gdp_bayesian", type = "write")
AddDeletions(ch_gdp, DT_gdp)
AddInsertions(ch_gdp, dt_gdp_long)
Finalize(ch_gdp)
message("✅ Done ")




##############################################
# Downloading the LPI data from World Bank site
##############################################
lpi_ind = "LP.LPI.OVRL.XQ"
lpi = WDI(
    country   = "all",
    indicator = "LP.LPI.OVRL.XQ",
    extra     = TRUE
)

#Sort by country + year (long format)
lpi_sorted = lpi %>%
    mutate(year = as.integer(year)) %>%
    arrange(country, year)

head(lpi_sorted)



# all years present in the downloaded data (includes NA years)
yrs = sort(unique(as.integer(lpi_sorted$year)))

lpi_wide_all_years = lpi_sorted %>%
    transmute(
        `Country Name` = country,
        `Country Code` = if_else(iso3c == "", iso2c, iso3c),
        `Indicator Code` = lpi_ind,
        year = as.integer(year),
        value = .data[[lpi_ind]]
    ) %>%
    pivot_wider(names_from = year, values_from = value) %>%
    select(`Country Name`, `Country Code`, `Indicator Code`,
           all_of(as.character(yrs))) %>%
    arrange(`Country Name`)

lpi_wide_all_years[1:15, 1:10]

###################
#Saving the raw lpi data
###################
# month columns look like "1901-01", "1901-02", ...
month_cols = setdiff(names(lpi_wide_all_years), c("Country Name", "Country Code", "Indicator Code") )

min_year = min(as.integer(substr(month_cols, 1, 4)))
max_year = max(as.integer(substr(month_cols, 1, 4)))

out_file_lpi = file.path(out_dir, sprintf("lpi_%d_%d.xlsx", min_year, max_year))


write.xlsx(
    x = as.data.frame(lpi_wide_all_years),
    file = out_file_lpi,
    overwrite = TRUE
)

meta = c("Country Name","Country Code","Indicator Code")

years_with_data = names(lpi_wide_all_years)[
    colSums(!is.na(lpi_wide_all_years[ , setdiff(names(lpi_wide_all_years), meta)])) > 0
]

years_with_data




#Make LPI long (country-year), exclude aggregates

LPI_data = lpi %>%
    mutate(year = as.integer(year)) %>%
    transmute(
        iso3 = iso3c,
        year = year,
        lpi  = .data[[lpi_ind]],
        wb_region = region
    ) %>%
    filter(!is.na(iso3), iso3 != "", wb_region != "Aggregates") %>%
    select(-wb_region)

# Use the same country list as the GDP table 
iso_keep = sort(unique(GDP_full$iso3))

# Build full grid and join LPI + GDP 
LPI_full = expand_grid(year = years_out, iso3 = iso_keep) %>%
    left_join(LPI_data, by = c("iso3","year")) %>%
    left_join(
        GDP_full %>% select(iso3, year, sm_GDP_percap),
        by = c("iso3","year")
    ) %>%
    mutate(
        iso3 = factor(iso3, levels = iso_keep),
        year_sc = as.numeric(scale(year)),
        sm_GDP_percap_sc = as.numeric(scale(sm_GDP_percap))
    )

#  Smooth LPI 
lpi_model = bam(
    lpi ~ s(year_sc, k = 7) + s(sm_GDP_percap_sc) +
        s(iso3, bs = "re") + s(iso3, bs = "re", by = year_sc),
    data = LPI_full,
    drop.unused.levels = FALSE
)

LPI_full$sm_lpi <- predict(lpi_model, LPI_full)

# Save long: isocode, year, lpi_sm (= smoothed), lpi_raw (= original)
dt_lpi_long = as.data.table(
    LPI_full %>%
        transmute(
            isocode  = as.character(iso3),
            year     = as.integer(year),
            lpi_sm   = as.numeric(sm_lpi),
            lpi_raw  = as.numeric(lpi)
        )
)
setcolorder(dt_lpi_long, c("isocode","year","lpi_sm","lpi_raw"))

DT_lpi = as.data.table(ReadDatatable("lpi_bayesian", readOnly = FALSE))
if (is.null(DT_lpi)) stop("Datatable 'lpi_bayesian' not found.")

ch_lpi = Changeset("lpi_bayesian", type = "write")
AddDeletions(ch_lpi, DT_lpi)
AddInsertions(ch_lpi, dt_lpi_long)
Finalize(ch_lpi)
message("✅ Done ")