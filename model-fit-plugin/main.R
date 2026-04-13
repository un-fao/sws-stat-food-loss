#########################################################
#
# Description:
# This plugin prepares the model training dataset for the
# Bayesian food loss workflow and fits the hierarchical
# Bayesian model in NIMBLE. It pulls production, import,
# and protected loss data from SWS, reconciles protected
# loss observations from SUA and AP sources, reconstructs
# observed loss percentages, combines them with cleaned
# literature-based loss factor data, rebuilds the M49/SDG
# country grouping structure, restricts the data to valid
# country-product-year combinations with production support,
# joins harvest calendar and preloaded weather, GDP, and
# LPI covariates from SWS datatables, applies the required
# preprocessing steps for modelling, fits the model, and
# saves the fitted outputs, scaling objects, factor-level
# mappings, and prediction inputs needed for the prediction
# phase. The plugin also produces convergence diagnostics
# and an HTML convergence report.
#
# Main outputs:
# - model_data.qs2
# - prediction_inputs.qs2
# - prediction_meta.qs2
# - fit_combined_samples.qs2
# - fit_samples_list_coda.qs2
# - levels_fit.qs2
# - scale_fit.qs2
# - convergence_report.html
# - psrf_diagnostics.csv
# - convergence_diagnostics.csv
#
# Parameters:
# - start_year: first year to include (default 1991)
# - end_year: last year to include (default 2024)
#########################################################

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

library(countrycode)

library(stats4)
library(scales)
library(faoswsFlag)


if (faosws::CheckDebug()) {
    library(faoswsModules)
    SETT <- ReadSettings("sws.yml")
    GetTestEnvironment(baseUrl = SETT[["server"]], token = SETT[["token"]])
}


R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")

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

if (end_year < start_year) {
    stop("end_year must be greater than or equal to start_year")
}

years_out <- start_year:end_year
selectedYear <- as.character(years_out)
#vector of years for which the plugin retrieves inputs and produces outputs.


#####################
#Prepare_model_data

#load(file.path(R_SWS_SHARE_PATH,'Bayesian_food_loss/Inputs/FAOCrops2.RData'))
#load(file.path(R_SWS_SHARE_PATH,'Bayesian_food_loss/Inputs/CropCalendar2.RData'))
#load(file.path(R_SWS_SHARE_PATH,'Bayesian_food_loss/Inputs/input_data2.RData'))

#lapply(list.files("R", pattern = "\\.R$", full.names = TRUE), source)
source_files <- c(
    list.files(file.path(if (CheckDebug()) "." else Sys.getenv("ROOT_PATH"), "R"), full.names = TRUE, pattern = "\\.R$")
)

invisible(lapply(source_files, source))
############################
# Training data construction
############################

REMOVE_VARS_METHOD <- 'new'
CLEAN_CONVFACTOR <- TRUE
AGGREGATE_ACTIVITIES <- TRUE
COMPLETE_COUNTRYGROUPS <- TRUE # Add missing country?
COMPLETE_CROPCALENDAR <- TRUE # Complete crop calendar?






rapid_ass <- as.data.table(dplyr::tribble(
    ~"geographicaream49",~"measureditemcpc",~"timepointyears",~"percentage_loss_of_quantity",~"analyst",
    "288","21",2008,"2.5","alicia.english@fao.org",
    "288","01316",2009,"5","x.noemail@fao.org",
    "410","0111",2014,"13","",
    "410","0111",2014,"15","",
    "288","01233",2010,"2.8-22","alicia.english@fao.org",
    "204","01651",2009,"5.9","x.noemail@fao.org",
    "404","0112",2012,"2.9-8.5","alicia.english@fao.org",
    "484","01318",2013,"32.78","alicia.english@fao.org",
    "204","01318",2010,"5-22.8","alicia.english@fao.org",
    "288","01359",2010,"30-40","alicia.english@fao.org",
    "204","01323",2009,"5","x.noemail@fao.org",
    "204","01651",2009,"10","x.noemail@fao.org",
    "204","01651",2009,"18","x.noemail@fao.org",
    "288","21",2008,"1.25-8.66","alicia.english@fao.org",
    "410","0113",2014,"10","",
    "694","012",2010,"10.0-20","alicia.english@fao.org",
    "646","01318",2009,"15.9","x.noemail@fao.org",
    "288","01313",2010,"26","alicia.english@fao.org",
    "646","01199",2009,"15","x.noemail@fao.org",
    "204","01234",2009,"24","x.noemail@fao.org",
    "204","01234",2009,"29","x.noemail@fao.org",
    "288","01239.01",2009,"8.5","",
    "288","01239.01",2009,"6.3","",
    "410","0111",2014,"8","alicia.english@fao.org",
    "410","0115",2014,"13","",
    "484","01330",2013,"45.53","alicia.english@fao.org",
    "204","01234",2010,"26.4-28","alicia.english@fao.org",
    "646","01234",2009,"2","x.noemail@fao.org",
    "646","01312",2009,"9.5","x.noemail@fao.org",
    "566","0112",2013,"2.5","x.noemail@fao.org",
    "800","01445",2017,"2%","alicia.english@fao.org",
    "356","01234",2010,"10.3-13.46","alicia.english@fao.org",
    "204","01316",2010,"20.8-70","alicia.english@fao.org",
    "566","0112",2013,"3","x.noemail@fao.org",
    "288","01651",2010,"8.7-17.5","alicia.english@fao.org",
    "288","01233",2009,"2.8","x.noemail@fao.org",
    "288","01316",2009,"0.4","x.noemail@fao.org",
    "410","0112",2014,"13","",
    "410","0115",2014,"15","",
    "566","0112",2013,"6","x.noemail@fao.org",
    "800","01445",2017,"35%","alicia.english@fao.org",
    "484","01652",2013,"44.14","alicia.english@fao.org",
    "404","01312",2012,"0.02","alicia.english@fao.org",
    "204","01323",2009,"11.6","x.noemail@fao.org",
    "484","01229",2013,"41.24","alicia.english@fao.org",
    "204","01199",2009,"89.5","x.noemail@fao.org",
    "204","01323",2009,"51","x.noemail@fao.org",
    "288","01212",2009,"8","x.noemail@fao.org",
    "288","01234",2010,"25","alicia.english@fao.org",
    "404","0112",2012,".1-8.8","alicia.english@fao.org",
    "404","0112",2012,"8-8.8","alicia.english@fao.org",
    "800","0112",2017,"17%","alicia.english@fao.org",
    "204","01219.90",2010,"34-89","alicia.english@fao.org",
    "646","01318",2009,"17","x.noemail@fao.org",
    "356","01316",2010,"18.5-31","alicia.english@fao.org",
    "566","0112",2013,"2","x.noemail@fao.org",
    "288","01316",2010,"10","x.noemail@fao.org",
    "204","01323",2009,"10","x.noemail@fao.org",
    "694","0113",2010,"7","alicia.english@fao.org",
    "646","01199",2009,"32.5","x.noemail@fao.org",
    "288","01239.01",2010,"16.6","alicia.english@fao.org",
    "288","01316",2009,"8","x.noemail@fao.org",
    "356","01239.01",2010,"2.0-7","alicia.english@fao.org",
    "204","01651",2010,"8.0-11","alicia.english@fao.org",
    "288","012",2010,"30-40","alicia.english@fao.org",
    "288","01234",2010,"22.5","x.noemail@fao.org",
    "204","01234",2010,"23-29","alicia.english@fao.org",
    "404","01359",2010,"30","alicia.english@fao.org",
    "356","01253.01",2010,"70-80","alicia.english@fao.org",
    "646","01234",2009,"11","x.noemail@fao.org",
    "646","01312",2009,"7.5","x.noemail@fao.org",
    "288","01233",2009,"16.2","x.noemail@fao.org",
    "288","01234",2009,"21.5","x.noemail@fao.org",
    "288","01239.01",2009,"28","x.noemail@fao.org",
    "410","0115",2014,"14","",
    "356","01239.01",2010,"2.0-5","alicia.english@fao.org",
    "484","01323",2013,"23.22","alicia.english@fao.org",
    "646","01234",2009,"6.5","x.noemail@fao.org",
    "410","0112",2014,"18","alicia.english@fao.org",
    "646","01199",2009,"8.3","x.noemail@fao.org",
    "204","01253.01",2010,"15-72.5","alicia.english@fao.org",
    "566","0112",2013,"2","x.noemail@fao.org",
    "288","01318",2010,"20","alicia.english@fao.org",
    "484","01322",2013,"33.38","alicia.english@fao.org",
    "356","01316",2010,"25-30","alicia.english@fao.org",
    "356","01234",2010,"8.18-10.69","alicia.english@fao.org",
    "356","01232",2010,"10.0-20","alicia.english@fao.org",
    "288","01234",2010,"21.5","alicia.english@fao.org",
    "566","0112",2013,"5","x.noemail@fao.org",
    "834","21",1996,"0.1","",
    "566","0112",2013,"2","x.noemail@fao.org",
    "404","21",2012,"1.0-2","alicia.english@fao.org",
    "484","01311",2013,"53.97","alicia.english@fao.org",
    "204","01316",2010,"22.5-76.5","alicia.english@fao.org",
    "288","01239.01",2010,"6.0-28","",
    "204","01234",2009,"31.2","x.noemail@fao.org",
    "404","0112",2012,"1","alicia.english@fao.org",
    "484","01233",2013,"49.07","alicia.english@fao.org",
    "646","01199",2009,"13.5","x.noemail@fao.org",
    "410","0112",2014,"18","",
    "800","0112",2017,"5%","alicia.english@fao.org",
    "484","21111.01",2013,"34.87","alicia.english@fao.org",
    "484","23140",2013,"9.39","alicia.english@fao.org",
    "288","01239.01",2010,"0-4.5","alicia.english@fao.org",
    "356","01239.01",2010,"10.5-29","alicia.english@fao.org",
    "288","01316",2009,"2.5","x.noemail@fao.org",
    "410","0112",2014,"11","alicia.english@fao.org",
    "410","0115",2014,"15","",
    "800","01445",2017,"5%","alicia.english@fao.org",
    "356","01312",2010,"26-31","alicia.english@fao.org",
    "288","01212",2010,"6.5-32","alicia.english@fao.org",
    "356","01239.01",2010,"3.0-5","alicia.english@fao.org",
    "356","01233",2010,"45-80","alicia.english@fao.org",
    "646","01312",2009,"14.8","x.noemail@fao.org",
    "204","01323",2009,"41","x.noemail@fao.org",
    "288","01239.01",2009,"2.3","x.noemail@fao.org",
    "410","0115",2014,"8","alicia.english@fao.org",
    "356","01316",2009,"4 - 5%","masakhwe.mayienga@fao.org",
    "484","01510",2013,"37.11","alicia.english@fao.org",
    "484","0113",2013,"46.87","alicia.english@fao.org",
    "484","01234",2013,"17.78","alicia.english@fao.org",
    "288","01316",2010,"2.3-6","alicia.english@fao.org",
    "288","0112",1987,"10.7 - 31.1","x.noemail@fao.org",
    "204","01651",2009,"6.2","x.noemail@fao.org",
    "646","01318",2009,"20","x.noemail@fao.org",
    "646","01234",2009,"6","x.noemail@fao.org",
    "204","01234",2009,"27.5","x.noemail@fao.org",
    "410","0111",2014,"11","alicia.english@fao.org",
    "566","0112",2013,"8.5","x.noemail@fao.org",
    "360","01354",2010,"30%","alicia.english@fao.org",
    "204","01219.90",2010,"17.3-47","alicia.english@fao.org",
    "484","01251",2013,"19.01","alicia.english@fao.org",
    "404","0112",2012,".2-.5","alicia.english@fao.org",
    "454","0112",2011,"8.4-9.7","alicia.english@fao.org",
    "288","01318",2010,"20-21.3","alicia.english@fao.org",
    "204","01318",2010,"22","alicia.english@fao.org",
    "646","01199",2009,"7.5","x.noemail@fao.org",
    "204","01234",2009,"26.4","x.noemail@fao.org",
    "646","01234",2009,"12.5","x.noemail@fao.org",
    "204","01651",2009,"8","x.noemail@fao.org",
    "288","01212",2009,"45","x.noemail@fao.org",
    "694","21",2010,"20","alicia.english@fao.org",
    "646","01312",2009,"35.1","x.noemail@fao.org",
    "694","01640",2010,"20-25","alicia.english@fao.org",
    "204","01323",2010,"5.0-15","alicia.english@fao.org",
    "484","0231",2013,"37.66","alicia.english@fao.org",
    "360","012",2010,"30","alicia.english@fao.org",
    "204","01234",2009,"28","x.noemail@fao.org",
    "410","0112",2014,"15","",
    "288","01253.01",2010,"5.5-18","alicia.english@fao.org",
    "356","012",2010,"20-30","alicia.english@fao.org",
    "484","21",2013,"54.07","alicia.english@fao.org",
    "204","01323",2009,"15","x.noemail@fao.org",
    "288","01234",2009,"25.1","x.noemail@fao.org",
    "288","01316",2009,"2.3","x.noemail@fao.org",
    "204","01219.90",2010,"17.3-79","alicia.english@fao.org",
    "404","012",2010,"25","alicia.english@fao.org",
    "404","01312",2010,"40","alicia.english@fao.org",
    "694","012",2010,"5","alicia.english@fao.org",
    "356","01510",2010,"19-28","alicia.english@fao.org",
    "204","01219.90",2010,"89.35","alicia.english@fao.org",
    "204","01234",2009,"27.5","x.noemail@fao.org",
    "288","21",2008,"2.56-6.66","alicia.english@fao.org",
    "288","01233",2009,"2","x.noemail@fao.org",
    "404","0112",2012,"3","alicia.english@fao.org",
    "288","01234",2010,"50.5","x.noemail@fao.org",
    "566","0112",2013,"2","x.noemail@fao.org",
    "484","02211",2013,"37.14","alicia.english@fao.org",
    "288","01234",2010,"10.5-23","alicia.english@fao.org",
    "288","01313",2010,"21","alicia.english@fao.org",
    "694","0113",2010,"8","alicia.english@fao.org",
    "204","01199",2009,"47","x.noemail@fao.org",
    "204","01651",2009,"7","x.noemail@fao.org",
    "288","21",2008,"1.7 - 2.5","alicia.english@fao.org",
    "288","01212",2009,"13","x.noemail@fao.org",
    "288","01212",2009,"32","x.noemail@fao.org",
    "288","01234",2009,"23","x.noemail@fao.org",
    "566","0112",2013,"2","x.noemail@fao.org",
    "404","01312",2012,".1-2.5","alicia.english@fao.org",
    "404","01312",2012,"3","alicia.english@fao.org",
    "204","01253.01",2010,"6.0-50","alicia.english@fao.org",
    "288","01239.01",2010,"6.3-15","alicia.english@fao.org",
    "834","21",1996,"1.1","",
    "484","01316",2013,"54.54","alicia.english@fao.org",
    "694","0113",2010,"6.2","alicia.english@fao.org",
    "204","01199",2009,"11","alicia.english@fao.org",
    "356","01316",2009,"4.5 - 7%","masakhwe.mayienga@fao.org",
    "356","01316",2009,"3 - 5%","masakhwe.mayienga@fao.org",
    "356","01213",2010,"31-40","alicia.english@fao.org",
    "818","012",2010,"30","alicia.english@fao.org",
    "566","0112",2013,"1","x.noemail@fao.org",
    "356","01234",2010,"15-20","alicia.english@fao.org",
    "646","01318",2009,"11.8","x.noemail@fao.org",
    "646","01312",2009,"19","x.noemail@fao.org",
    "204","01199",2009,"17.3","x.noemail@fao.org",
    "288","01233",2009,"13.9","x.noemail@fao.org",
    "288","01239.01",2009,"15","x.noemail@fao.org",
    "288","01316",2010,".4-10.4","alicia.english@fao.org",
    "404","0112",2012,".2-2.4","alicia.english@fao.org",
    "410","0113",2014,"8","",
    "410","0113",2014,"13","",
    "288","01212",2010,"5.0-45","alicia.english@fao.org",
    "204","01651",2010,"5.9-15","alicia.english@fao.org",
    "288","01318",2010,"6.7-12.5","alicia.english@fao.org",
    "566","0112",2013,"2.8","x.noemail@fao.org",
    "204","01199",2009,"34.5","x.noemail@fao.org",
    "356","01239.01",2010,"1.5-7","alicia.english@fao.org",
    "356","012",2010,"20","alicia.english@fao.org",
    "288","01233",2009,"11.3","x.noemail@fao.org",
    "288","01233",2009,"19","x.noemail@fao.org",
    "288","01239.01",2009,"16.6","x.noemail@fao.org",
    "410","0113",2014,"16","",
    "410","0115",2014,"24","alicia.english@fao.org",
    "484","01235",2013,"14.65","alicia.english@fao.org",
    "484","01651",2013,"45.46","alicia.english@fao.org",
    "646","01318",2009,"2","x.noemail@fao.org",
    "646","01318",2009,"2.9","x.noemail@fao.org",
    "204","01199",2009,"79","x.noemail@fao.org",
    "288","21",2008,"0 - 1.4","alicia.english@fao.org",
    "288","01239.01",2009,"4.5","x.noemail@fao.org",
    "288","01316",2009,"1","x.noemail@fao.org",
    "484","21113.01",2013,"40.91","alicia.english@fao.org",
    "288","01233",2010,"2.0-19","alicia.english@fao.org",
    "288","01706",1996,"3.7-4.3","x.noemail@fao.org",
    "484","01701",2013,"24.96","alicia.english@fao.org",
    "834","21",1996,"2","",
    "204","01323",2009,"16.4","x.noemail@fao.org",
    "204","01651",2009,"15","x.noemail@fao.org",
    "288","01212",2009,"5","x.noemail@fao.org",
    "288","01233",2009,"22","x.noemail@fao.org",
    "288","01239.01",2009,"6","",
    "404","0112",2012,"0.04","alicia.english@fao.org",
    "410","0111",2014,"14","",
    "410","0111",2014,"15","",
    "410","0112",2014,"11","",
    "202","0112",2011,"1.4-5.9","",
    "646","01318",2009,"10.4","x.noemail@fao.org",
    "356","01234",2010,"3.64-4.75","alicia.english@fao.org",
    "204","01323",2009,"10.9","x.noemail@fao.org",
    "646","01318",2009,"21","x.noemail@fao.org",
    "404","0112",2012,"10.7","alicia.english@fao.org",
    "288","01316",2010,"1.0-8","alicia.english@fao.org",
    "404","012",2010,"30","alicia.english@fao.org",
    "646","01199",2009,"2","x.noemail@fao.org",
    "356","01239.01",2010,"2.0-5","alicia.english@fao.org",
    "646","01312",2009,"25","x.noemail@fao.org",
    "288","01212",2009,"28.1","x.noemail@fao.org",
    "288","01234",2009,"21.5","x.noemail@fao.org",
    "404","0112",2012,".7-6.7","alicia.english@fao.org",
    "404","0112",2012,"0.04","alicia.english@fao.org",
    "410","0113",2014,"14","",
    "410","0113",2014,"15","",
    "404","0112",2012,".2-1.6","alicia.english@fao.org",
    "484","01317",2013,"22.8","alicia.english@fao.org",
    "288","01233",2010,"0-16.2","alicia.english@fao.org",
    "204","01234",2009,"21.2","x.noemail@fao.org",
    "288","01212",2009,"54","x.noemail@fao.org",
    "288","01316",2009,"6","x.noemail@fao.org",
    "410","0111",2014,"24","alicia.english@fao.org",
    "202","0112",2011,"8","",
    "356","01359",2010,"20-30","alicia.english@fao.org",
    "404","01312",2012,".6-3","alicia.english@fao.org",
    "288","01239.01",2010,"0.34","x.noemail@fao.org",
    "646","01199",2009,"18.5","x.noemail@fao.org",
    "646","01234",2009,"7","x.noemail@fao.org",
    "288","01234",2010,"23","alicia.english@fao.org",
    "288","01706",1996,"50-100","x.noemail@fao.org",
    "288","01706",1997,"8.0-21","alicia.english@fao.org",
    "800","0112",2017,"3.3%","alicia.english@fao.org",
    "356","01316",2009,"2 - 4%","masakhwe.mayienga@fao.org",
    "484","01252",2013,"21.35","alicia.english@fao.org",
    "694","012",2010,"20","alicia.english@fao.org",
    "484","01234",2013,"28.86","alicia.english@fao.org",
    "484","01313",2013,"53.76","alicia.english@fao.org",
    "288","01253.01",2010,"0-6.5","alicia.english@fao.org",
    "484","2349",2013,"45.31","alicia.english@fao.org",
    "288","01234",2010,"35.5","x.noemail@fao.org",
    "356","01319",2010,"32-48","alicia.english@fao.org",
    "484","21",2013,"23.43","alicia.english@fao.org",
    "288","01233",2009,"9.5","x.noemail@fao.org",
    "410","0112",2014,"18","",
    "204","01234",2009,"23","x.noemail@fao.org",
    "694","0113",2010,"5","alicia.english@fao.org",
    "204","01323",2010,"10.9-51","alicia.english@fao.org",
    "288","01234",2010,"14-21.5","alicia.english@fao.org",
    "694","0113",2010,"6.6","alicia.english@fao.org",
    "204","01323",2009,"33","x.noemail@fao.org",
    "410","0113",2014,"10","",
    "288","01651",2010,"13-18.1","alicia.english@fao.org",
    "484","01253.01",2013,"32.08","alicia.english@fao.org",
    "404","0112",2012,"1","alicia.english@fao.org",
    "566","0112",2013,"8.5","x.noemail@fao.org",
    "646","01199",2009,"25","x.noemail@fao.org",
    "356","01253.01",2010,"23-32","alicia.english@fao.org",
    "646","01312",2009,"30.1","x.noemail@fao.org",
    "646","01199",2009,"12.5","x.noemail@fao.org",
    "288","01212",2009,"6.5","x.noemail@fao.org",
    "288","01239.01",2010,"2.3","alicia.english@fao.org",
    "410","0113",2014,"14","",
    "410","0115",2014,"11","",
    "204","01651",2010,"6.2-18","alicia.english@fao.org",
    "404","21",2012,"6","alicia.english@fao.org",
    "356","01316",2010,"5.0-10","alicia.english@fao.org",
    "356","01234",2010,"3.94-5.54","alicia.english@fao.org",
    "288","01212",2010,"23-53","alicia.english@fao.org",
    "288","01318",2010,"26.3-28","alicia.english@fao.org"))





############# Computation Parameters #####################################
savesws <- TRUE
maxYear <- format(Sys.Date(), "%Y")


ctry_modelvar <- 'All'
updatemodel <- TRUE
# updatemodel <- swsContext.computationParams$updateModel
subnationalestimates <- TRUE
#lossDataset <- swsContext.datasets[[1]]@dataset

message("Updatemodel: ", updatemodel)
message("Subnationalestimates: ", subnationalestimates)
message('SelectedYear: ', paste(selectedYear, collapse = ', '))

flwdb <- "flw_lossperfactors__20250506_solr"
# flwdb <- swsContext.computationParams$flwdb
flwdb_name <- gsub('flw_lossperfactors__', '',flwdb)
if(as.numeric(substr(flwdb_name, 1, 4)) < 2024){
    
    # These are all the potential tags on the SUbnational Estimates
    # selecting data collection methods for aggregating the subnational estimates are
    # based on those that will give the best range of representative data
    # DataCollectionTags_all <- c("Expert Opinion", "-", "SWS", "NationalStatsYearbook",
    #                             "NonProtected", "Survey", "Rapid Assessment", "NationalAcctSys",
    #                             "WRI Protocol", "FBS/APQ", "LitReview", "Case Study",
    #                             "APHLIS", "NP", "Laboratory Trials", "Modelled",
    #                             "Field Trial", "Crop Cutting Field Experiment", "Census",
    #                             "No Data Collection Specified")
    # 
    DataCollectionTags_represent <- c("Expert Opinion", "-", "SWS", "NationalStatsYearbook",
                                      "NonProtected", "Survey", "NationalAcctSys",
                                      "WRI Protocol", "FBS/APQ", "LitReview", "Case Study",
                                      "APHLIS", "NP", "Laboratory Trials", "Modelled", "Census",
                                      "No Data Collection Specified")
} else {
    
    DataCollectionTags_represent <- c("Literature Review", 
                                      "Case Study",                  
                                      "Survey",
                                      "No Data Collection Specified",
                                      #  "Controlled Experiment",
                                      "Modelled Estimates",
                                      "National Accounts",
                                      "Expert Opinion",              
                                      "",
                                      "FLW Protocol",                
                                      "Census",
                                      "Modelled estimates")
}

ExternalDataOpt <- DataCollectionTags_represent

# Order in the target dataset
cols_order <- c("geographicAreaM49", "measuredElementSuaFbs",
                "measuredItemSuaFbs", "timePointYears", "Value",
                "flagObservationStatus", "flagMethod")

# Overall Lower and Upper bounds
LB <- 0.02
UB <- 0.65

# Flags for estimates
estflag <- c( "I;ec", "I;m", "I;es", "I;e", "I;i", "I;esr", "I;ecr") # CHA: Added 'I;i' to estimated flags 


############### Connection to the SWS ###########################################

areaVar <- "geographicAreaM49"
yearVar <- "timePointYears"
itemVar <- "measuredItemCPC"
elementVar <- "measuredElement"
valuePrefix <- "Value_"
flagObsPrefix <- "flagObservationStatus_"
flagMethodPrefix <- "flagMethod_"

keys <- c(areaVar, yearVar, itemVar)
keys_lower <- tolower(keys)
keys2 <- c(areaVar, itemVar)

##### Load Data ######

#####  Collects the data from the SWS #####

message('FL model: Load and prepare data tables')

FAOCrops     <- ReadDatatable("fcl2cpc_ver_2_1")

ConvFactor1  <- ReadDatatable(flwdb, readOnly = T)

ConvFactor1[ , geographicaream49 := as.numeric(geographicaream49)]
ConvFactor1[ , geographicaream49 := as.character(geographicaream49)]
# XXX 2023-02-22
ConvFactor1 <- ConvFactor1[!(country == 'Mexico' & crop %in% c("Meat of cattle with the bone, fresh or chilled", "Meat of pig with the bone, fresh or chilled", "Raw milk of cattle"))]
# RAPID ASSESSMENT FIX
ConvFactor1 <- ConvFactor1[!unique(rapid_ass), on = names(rapid_ass)] # Reinserted


CountryGroup <- ReadDatatable("a2017regionalgroupings_sdg_feb2017")

CountryGroup[, geographicaream49 := m49_code]
CountryGroup[, country := tolower(m49_region)]

FAOCrops[, crop := description]
FAOCrops[, measureditemcpc := cpc] #FAOCrops[, measureditemcpc := addHeadingsCPC(cpc)]

# There are 3 FCL codes going to 0112!
# This can create duplicates, so removing the ones not to consider (series are not in use anymore)  
# This is the only case an FCL points to more than one CPC.
FAOCrops <- FAOCrops[!fcl %in% c('0067', '0068')]

# CHA: 'append2tree' object contains elements that are missing from the tree
# this happens because the tree contains the lower level of the CPC 
# (e.g. 01241 is not in the tree beacuse only 01241.01 and 01241.90 are)
# these elements end up being wrogly excluded in calculations 
fbsTree      <- ReadDatatable("gfli_basket_cpc_article_2018", readOnly = T) # 456 5 var

# Using here fbsTree1 as it has slight differences wrt fbsTree, as used
# inside the lossModel functions (global and country).
# TODO: Probably the two should be made a single object.
fbsTree1 <- copy(fbsTree)

fbsTree1[, GFLI_Basket := NA_character_]
fbsTree1[foodgroupname %in% c(2905), GFLI_Basket := 'Cereals']
fbsTree1[foodgroupname %in% c(2911), GFLI_Basket := 'Pulses']
fbsTree1[foodgroupname %in% c(2919,2918), GFLI_Basket := 'Fruits & Vegetables']
fbsTree1[foodgroupname %in% c(2907,2913), GFLI_Basket := 'Roots, Tubers & Oil-Bearing Crops'] # 2913 Oilseeds
fbsTree1[foodgroupname %in% c(2914,2908,2909,2912,2922,2923), GFLI_Basket := 'Other']
fbsTree1[foodgroupname %in% c(2943, 2946,2945), GFLI_Basket := 'Animals Products & Fish and fish products'] # ,2948 Change, put dairy with Eggs
# XXX: For country model this is eggs, for global it is animal products...
fbsTree1[foodgroupname %in% c(2949, 2948), GFLI_Basket := 'Eggs & Dairy'] # Modified from Eggs to Eggs & Dairy adding 2948
#fbsTree1[foodgroupname %in% c(2943, 2946,2945,2949,2948), GFLI_Basket :='Fish',] #Fish needs to be included after it has losses in the SWS


library(magrittr)
library(readxl)

CropCalendar  <- ReadDatatable("crop_calendar_nov17")


if (COMPLETE_COUNTRYGROUPS == TRUE) {
    CountryGroup_fix <- CountryGroup[isocode == 'CHN']
    CountryGroup_fix[, c("m49_code", "iso2code", "isocode", "m49_region", "geographicaream49", "country") := 
                         list("158", "TW", "TWN", "Taiwan", "158", "taiwan")]
    CountryGroup <- rbind(CountryGroup, CountryGroup_fix)
}


if (COMPLETE_CROPCALENDAR == TRUE) {
    CropCalendar[geographicaream49 == '156', geographicaream49 := '1248']
    CropCalendar[, geographicaream49 := as.integer(geographicaream49)]
    CropCalendar <- CropCalendar[, c("geographicaream49", "measureditemcpc","crop", "harvesting_month_onset", "harvesting_month_end"), with = FALSE]
    CropCalendar <- CropCalendar[!grepl(' ', measureditemcpc)]
    # CropCalendar[, measureditemcpc := addHeadingsCPC(measureditemcpc)]
    CropCalendar[, crop := tolower(crop)]
    
    # TWN has no crop calendar, assigning CHN
    CropCalendar <-
        rbind(
            CropCalendar,
            # It's either 156 OR 1248
            CropCalendar[geographicaream49 %in% c(156L, 1248L), .(geographicaream49 = 158L, measureditemcpc, crop, harvesting_month_onset, harvesting_month_end)]
        )
    
    # NZ has no crop calendar, assigning AUS
    CropCalendar <-
        rbind(
            CropCalendar,
            CropCalendar[geographicaream49 == 36L, .(geographicaream49 = 554L, measureditemcpc, crop, harvesting_month_onset, harvesting_month_end)]
        )
    
    # Fiji has no crop calendar, assigning AUS
    CropCalendar <-
        rbind(
            CropCalendar,
            CropCalendar[geographicaream49 == 36L, .(geographicaream49 = 242L, measureditemcpc, crop, harvesting_month_onset, harvesting_month_end)]
        )
    
    # Jamaica has no crop calendar, assigning Dominican Rep.
    CropCalendar <-
        rbind(
            CropCalendar,
            CropCalendar[geographicaream49 == 214L, .(geographicaream49 = 388L, measureditemcpc, crop, harvesting_month_onset, harvesting_month_end)]
        )
    
    # UAE has no crop calendar, assigning Saudi Arabia
    CropCalendar <-
        rbind(
            CropCalendar,
            CropCalendar[geographicaream49 == 682L, .(geographicaream49 = 784L, measureditemcpc, crop, harvesting_month_onset, harvesting_month_end)]
        )
    
    # Belize has no crop calendar, assigning Guatemala
    CropCalendar <-
        rbind(
            CropCalendar,
            CropCalendar[geographicaream49 == 320L, .(geographicaream49 = 84L, measureditemcpc, crop, harvesting_month_onset, harvesting_month_end)]
        )
}

# # Set China to 1248
# CountryGroup[m49_code == '156', m49_code := '1248']
# CountryGroup[geographicaream49 == '156', geographicaream49 := '1248']
# CropCalendar[geographicaream49 == '156', geographicaream49 := '1248']

#save(CropCalendar, file.path(save_dir, "CropCalendar2.RData"))

###### ----- Start pulling datasets ---- #####


message('FL model: Pull datasets')

countriesNotToInclude <- c("831", "832", "274") # Guersney, Jersey, Gaza
faostatcountriesNotToInclude <- c("76") # Sudan
areaList <- GetCodeList('aproduction', 'aproduction', 'geographicAreaM49')[type == 'country' & !code %in% countriesNotToInclude, code]
faostatcountryList <- GetCodeList("faostat_one", "updated_sua_2013_data", "geographicAreaFS")[!code %in% faostatcountriesNotToInclude,code]
FbsSuaList <- GetCodeList('suafbs', 'sua_balanced', 'measuredItemFbsSua')[,code]
itemList <- GetCodeList('aproduction', 'aproduction', 'measuredItemCPC')[code %in% FbsSuaList, code]
faostatitemList <- GetCodeList("faostat_one", "updated_sua_2013_data", "measuredItemFS")[,code]

startYearNewMethodology_value <- '2010'
# Get production data the 'startYearNewMethodology' is to keep up with FSB revision 
if(!exists("production")) production <-  getProductionData_new(areaList = areaList, itemList = itemList, elementList = "5510",
                                                               faostatcountryList = faostatcountryList,
                                                               faostatitemList = faostatitemList,
                                                               selectedYear = selectedYear, 
                                                               startYearNewMethodology = startYearNewMethodology_value)

stopifnot(nrow(production) > 0)
setnames(production, "Value", "value_measuredelement_5510")
setnames(production, tolower(names(production)))
production[, geographicaream49 := as.character(geographicaream49)]
production[, timepointyears := as.numeric(timepointyears)]

# Get import data
if(!exists("imports")) imports <- getImportData_new(areaList,itemList, elementList = "5610", selectedYear)

stopifnot(nrow(imports) > 0)
setnames(imports, "Value", "value_measuredelement_5610")
setnames(imports, tolower(names(imports)))
imports[, timepointyears := as.numeric(timepointyears)]

# Get loss data from SUA balanced
lossProtectedSUA <- getLossData_SUA(areaList,itemList, elementList = "5016",
                                    faostatcountryList, faostatitemList,
                                    selectedYear, startYearNewMethodology = as.numeric(startYearNewMethodology_value), protected = TRUE)

### Added 28/08/2021
lossProtectedAP <- getLossData_AP(areaList,itemList, elementList = "5016",
                                  selectedYear, protected = TRUE)


lp <- merge(lossProtectedSUA[flagObservationStatus != 'M'], 
            lossProtectedAP[flagObservationStatus != 'M'], by = c(keys, 'measuredElement'), suffixes = c('SUA', 'AP'),
            all = T)

# Try to get the maximum number of loss data as sometimes they are only in one of the two datasets (SUA or AP)
lp[ValueSUA == ValueAP, check := 'compliant']
lp[ValueSUA != ValueAP, check := 'AP data changed']
lp[is.na(ValueSUA) & !is.na(ValueAP), check := 'Only in AP']
lp[!is.na(ValueSUA) & is.na(ValueAP), check := 'Only in SUA']
lp[check == 'AP data changed', ValueAP := round(ValueAP)]
lp[ValueSUA == ValueAP, check := 'compliant']
lp[ValueSUA != ValueAP, check := 'AP data changed']
lp[check == 'AP data changed', ValueSUA := round(ValueSUA)]
lp[ValueSUA == ValueAP, check := 'compliant']
lp[ValueSUA != ValueAP, check := 'AP data changed']

lp[check == 'Only in AP', c('Value', 'flagObservationStatus', 'flagMethod' ,"datasource") := list(ValueAP,
                                                                                                  flagObservationStatusAP,
                                                                                                  flagMethodAP,"AP")]
lp[check == 'Only in SUA',c('Value', 'flagObservationStatus', 'flagMethod',"datasource") := list(ValueSUA,
                                                                                                 flagObservationStatusSUA,
                                                                                                 flagMethodSUA,"SUA")]
lp[check == 'compliant', c('Value', 'flagObservationStatus', 'flagMethod',"datasource") := list(ValueAP,
                                                                                                flagObservationStatusAP,
                                                                                                flagMethodAP,"AP&SUA")]
lp[check == 'AP data changed', c('Value', 'flagObservationStatus', 'flagMethod',"datasource") := list(ValueAP,
                                                                                                      flagObservationStatusAP,
                                                                                                      flagMethodAP,"AP")]

# If loss data in AP changed before the startYearNewMethodology the changes must be reflected
# in the production data, i.e. ratios must be calculated with consistent datasets either both from old faostat methodology
# or both from the new data
APupd <- lp[check == 'AP data changed' & timePointYears < startYearNewMethodology_value]

lp[, c("ValueSUA", "flagObservationStatusSUA", "flagMethodSUA",
       "flagCombinationSUA", "ValueAP", "flagObservationStatusAP",
       "flagMethodAP", "flagCombinationAP", "check")] <- NULL

lossProtected <- copy(lp)

if(nrow(APupd)>0){
    productionKey = DatasetKey(
        domain = "agriculture",
        dataset = "aproduction",
        dimensions = list(
            Dimension(name = "geographicAreaM49",
                      keys = unique(as.character(APupd$geographicAreaM49))),
            Dimension(name = "measuredElement", keys = '5510'),
            Dimension(name = "timePointYears", keys = as.character(unique(APupd$timePointYears))),
            Dimension(name = "measuredItemCPC",
                      keys = unique(APupd$measuredItemCPC)))
    )
    
    productionUpdt = GetData(
        key = productionKey,
        flags = TRUE)
    
    productionUpdt[, timePointYears := as.numeric(timePointYears)]
    productionUpdt[, geographicAreaM49 := as.numeric(geographicAreaM49)]
    
    productionUpdt <- productionUpdt[APupd[, keys, with = F], on = keys]
    names(productionUpdt) <- tolower(names(productionUpdt))
    setnames(productionUpdt, 'value', 'value_measuredelement_5510')
    productionUpdt[, geographicaream49 := as.character(geographicaream49)]
    production <- rbind(production[!productionUpdt, on = keys_lower ], productionUpdt)
}

### end addition

stopifnot(nrow(lossProtected) > 0)

lossProtected[, value_measuredelement_5126 := 0]
setnames(lossProtected, "Value", "value_measuredelement_5016")
setnames(lossProtected, tolower(names(lossProtected)))

# Combine Production and import data
prod_imports <- merge(production, imports, by = keys_lower, all.x = TRUE)
prod_imports[, prod_imports := rowSums_(.SD), .SDcols = c("value_measuredelement_5510", "value_measuredelement_5610")]
prod_imports <- prod_imports[, c(keys_lower, "value_measuredelement_5510", "value_measuredelement_5610", "prod_imports"), with = FALSE]

lossProtected[, geographicaream49 := as.character(geographicaream49)]
lossProtected[, timepointyears := as.numeric(timepointyears)]

# Do not consider 'Wine' and live animals
lossProtected <- lossProtected[ measureditemcpc !=  '24212.02']
meatcodes <- unique(lossProtected$measureditemcpc[grepl('021', lossProtected$measureditemcpc)])
lossProtected <- lossProtected[! measureditemcpc %in% meatcodes]
prod_imports <-prod_imports[! measureditemcpc %in% meatcodes]
prod_imports <- prod_imports[ measureditemcpc !=  '24212.02']


lossData <- merge(prod_imports, lossProtected, by = keys_lower, all.y = TRUE)
lossData[, loss_per_clean := value_measuredelement_5016/value_measuredelement_5510]
lossData[, loss_per_clean_pi := value_measuredelement_5016/prod_imports]
lossData[, per_diff := loss_per_clean - loss_per_clean_pi]
lossData[, value_measuredelement_5126 := loss_per_clean]

### Some countries are import dependent and protected losses are over 100%,
### therefore % should be applied to both production + imports

# Import dependent ratio
# IDR = (Imports-Exports)/(Production+Imports-Exports)

comodities <-
    lossData[
        per_diff > 0.1,
        c("geographicaream49", "measureditemcpc", "value_measuredelement_5016",
          "prod_imports", "loss_per_clean", "loss_per_clean_pi", "per_diff"),
        with = FALSE
    ]

comodities[, combp := paste(geographicaream49, measureditemcpc, sep = ";")]

#save(prod_imports_input,comodities_input,file="production_inputs.RData")


lossData[, fsc_location := "SWS"]

for (t in unique(comodities$geographicaream49)) {
    for(i in unique(comodities[geographicaream49 == t]$measureditemcpc)) {
        lossData[
            geographicaream49 == t & measureditemcpc == i,
            `:=`(
                value_measuredelement_5126 = loss_per_clean_pi,
                fsc_location = "SWS;Prod_imp"
            )
        ]
    }
}


## Some commodities still produce greater than 100 % losses
lossData[value_measuredelement_5126 > 1, value_measuredelement_5126 := 0]

setnames(lossData, tolower(names(lossData)))

lossData <-
    merge(
        lossData,
        CountryGroup[, c("isocode", "geographicaream49", "country"), with = FALSE],
        by = "geographicaream49",
        all.x = TRUE
    )

lossData[, loss_per_clean := value_measuredelement_5126]



lossData <-
    lossData[,
             c(keys_lower, "isocode", "country", "loss_per_clean",
               "fsc_location", "flagobservationstatus", "flagmethod"),
             with = FALSE
    ]

message("Number of lossData available: ", nrow(lossData))
#message("Number of losses to estimate: ", nrow(timeSeriesDataToBeImputed[is.na(protected)]))


message('FL model: Work on literature data')


lossData <- lossData[measureditemcpc %in% fbsTree$measureditemcpc]


# 27/09/21 remove E,f and imp dep lower than LB

lossData <- lossData[flagobservationstatus != 'E']
lossData[fsc_location == 'SWS;Prod_imp' & loss_per_clean < LB, loss_per_clean := NA]
lossData <- lossData[!is.na(loss_per_clean)]



#timeSeriesDataToBeImputed <- timeSeriesDataToBeImputed[measureditemcpc %in% fbsTree$measureditemcpc]

# No data 21 as most of them are for Fish
ConvFactor1 <- ConvFactor1[measureditemcpc != '21']
### end addition

#ConvFactor1  <- ReadDatatable('flw_lossperfactors__20210828_provisional', readOnly = T)
# Remove 278 Germany New Lander, 280 Germany Fed. Rep. of, 530, 582 Pacific Islands Trust Tr 

### Added 5 August
ConvFactor1[ fsc_location %in% c('Export','Market', 'Distribution', 'Post-harvest', 
                                 'Collector', 'Packing', 'Grading', 'Stacking'), .N]
#ConvFactor1[fsc_location == 'Export', fsc_location := 'Storage']
ConvFactor1[fsc_location == 'Market', fsc_location := 'Storage']
ConvFactor1[fsc_location == 'Distribution', fsc_location := 'Wholesale']
ConvFactor1[fsc_location == 'Post-harvest', fsc_location := 'Farm']
ConvFactor1[fsc_location == 'Collector',fsc_location := 'Trader']
ConvFactor1[fsc_location == 'Packing',fsc_location := 'Processing']
ConvFactor1[fsc_location == 'Grading',fsc_location := 'Farm']
ConvFactor1[fsc_location == 'Stacking',fsc_location := 'Farm']
### End addition

lossData[geographicaream49 %in% c("278", "280", "530", "582",
                                  "474", # Martinique
                                  "312", # Guadeloupe
                                  "254", # French Guaiana
                                  "638" # Reunion
),]
lossData <- lossData[!geographicaream49 %in% c("278", "280", "530", "582",
                                               "474", # Martinique
                                               "312", # Guadeloupe
                                               "254", # French Guaiana
                                               "638" # Reunion
),]


ConvFactor1[, geographicaream49 := sub("^0+", "", geographicaream49)]


USretail <- ConvFactor1[geographicaream49 == '840' & fsc_location == 'Retail']
ConvFactor1 <- ConvFactor1[!USretail, on = names(ConvFactor1)]
#
ConvFactor1[url == "https://www.ers.usda.gov/data-products/food-availability-per-capita-data-system/", fsc_location := 'whole supply chain']
ConvFactor1[geographicaream49 == '156' & loss_per_clean >= 15, loss_per_clean := NA]

########### Loss Factor Data and Aggregation ###################

ConvFactor1[, fsc_location := tolower(fsc_location)] # Added to use new dt 'flw_lossperfactors__20200925'
unique(ConvFactor1$fsc_location)[! unique(ConvFactor1$fsc_location) %in% c("farm", "transport", "storage", "trader", "wholesale", "processing", "wholesupplychain", "sws_total")]  
ConvFactor1[fsc_location == "whole supply chain", fsc_location := "wholesupplychain"]
unique(ConvFactor1$fsc_location)[! unique(ConvFactor1$fsc_location) %in% c("farm", "transport", "storage", "trader", "wholesale", "processing", "wholesupplychain", "sws_total")]  

### Added 28/08/2021
ConvFactor1[url == "http://www.bibliotheque.auf.org/doc_num.php?explnum_id=383", 
            fsc_location := 'storage']
### end addition

ConvFactor1 <- ConvFactor1[measureditemcpc != '24212.02']
ConvFactor1[geographicaream49 == '156', geographicaream49 := '1248']
ConvFactor1[, loss_per_clean := loss_per_clean/100]
ConvFactor1 <- ConvFactor1[!measureditemcpc %in% meatcodes]
UBstage <- 0.25

if(CLEAN_CONVFACTOR){
    # Filters out the non-representative observations
    ########################
    # CHA: ignore sws_total in ConvFactor1 and put instead clean lossData
    ########################
    ConvFactor_clean <- ConvFactor1[fsc_location != 'sws_total']
    ConvFactor_clean <- ConvFactor_clean[tag_datacollection != 'FBS/APQ']
    # Set keys all columns
    setkeyv(ConvFactor_clean, names(ConvFactor_clean))
    # Exactly duplicated rows discarded and other incomplete rows
    setkey(ConvFactor_clean)
    CFdups <- ConvFactor_clean[duplicated(ConvFactor_clean)]
    ConvFactor_clean <- ConvFactor_clean[!duplicated(ConvFactor_clean)]
    ConvFactor_clean <- ConvFactor_clean[loss_per_clean > 0]
    ConvFactor_clean <- ConvFactor_clean[!is.na(geographicaream49)]
    ConvFactor_clean <- ConvFactor_clean[!is.na(timepointyears)]
    ConvFactor_clean <- ConvFactor_clean[!is.na(measureditemcpc) & measureditemcpc != ""]
    # ConvFactor_clean[country == 'United States Of America', country := 'United States of America']
    # ConvFactor_clean[country == 'Timor Leste', country := 'Timor-Leste']
    # ConvFactor_clean[country == 'Democratic Republic Of Congo', country := 'Democratic Republic of the Congo']
    # ConvFactor_clean[country == 'China, Main', country := 'China']
    # ConvFactor_clean[country == 'Austrailia', country := 'Australia']
    # ConvFactor_clean[country == "Cote D'Ivoire", country := "Cote d'Ivoire"]
    # ConvFactor_clean[country == "Côte d'Ivoire", country := "Cote d'Ivoire"]
    # ConvFactor_clean[country == 'Tanzania', country := 'United Republic of Tanzania']
    # 
    ConvFactor_clean[country == 'Belize', geographicaream49 := '84']
    ConvFactor_clean[country == 'Brazil', geographicaream49 := '76']
    ConvFactor_clean[isocode == 'CRI', geographicaream49 := '188']
    ConvFactor_clean[isocode == 'PAK', geographicaream49 := '586']
    # 
    # Maximum 25% loss per stage
    ConvFactor_clean[fsc_location != "wholesupplychain" & loss_per_clean >= UBstage, loss_per_clean := NA]
    # Lower Bound for WSC losses - 27/09/21
    ConvFactor_clean[fsc_location == "wholesupplychain" & loss_per_clean < LB,loss_per_clean := NA]
    #Added 07/10/21
    ConvFactor_clean[fsc_location == "wholesupplychain" & loss_per_clean > UB,loss_per_clean := NA]
    
    ConvFactor_clean <- ConvFactor_clean[!is.na(loss_per_clean)]
    
    
    ConvFactor_clean <- ConvFactor_clean[geographicaream49 %in% areaList]
    ### Added 28/08/2021
    ConvFactor_clean[measureditemcpc == '0146', measureditemcpc := '01460']
    ConvFactor_clean[measureditemcpc == '01316.01', measureditemcpc := '01316']
    ConvFactor_clean[measureditemcpc == '01316.02', measureditemcpc := '01316']
    ConvFactor_clean[measureditemcpc == '21116i', measureditemcpc := '21116']
    ConvFactor_clean[measureditemcpc == '21113', measureditemcpc := '21113.01']
    ConvFactor_clean[measureditemcpc == '2112', measureditemcpc := '21121']
    ConvFactor_clean[measureditemcpc == '0215', measureditemcpc := '21121'] 
    ConvFactor_clean[measureditemcpc == '01709', measureditemcpc := '01709.90']
    ConvFactor_clean[measureditemcpc == '022', measureditemcpc := '02211']
    ConvFactor_clean[measureditemcpc == '0129', measureditemcpc := '01290.90']
    # end addition
    
    ConvFactor_clean <- ConvFactor_clean[measureditemcpc %in% fbsTree1$measureditemcpc & loss_per_clean < 1]
    
    if(AGGREGATE_ACTIVITIES){
        # Make sure same activities are labelled the same at least in APHLIS
        ### ConvFactor_clean <- ConvFactor_clean[tag_datacollection == 'APHLIS' & activity == 'Threshing & Shelling', activity := 'Threshing/Shelling']
        
        # Location of inerest for the whole supply chain (needs to be added 'wholesupplychain' and 'sws_total')
        loc2 <- c("farm","transport", "storage", "trader", "wholesale", "processing") #, "retail")
        
        # Observation not in location of interest
        fscobs <- copy(ConvFactor_clean[!fsc_location %in% loc2, c(keys_lower, "isocode", "country", "loss_per_clean", 
                                                                   "fsc_location", "activity","tag_datacollection", "url","solr_id"), with = FALSE])
        # Observation in location of interest
        activity0 <- copy(ConvFactor_clean[fsc_location %in% loc2 , c(keys_lower, "isocode", "country", "loss_per_clean", 
                                                                      "fsc_location", "activity","tag_datacollection", "url","solr_id"), with = FALSE])
        
        # Separate rows where activities is specified or not
        activity <- activity0[!activity %in% ("") ,]
        wholeact <- activity0[activity %in% ("") ,]
        
        # Create object of unique stages
        setkey(activity)
        uniquestage <- unique(activity[,c("geographicaream49", "isocode", "country", "timepointyears", 
                                          "measureditemcpc",  "fsc_location", "tag_datacollection", "url","solr_id"), with = F])
        
        # Start loop to aggregate activities
        newactivity <- data.table()
        
        for(i in seq_len(uniquestage[,.N])){
            subsact <- activity[uniquestage[i,], on = c("geographicaream49", "isocode", "country", "timepointyears", 
                                                        "measureditemcpc",  "fsc_location", "tag_datacollection", "solr_id")]
            
            if(subsact[ , .N] > 1 & length(subsact$activity) == length(unique(subsact$activity))){ # More than one occurrence with all different activities
                subsact[  , loss_per_clean := 1-prod(1- subsact$loss_per_clean)]
                aggract <- unique(subsact[, c("geographicaream49", "isocode", "country", "timepointyears", "measureditemcpc",  "fsc_location", "loss_per_clean", "tag_datacollection", "url","solr_id"), with = F]) 
            } else if(subsact[ , .N] > 1 & length(subsact$activity) != length(unique(subsact$activity))){ # More than one occurrence with duplicated activities
                
                subsact <- subsact[, loss_per_clean := mean_(loss_per_clean), activity]
                setkey(subsact)
                subsact <- unique(subsact)
                
                # Now, after averaging, aggregate activities if more than one
                if(subsact[ , .N] > 1 & length(subsact$activity) == length(unique(subsact$activity))){ # More than one occurrence with all different activities
                    subsact[  , loss_per_clean := 1-prod(1- subsact$loss_per_clean)]
                    aggract <- unique(subsact[, c("geographicaream49", "isocode", "country", "timepointyears", "measureditemcpc",  "fsc_location", "loss_per_clean", "tag_datacollection", "url","solr_id"), with = F]) 
                } else {
                    aggract <- subsact[, c("geographicaream49", "isocode", "country", "timepointyears", "measureditemcpc",  "fsc_location", "loss_per_clean", "tag_datacollection", "url","solr_id"), with = F]
                }
                
            } else {
                aggract <- subsact[, c("geographicaream49", "isocode", "country", "timepointyears", "measureditemcpc",  "fsc_location", "loss_per_clean", "tag_datacollection", "url","solr_id"), with = F]
            }
            newactivity <- rbind(newactivity, aggract)
        }
        
        # Check if overlaps and complete the dataset with observation with no activity specified
        wholeact[,activity:=NULL]
        
        ## CHA: Possibility to merge 'no activity' and 'aggregated activity' figure, i.e:
        ## If a figure has been calculated aggregating activities hence the figure
        ## with no activity is discarded, e.g. APHLIS data should be treated like this.
        ## Waiting for further discussion, to keep it simple and avoid further loss of row, 
        ## data will be just bind for now
        
        # Maximum 20% loss per stage
        newactivity[loss_per_clean >= UBstage, loss_per_clean := UBstage]
        newactivity <- newactivity[!is.na(loss_per_clean)]
        newconfact <- rbind(newactivity, wholeact)
        
        newconfact <-   rbind(newconfact, fscobs[ , activity := NULL])
        newconfact[, country := tolower(country)]
        
    }else {
        newconfact <- ConvFactor_clean
        newconfact[, country := tolower(country)] 
    }
    
    # Repeat Maximum 20% loss per stage, now winsoring
    newconfact[fsc_location != "wholesupplychain" & loss_per_clean > UBstage, loss_per_clean := UBstage]
    newconfact <- newconfact[!is.na(loss_per_clean)]
    
    # Add lower bound per stage 27/09/21
    newconfact <- newconfact[loss_per_clean > 0.005]
    
    # Correct data and avoid inserting NAs or data below LB, NO MORE CHINA 1248!
    lossData_clean <- copy(as.data.table(lossData[flagobservationstatus != 'M' &
                                                      !is.na(loss_per_clean) & 
                                                      loss_per_clean > LB & loss_per_clean < UB])) ### Added and removed 28/08/21  
    
    
    lossData_clean[, fsc_location := 'sws_total']
    lossData_clean[, tag_datacollection := 'SWS_2']
    lossData_clean[geographicaream49 == '1248', geographicaream49 := '156']
    
    # ORS added url="SWS", solr_id="SWS"
    lossData_clean$url <- "Statistical Working System"
    lossData_clean$solr_id <- "Statistical Working System"
    
    ConvFactor1_clean <- rbind(newconfact, lossData_clean[, c("geographicaream49",
                                                              "timepointyears",
                                                              "measureditemcpc",
                                                              "isocode",
                                                              "country",
                                                              "loss_per_clean",
                                                              "fsc_location",
                                                              "tag_datacollection","url","solr_id"), with = F], fill = TRUE)
    
    ConvFactor1_clean[geographicaream49 == '156', geographicaream49 := '1248']
    
    #ConvFactor1_clean[tag_datacollection == "Literature Review", tag_datacollection := "LitReview"]
    #ConvFactor1_clean[tag_datacollection == "National Stats Yearbook", tag_datacollection := "NationalStatsYearbook"]
    #ConvFactor1_clean[tag_datacollection == "", tag_datacollection := "-"]
    
    ### Added 28/08/2021
    #ConvFactor1_clean[tag_datacollection == "Modelled Estimates", tag_datacollection := "Modelled"]
    #ConvFactor1_clean[tag_datacollection == "No Data Collection Specified", tag_datacollection := "-"]
    #ConvFactor1_clean[tag_datacollection == "National Accounts System", tag_datacollection := "NationalAcctSys"]
    ### End addition
    # ORS Added SWS below to keep for new model
    ConvFactor2 <- ConvFactor1_clean[tag_datacollection %in% c(ExternalDataOpt,"SWS_2")] # CHA: substitute ConvFactor1
    
    ConvFactor2 <- ConvFactor2[!is.na(loss_per_clean)]
    ConvFactor2 <- ConvFactor2[loss_per_clean < UB]
    locations <- c("farm", "transport", "storage", "trader", "wholesale", "processing", "wholesupplychain", "sws_total") ## <>, "retail"
    ConvFactor2 <- ConvFactor2[fsc_location %in% locations]
    
} else {
    # Filters out the non-representative observations
    ConvFactor2 <- ConvFactor1[tag_datacollection %in% ExternalDataOpt]
    ConvFactor2 <- ConvFactor2[!is.na(loss_per_clean)]
    ConvFactor2 <- ConvFactor2[loss_per_clean < UB]
    ConvFactor2[geographicaream49 == '156', geographicaream49 := '1248']
    locations <- c("farm", "transport", "storage", "trader", "wholesale", "processing", "wholesupplychain", "sws_total") ## <>, "retail"
    ConvFactor2 <- ConvFactor2[fsc_location %in% locations]
}

##### Fix factors ----
# XXX: Added 2023-02-22: loss of meat fo cattle in Peru seem way too high
# to be a reliable factor. This will require more investigation by the
# CLFS team. For now, removing.
ConvFactor2 <- ConvFactor2[!(measureditemcpc == '21111.01' & geographicaream49 == 604)]


setkey(ConvFactor2)
ConvFactor2 <- unique(ConvFactor2)


ConvFactor2[,isocode := NULL]
ConvFactor2 <- merge(ConvFactor2, CountryGroup[,.(geographicaream49, isocode)], by = 'geographicaream49', all.x = T)


FullSet <- ConvFactor2

setkey(FullSet)
FullSet <- unique(FullSet)



# CHA change function from join to merge
FullSet <- merge(FullSet, FAOCrops[, c("measureditemcpc", "crop"), with = FALSE], by = "measureditemcpc", all.x = T)
############################
# FULLSET IS A PART OF MY TRAINING DATA
############################

save_dir <- file.path(R_SWS_SHARE_PATH, "Bayesian_food_loss", "Inputs")
dir.create(save_dir, recursive = TRUE, showWarnings = FALSE)
save(FAOCrops, file = file.path(save_dir, "FAOCrops.RData"))
save(CropCalendar,file = file.path(save_dir, "CropCalendar.RData"))

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
    isocode %in% M49$iso3 & year >= start_year,
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
year_min = start_year
year_max = end_year

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
    isocode %in% M49$iso3 & year >= start_year & year <= end_year,
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
model_data <- FullSet%>%select(geographicaream49,timepointyears,measureditemcpc,
                               fsc_location,loss_per_clean,tag_datacollection,
                               url,solr_id)%>%
    left_join(select(fbsTree1,measureditemcpc,gfli_basket,food_group))%>%#,store_min,store_max,requires_fridge,perish_class,perish_score,fridge_or_cool))%>%
    dplyr::rename(year=timepointyears,
                  loss_percentage=loss_per_clean)%>%
    mutate(m49_numeric=as.numeric(geographicaream49))


# Label official SUA data.
model_data$tag_datacollection[model_data$tag_datacollection=="SWS_2"] <- "SUA"

# Relabel China's M49 code.
#model_data$m49_numeric[model_data$m49_numeric==1248] <- 156

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
    filter(year >= start_year) %>%
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
model_data %<>%
    filter(!is.na(iso3), !is.na(year), !is.na(rainfall_mean),
           !is.na(temperature_mean), !is.na(sm_GDP_percap),
           !is.na(sm_lpi)) %>%
    ungroup() %>%
    mutate(
        stage = factor(stage),
        method = factor(method)
    ) %>%
    mutate(
        stage = stats::relevel(stage, ref = "wholesupplychain"),
        method = stats::relevel(method, ref = "Supply utilization accounts")
    )

# Make SWS the first source in the data source effect.
model_data$source <- factor(model_data$datasource)
model_data$source <- stats::relevel(model_data$source, ref = "SWS")

# Make a lot of variables factors with carefully-specified levels.
model_data %<>% mutate(
    iso3 = factor(iso3, levels = sort(unique(model_data$iso3))),
    country_m49 = factor(m49_numeric, levels = sort(unique(m49_numeric))), 
    region_l2 = factor(region_l2, levels = sort(unique(M49$region_l2))),
    region_l1 = factor(region_l1, levels = sort(unique(M49$region_l1))),
    food_group = factor(food_group, levels = sort(unique(food_group))),
    crop = factor(crop, levels = sort(unique(crop))),
    measureditemcpc = factor(measureditemcpc, levels = sort(unique(model_data$measureditemcpc))),
    basket_country = factor(paste(food_group, m49_numeric), levels = sort(unique(paste(food_group, m49_numeric)))),
    crop_country = factor(paste(measureditemcpc, m49_numeric), levels = sort(unique(paste(measureditemcpc, m49_numeric))))
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
    year_seq = start_year:end_year
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
# Set up the parallel computing cluster 
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

# Save ALL posterior draws (parameters) as a matrix (fast for prediction)
qs2::qs_save(
    fit_combined_samples,
    file = file.path(save_dir, "fit_combined_samples.qs2")
)
#fit_combined_samples <- qs2::qs_read("fit_combined_samples.qs2")
#class(fit_combined_samples)
#str(fit_combined_samples, max.level = 2)
#dim(fit_combined_samples)
# (Optional) Save the chain-wise coda objects too (for diagnostics later)
qs2::qs_save(
    fit_samples_list,
    file = file.path(save_dir, "fit_samples_list_coda.qs2")
)
#fit_samples_list_coda <- qs2::qs_read("fit_samples_list_coda.qs2")
#class(fit_samples_list_coda)
#str(fit_samples_list_coda, max.level = 2)
#dim(fit_samples_list_coda)
# Save factor level mappings used in the fit (CRITICAL if to rebuild model_data later)
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

# Save scaling constants used for covariates in the fit/prediction code
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
###########################################
# Reporting
###########################################
library(htmltools)
library(sendmailR)
library(base64enc)
save_dir = file.path(R_SWS_SHARE_PATH, "Bayesian_food_loss", "Saved_models", "mcmc_outputs_2026")
fit_samples_list = qs2::qs_read(file.path(save_dir, "fit_samples_list_coda.qs2"))


report_dir = file.path(R_SWS_SHARE_PATH, "Bayesian_food_loss", "Convergence_report")
dir.create(report_dir, recursive = TRUE, showWarnings = FALSE)



coef_labels_6 = c("intercept", "year", "rainfall", "temperature", "GDP", "LPI")
coef_labels_2 = c("intercept", "year")

get_idx = function(x, pattern) {
    m = regmatches(x, regexec(pattern, x))[[1]]
    if (length(m) == 0) return(NULL)
    as.integer(m[-1])
}

decode_parameter = function(param, levels_fit) {
    
    idx = get_idx(param, "^a1\\[(\\d+)\\]$")
    if (!is.null(idx)) {
        return(sprintf("Global effect; coefficient = %s",
                       coef_labels_6[idx[1]]))
    }
    
    idx = get_idx(param, "^a2\\[(\\d+),\\s*(\\d+)\\]$")
    if (!is.null(idx)) {
        return(sprintf("Level-1 region effect; region_l1 = '%s'; coefficient = %s",
                       levels_fit$region_l1[idx[1]], coef_labels_6[idx[2]]))
    }
    
    idx = get_idx(param, "^a3\\[(\\d+),\\s*(\\d+)\\]$")
    if (!is.null(idx)) {
        return(sprintf("Level-2 region effect; region_l2 = '%s'; coefficient = %s",
                       levels_fit$region_l2[idx[1]], coef_labels_6[idx[2]]))
    }
    
    idx = get_idx(param, "^a4\\[(\\d+),\\s*(\\d+)\\]$")
    if (!is.null(idx)) {
        m49_code = as.character(levels_fit$country_m49[idx[1]])
        country_label = M49$country[match(m49_code, as.character(M49$m49_numeric))]
        
        if (is.na(country_label) || length(country_label) == 0) {
            country_label = m49_code
        }
        
        return(sprintf("Country effect; country = '%s' (M49 %s); coefficient = %s",
                       country_label, m49_code, coef_labels_2[idx[2]]))
    }
    
    idx = get_idx(param, "^b1\\[(\\d+),\\s*(\\d+)\\]$")
    if (!is.null(idx)) {
        return(sprintf("Food-group effect; food_group = '%s'; coefficient = %s",
                       levels_fit$food_group[idx[1]], coef_labels_6[idx[2]]))
    }
    
    idx = get_idx(param, "^b2\\[(\\d+),\\s*(\\d+)\\]$")
    if (!is.null(idx)) {
        return(sprintf("Product effect; crop = '%s'; coefficient = %s",
                       levels_fit$crop[idx[1]], coef_labels_2[idx[2]]))
    }
    
    idx = get_idx(param, "^c1\\[(\\d+)\\]$")
    if (!is.null(idx)) {
        return(sprintf("Stage effect; stage = '%s'",
                       levels_fit$stage[idx[1]]))
    }
    
    idx = get_idx(param, "^c2\\[(\\d+)\\]$")
    if (!is.null(idx)) {
        return(sprintf("Method effect; method = '%s'",
                       levels_fit$method[idx[1]]))
    }
    
    idx = get_idx(param, "^c3\\[(\\d+)\\]$")
    if (!is.null(idx)) {
        return(sprintf("Source effect; source = '%s'",
                       levels_fit$source[idx[1]]))
    }
    
    idx = get_idx(param, "^sigma_a2\\[(\\d+)\\]$")
    if (!is.null(idx)) {
        return(sprintf("SD of level-1 region effects; coefficient = %s",
                       coef_labels_6[idx[1]]))
    }
    
    idx = get_idx(param, "^sigma_a3\\[(\\d+)\\]$")
    if (!is.null(idx)) {
        return(sprintf("SD of level-2 region effects; coefficient = %s",
                       coef_labels_6[idx[1]]))
    }
    
    idx = get_idx(param, "^sigma_a4\\[(\\d+)\\]$")
    if (!is.null(idx)) {
        return(sprintf("SD of country effects; coefficient = %s",
                       coef_labels_2[idx[1]]))
    }
    
    idx = get_idx(param, "^sigma_b1\\[(\\d+)\\]$")
    if (!is.null(idx)) {
        return(sprintf("SD of food-group effects; coefficient = %s",
                       coef_labels_6[idx[1]]))
    }
    
    idx = get_idx(param, "^sigma_b2\\[(\\d+)\\]$")
    if (!is.null(idx)) {
        return(sprintf("SD of product effects; coefficient = %s",
                       coef_labels_2[idx[1]]))
    }
    
    idx = get_idx(param, "^b3\\[(\\d+),\\s*(\\d+)\\]$")
    if (!is.null(idx)) {
        key = levels_fit$basket_country[idx[1]]
        m = regmatches(key, regexec("^(.*)\\s+([0-9]+)$", key))[[1]]
        
        food_group_label = m[2]
        m49_code = m[3]
        country_label = M49$country[match(m49_code, as.character(M49$m49_numeric))]
        
        if (is.na(country_label) || length(country_label) == 0) {
            country_label = m49_code
        }
        
        return(sprintf(
            "Food-group-by-country effect; food_group = '%s'; country = '%s'; coefficient = %s",
            food_group_label, country_label, coef_labels_2[idx[2]]
        ))
    }
    
    idx = get_idx(param, "^b4\\[(\\d+),\\s*(\\d+)\\]$")
    if (!is.null(idx)) {
        key = levels_fit$crop_country[idx[1]]
        m = regmatches(key, regexec("^(.*)\\s+([0-9]+)$", key))[[1]]
        
        cpc_code = m[2]
        m49_code = m[3]
        
        country_label = M49$country[match(m49_code, as.character(M49$m49_numeric))]
        if (is.na(country_label) || length(country_label) == 0) {
            country_label = m49_code
        }
        
        crop_label = FAOCrops$crop[match(cpc_code, FAOCrops$measureditemcpc)]
        if (is.na(crop_label) || length(crop_label) == 0) {
            crop_label = cpc_code
        }
        
        return(sprintf(
            "Product-by-country effect; product = '%s'; country = '%s'; coefficient = %s",
            crop_label, country_label, coef_labels_2[idx[2]]
        ))
    }
    
    if (param == "sigma_c3") return("SD of source effects")
    if (param == "sigma_y")  return("Residual SD")
    
    return(NA_character_)
}


#Potential Scale Reduction Factor
uni_psrf = lapply(
    colnames(fit_samples_list$samples[[1]]),
    function(x) {
        gelman.diag(
            fit_samples_list$samples[, x],
            multivariate = FALSE,
            autoburnin = FALSE,
            transform = TRUE
        )
    }
)

param_names = colnames(fit_samples_list$samples[[1]])

psrf_df = data.frame(
    parameter = param_names,
    psrf_point = sapply(uni_psrf, function(x) x$psrf[1]),
    psrf_upper = sapply(uni_psrf, function(x) x$psrf[2]), #llowing for sampling uncertainty, PSRF could plausibly be as high as this upper value.
    stringsAsFactors = FALSE
)

prop_psrf_105 = mean(psrf_df$psrf_point <= 1.05, na.rm = TRUE)
#proportion of psrf less than 1.05


psrf_df$meaning = vapply(
    psrf_df$parameter,
    decode_parameter,
    character(1),
    levels_fit = levels_fit
)

# Save CSV 
write.csv(
    psrf_df,
    file = file.path(report_dir, "psrf_diagnostics.csv"),
    row.names = FALSE
)

psrf_df = psrf_df[order(-psrf_df$psrf_point), ]
top_psrf_df = head(
    psrf_df[, c("parameter", "meaning", "psrf_point", "psrf_upper")],
    50
)









################################################################################
# Geweke diagnostic
# Compute Geweke z-scores separately for each chain, then summarise by parameter
################################################################################
geweke_list = lapply(fit_samples_list$samples, function(chain_obj) {
    out = tryCatch(coda::geweke.diag(chain_obj)$z, error = function(e) NULL)
    if (is.null(out)) {
        z = rep(NA_real_, length(param_names))
        names(z) = param_names
        return(z)
    }
    z = rep(NA_real_, length(param_names))
    names(z) = param_names
    z[names(out)] = out
    z
})

geweke_mat = do.call(cbind, geweke_list)


#there are c1, c2 and c3 which are set to 0 since they represent baseline stage, method and source effects corrispondingly
all_na_rows = apply(geweke_mat, 1, function(x) all(is.na(x)))

if (any(all_na_rows)) {
    geweke_mat = geweke_mat[!all_na_rows, , drop = FALSE]
    param_names_geweke = param_names[!all_na_rows]
} else {
    param_names_geweke = param_names
}



geweke_df = data.frame(
    parameter = param_names_geweke,
    geweke_mean_abs_z = rowMeans(abs(geweke_mat), na.rm = TRUE),
    geweke_max_abs_z  = apply(abs(geweke_mat), 1, max, na.rm = TRUE),
    stringsAsFactors = FALSE
)

prop_geweke_2 = mean(geweke_df$geweke_mean_abs_z <= 2, na.rm = TRUE)
#proportion of parameters whose Geweke diagnostic looks acceptable under the chosen threshold of 2
geweke_df$meaning = vapply(
    geweke_df$parameter,
    decode_parameter,
    character(1),
    levels_fit = levels_fit
)




###########################################################################
# Merge diagnostics
###########################################################################
diag_df = merge(
    psrf_df[, c("parameter", "meaning", "psrf_point", "psrf_upper")],
    geweke_df[, c("parameter", "geweke_mean_abs_z", "geweke_max_abs_z")],
    by = "parameter",
    all = TRUE
)

write.csv(
    diag_df,
    file = file.path(report_dir, "convergence_diagnostics.csv"),
    row.names = FALSE
)

# Worst parameters first
psrf_df = psrf_df[order(-psrf_df$psrf_point), ]
top_psrf_df = head(
    psrf_df[, c("parameter", "meaning", "psrf_point", "psrf_upper")],
    50
)

geweke_df = geweke_df[order(-geweke_df$geweke_mean_abs_z), ]
top_geweke_df = head(
    geweke_df[, c("parameter", "meaning", "geweke_mean_abs_z", "geweke_max_abs_z")],
    50
)


fmt_num = function(x, digits = 3) {
    ifelse(is.na(x), "NA", formatC(x, digits = digits, format = "f"))
}

html_table_from_df = function(df) {
    if (nrow(df) == 0) {
        return(htmltools::HTML("<p><em>No rows to display.</em></p>"))
    }
    
    header = paste0(
        "<tr>",
        paste(sprintf("<th>%s</th>", htmltools::htmlEscape(names(df))), collapse = ""),
        "</tr>"
    )
    
    rows = apply(df, 1, function(r) {
        cells = paste(
            mapply(function(v, nm) {
                if (nm %in% c("psrf_point", "psrf_upper", "geweke_mean_abs_z", "geweke_max_abs_z")) {
                    paste0("<td>", fmt_num(as.numeric(v), 3), "</td>")
                } else {
                    paste0("<td>", htmltools::htmlEscape(as.character(v)), "</td>")
                }
            }, r, names(df), SIMPLIFY = TRUE),
            collapse = ""
        )
        paste0("<tr>", cells, "</tr>")
    })
    
    htmltools::HTML(
        paste0(
            "<table style='border-collapse:collapse;width:100%;'>",
            "<thead>", header, "</thead>",
            "<tbody>", paste(rows, collapse = "\n"), "</tbody>",
            "</table>"
        )
    )
}


report_doc = htmltools::tags$html(
    htmltools::tags$head(
        htmltools::tags$meta(charset = "utf-8"),
        htmltools::tags$title("Bayesian food loss model - convergence report"),
        htmltools::tags$style(htmltools::HTML("
            body { font-family: Arial, Helvetica, sans-serif; margin: 32px; color: #222; line-height: 1.5; }
            h1, h2, h3 { color: #1f3b5b; }
            table, th, td { border: 1px solid #d9d9d9; }
            table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }
            th, td { padding: 8px; text-align: left; font-size: 13px; vertical-align: top; }
            th { background: #f3f6fa; }
            code { background: #f7f7f7; padding: 2px 4px; border-radius: 3px; }
            ul { margin-top: 0.3em; }
        "))
    ),
    htmltools::tags$body(
        htmltools::tags$h1("Bayesian food loss model - convergence report"),
        
        htmltools::tags$p(
            "This report summarizes convergence diagnostics for the posterior samples produced by the Bayesian food loss model. ",
            "The diagnostics are computed from the saved MCMC chains and are reported at the level of individual monitored parameters. ",
            "The purpose of the report is descriptive: it documents the distribution of diagnostic values and highlights the parameters with the largest values in the monitored output."
        ),
        
        htmltools::tags$h2("Convergence diagnostics"),
        htmltools::tags$p(
            "This report summarizes two standard diagnostics for the monitored MCMC parameters: ",
            htmltools::tags$b("Gelman-Rubin PSRF"),
            " and ",
            htmltools::tags$b("Geweke z-scores"),
            "."
        ),
        htmltools::tags$p(
            htmltools::tags$b("PSRF"),
            " compares the behavior of parallel chains. Values close to 1 indicate that between-chain and within-chain variation are similar. ",
            "For each parameter, the report shows both the PSRF point estimate and the upper limit returned by ",
            htmltools::tags$code("gelman.diag(...)"),
            "."
        ),
        htmltools::tags$p(
            htmltools::tags$b("Geweke"),
            " is a within-chain diagnostic. It compares the early part of a chain with the late part of the same chain using a z-score. ",
            "In this report, Geweke values are first computed separately for each chain and then summarized across chains using both the mean absolute z-score and the maximum absolute z-score."
        ),
        htmltools::tags$p(
            "The proportions shown below are summary counts of how many monitored parameters fall under the chosen reference thresholds ",
            htmltools::tags$code("PSRF <= 1.05"),
            " and ",
            htmltools::tags$code("max |Geweke z| <= 2"),
            ". These proportions are included for descriptive reporting only."
        ),
        
        htmltools::tags$ul(
            htmltools::tags$li(sprintf("Number of monitored parameters: %d", nrow(diag_df))),
            htmltools::tags$li(sprintf("Proportion with PSRF <= 1.05: %s", fmt_num(prop_psrf_105, 3))),
            htmltools::tags$li(sprintf("Proportion with max |Geweke z| <= 2: %s", fmt_num(prop_geweke_2, 3)))
        ),
        
        htmltools::tags$h2("How to read the tables"),
        htmltools::tags$p(
            "The sections below show the ",
            htmltools::tags$b("50 worst values"),
            ", meaning the parameters with the highest PSRF values or the highest Geweke values."
        ),
        htmltools::tags$ul(
            htmltools::tags$li(
                htmltools::tags$code("parameter"),
                ": the original parameter name as stored in the MCMC output."
            ),
            htmltools::tags$li(
                htmltools::tags$code("meaning"),
                ": decoded interpretation of the parameter, showing the corresponding group label and coefficient."
            ),
            htmltools::tags$li(
                htmltools::tags$code("psrf_point"),
                ": the point estimate of the potential scale reduction factor."
            ),
            htmltools::tags$li(
                htmltools::tags$code("psrf_upper"),
                ": the upper confidence limit returned by ",
                htmltools::tags$code("gelman.diag(...)"),
                "."
            ),
            htmltools::tags$li(
                htmltools::tags$code("geweke_mean_abs_z"),
                ": mean absolute Geweke z-score across chains."
            ),
            htmltools::tags$li(
                htmltools::tags$code("geweke_max_abs_z"),
                ": maximum absolute Geweke z-score across chains."
            )
        ),
        htmltools::tags$p(
            "The tables labelled ",
            htmltools::tags$b("Worst PSRF values"),
            " and ",
            htmltools::tags$b("Worst Geweke values"),
            " are ranking tables. ",
            "They do not contain all monitored parameters; instead, they show only the parameters with the largest diagnostic values. ",
            "The PSRF table is ordered from the largest ",
            htmltools::tags$code("psrf_point"),
            " downward, while the Geweke table is ordered from the largest ",
            htmltools::tags$code("geweke_mean_abs_z"),
            " downward."
        ),
        htmltools::tags$p(
            "A parameter may appear near the top of one table and not the other, because PSRF and Geweke measure different aspects of chain behavior. ",
            "PSRF focuses on agreement across chains, whereas Geweke focuses on within-chain stability between the beginning and the end of the chain."
        ),
        
        htmltools::tags$h2("Decoded parameter legend"),
        htmltools::tags$p(
            "The ",
            htmltools::tags$code("parameter"),
            " column reports the original parameter name stored in the MCMC output. The ",
            htmltools::tags$code("meaning"),
            " column provides the decoded interpretation, using the corresponding labels for regions, countries, stages, methods, and other indexed effects."
        ),
        
        htmltools::tags$p(
            "In indexed coefficient families, the second index ",
            htmltools::tags$code("p"),
            " identifies which coefficient is being referred to. For parameter families defined over all six main terms (",
            htmltools::tags$code("a1"),
            ", ",
            htmltools::tags$code("a2"),
            ", ",
            htmltools::tags$code("a3"),
            ", ",
            htmltools::tags$code("b1"),
            "), ",
            htmltools::tags$code("p = 1"),
            " is the intercept, ",
            htmltools::tags$code("p = 2"),
            " is year, ",
            htmltools::tags$code("p = 3"),
            " is rainfall, ",
            htmltools::tags$code("p = 4"),
            " is temperature, ",
            htmltools::tags$code("p = 5"),
            " is GDP, and ",
            htmltools::tags$code("p = 6"),
            " is LPI. For parameter families defined only for intercept and year (",
            htmltools::tags$code("a4"),
            ", ",
            htmltools::tags$code("b2"),
            ", ",
            htmltools::tags$code("b3"),
            ", ",
            htmltools::tags$code("b4"),
            "), ",
            htmltools::tags$code("p = 1"),
            " is the intercept and ",
            htmltools::tags$code("p = 2"),
            " is year."
        ),
        
        htmltools::tags$ul(
            htmltools::tags$li(htmltools::tags$code("a1[p]"), ": global effect."),
            htmltools::tags$li(htmltools::tags$code("a2[r,p]"), ": level-1 region effect."),
            htmltools::tags$li(htmltools::tags$code("a3[r,p]"), ": level-2 region effect."),
            htmltools::tags$li(
                htmltools::tags$code("a4[c,p]"),
                ": country effect; the decoded country name is shown directly in the ",
                htmltools::tags$code("meaning"),
                " column."
            ),
            htmltools::tags$li(htmltools::tags$code("b1[g,p]"), ": food-group effect."),
            htmltools::tags$li(htmltools::tags$code("b2[i,p]"), ": product effect."),
            htmltools::tags$li(htmltools::tags$code("b3[i,p]"), ": food-group-by-country interaction effect."),
            htmltools::tags$li(htmltools::tags$code("b4[i,p]"), ": product-by-country interaction effect."),
            htmltools::tags$li(htmltools::tags$code("c1[s]"), ": stage effect."),
            htmltools::tags$li(htmltools::tags$code("c2[m]"), ": method effect."),
            htmltools::tags$li(htmltools::tags$code("c3[s]"), ": source effect.")
        ),
        
        
        htmltools::tags$p(
            "Baseline effects such as ",
            htmltools::tags$code("c1[1]"),
            ", ",
            htmltools::tags$code("c2[1]"),
            ", and ",
            htmltools::tags$code("c3[1]"),
            " are fixed to zero by construction."
        ),
        
        htmltools::tags$p(
            "Detailed index tables for region, stage, and method labels are reported in the Appendix."
        ),
        
        htmltools::tags$h2("Worst PSRF values"),
        htmltools::tags$p(
            "These are the parameters with the 50 highest PSRF values in the fitted model."
        ),
        html_table_from_df(top_psrf_df),
        
        htmltools::tags$h2("Worst Geweke values"),
        htmltools::tags$p(
            "Geweke z-scores are computed separately for each chain. For each parameter, the report shows the mean absolute Geweke z-score across chains (",
            htmltools::tags$code("geweke_mean_abs_z"),
            ") and the largest absolute Geweke z-score observed in any chain (",
            htmltools::tags$code("geweke_max_abs_z"),
            "). Larger absolute values indicate greater disagreement between the early and late parts of a chain. The ranking is based on ",
            htmltools::tags$code("geweke_mean_abs_z")
        ),
        
        html_table_from_df(top_geweke_df),
        
        htmltools::tags$h2("Summary"),
        htmltools::tags$ul(
            htmltools::tags$li(sprintf("Proportion with PSRF <= 1.05: %s", fmt_num(prop_psrf_105, 3))),
            htmltools::tags$li(sprintf("Proportion with mean |Geweke z| <= 2: %s", fmt_num(prop_geweke_2, 3)))
        ),
    
        htmltools::tags$h2("Appendix"),
        htmltools::tags$h3("Level-1 region index"),
        html_table_from_df(data.frame(
            index = seq_along(levels_fit$region_l1),
            region_l1 = levels_fit$region_l1,
            stringsAsFactors = FALSE
        )),
        
        htmltools::tags$h3("Level-2 region index"),
        html_table_from_df(data.frame(
            index = seq_along(levels_fit$region_l2),
            region_l2 = levels_fit$region_l2,
            stringsAsFactors = FALSE
        )),
        
        htmltools::tags$h3("Stage index"),
        html_table_from_df(data.frame(
            index = seq_along(levels_fit$stage),
            stage = levels_fit$stage,
            stringsAsFactors = FALSE
        )),
        
        htmltools::tags$h3("Method index"),
        html_table_from_df(data.frame(
            index = seq_along(levels_fit$method),
            method = levels_fit$method,
            stringsAsFactors = FALSE
        )),
        )
)



report_path = file.path(report_dir, "convergence_report.html")

htmltools::save_html(report_doc, file = "convergence_report.html")


# Retrieve the path to the output folder.
out_dir = Sys.getenv("OUTPUT_FOLDER")


###############################
# Email the report
###############################
send_report <- function(to, domain_code, report_path) {
    from <- "<sws@fao.org>"
    to <- paste0("<", to, ">")
    subject <- "Bayesian food loss model - convergence report"
    
    htmlBody <- paste(readLines(report_path), collapse = "\n")
    #htmlBody <- report
    
    # try to generate and send the email, fail if report too big
    tryCatch(
        {
            # Convert the HTML body to MIME format
            body <- mime_part(htmlBody)
            body[["headers"]][["Content-Type"]] <- "text/html"
            
            # Create MIME parts for the attachments
            report_att <- mime_part(x = report_path, name = basename(report_path))
            
            # Combine the HTML body and attachments into the email message
            msg <- list(body, report_att)
            
            
            # Sending the email
            sendmail(
                from = from,
                to = to,
                subject = subject,
                msg = msg
            )
        },
        error = function(e) {
            message("◬ Error: ", e$message)
            message("Please download report from task artifacts")
        }
    )
}


send_report(
    to = swsContext.userEmail,
    domain_code = "lossWaste",
    report_path = report_path
)