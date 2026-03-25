##' Get Production Data
##' 
##' Function to obtain production data at primary level
#' @import  faosws
#' @export getProductionData


getProductionData_new <- function(areaList, itemList, elementList = "5510",
                                  faostatcountryList, faostatitemList,
                                  selectedYear, startYearNewMethodology){
 
  # Period for new methodology production data
  if(startYearNewMethodology > min(as.numeric(selectedYear))){
  yearNM <- as.numeric(startYearNewMethodology):max(as.numeric(selectedYear))
  } else {yearNM <- min(as.numeric(selectedYear)):max(as.numeric(selectedYear))}
  ## create keys for data retrieval
  productionKey = DatasetKey(
    domain = "agriculture",
    dataset = "aproduction",
    dimensions = list(
      Dimension(name = "geographicAreaM49",
                keys = areaList),
      Dimension(name = "measuredElement", keys = elementList),
      Dimension(name = "timePointYears", keys = as.character(yearNM)),
      Dimension(name = "measuredItemCPC",
                keys = itemList))
  )

  productionQuery = GetData(
    key = productionKey,
    flags = TRUE)
  
  # Period for frozen FAOSTAT production data
  
  if(min(as.numeric(selectedYear)) < startYearNewMethodology){
  
  yearOM <- as.numeric(min(as.numeric(selectedYear)):(as.numeric(startYearNewMethodology)-1))

  faostatKey = DatasetKey(
    domain = "faostat_one",
    dataset = "updated_sua_2013_data",
    dimensions = list(
      Dimension(name = "geographicAreaFS",
                keys = faostatcountryList),
      Dimension(name = "measuredElementFS", keys = '51'),
      Dimension(name = "timePointYears", keys = as.character(yearOM)),
      Dimension(name = "measuredItemFS",
                keys = faostatitemList)
    )
  )
  
  faostatQuery = GetData(
    key = faostatKey,
    flags = TRUE)
  
  faostatQuery <- faostatQuery[!measuredItemFS %in% c('67', '68')] # Discontinued series
  faostatQuery[, geographicAreaM49 := fs2m49(geographicAreaFS)]
  faostatQuery[, measuredItemCPC := fcl2cpc(formatC(as.numeric(measuredItemFS), width = 4,
                                            flag = "0"))]
  
  # unique(faostatQuery[is.na(measuredItemCPC)]$measuredItemFS)
  # fs <- GetCodeList("faostat_one", "updated_sua_2013_data", "measuredItemFS")[code %in% unique(faostatQuery[is.na(measuredItemCPC)]$measuredItemFS)]
  # 1159  Offals of Other Camelids
  # 1115  Suckled Milk, Equines
  # 1092  Eggs, Exc Hen Eggs (No)
  # 1067  Hen Eggs (No)
  # 918  Suckled Milk, Cattle
  # 861  Improved Pasture
  # 860  Range Pasture
  # 465  Vegetables Canned nes
  # 464  Vegetables Dried nes
  # 389  Tomatojuice Concentrated
  # 171  Sugars Flavoured
  # 1  Population
  
  faostatQuery <- faostatQuery[!is.na(measuredItemCPC)]
  faostatQuery[, flagObservationStatus := getFlagObservationStatus(flagFaostat)]
  faostatQuery[, flagMethod := getFlagMethod(flagFaostat)]    
  
  faostatQuery[, measuredElement := elementList]
  faostatQuery[, c("geographicAreaFS", "measuredItemFS", "measuredElementFS", "flagFaostat") := NULL]
  
  setcolorder(faostatQuery, c("geographicAreaM49", "measuredElement",
                      "measuredItemCPC", "Value", "timePointYears",
                      "flagObservationStatus", "flagMethod"))
  
  # Mapping many to one
  faostatQuery[, Value := sum(Value), c("geographicAreaM49", "measuredElement",
                               "measuredItemCPC","timePointYears")]
  setkey(faostatQuery)
  faostatQuery <- unique(faostatQuery)
  
  # Put together two productions
  productionWhole <- rbind(faostatQuery, productionQuery)
  } else {
    productionWhole <- productionQuery
  }
  
  
  ## Convert time to numeric
  productionWhole[, timePointYears := as.numeric(timePointYears)]
  
  productionWhole[, geographicAreaM49 := as.numeric(geographicAreaM49)]
  
  return(productionWhole)
  
}

