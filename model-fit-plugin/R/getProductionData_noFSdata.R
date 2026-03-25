##' Get Production Data
##' 
##' Function to obtain production data at primary level
#' @import  faosws
#' @export getProductionData


getProductionData_new <- function(areaList, itemList, elementList = "5510",
                                  faostatcountryList, faostatitemList,
                                  selectedYear, startYearNewMethodology){
  
  # Period for new methodology production data
 yearNM <- min(as.numeric(selectedYear)):max(as.numeric(selectedYear))
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
  
    productionWhole <- productionQuery
 
  ## Convert time to numeric
  productionWhole[, timePointYears := as.numeric(timePointYears)]
  
  productionWhole[, geographicAreaM49 := as.numeric(geographicAreaM49)]
  
  return(productionWhole)
  
}

