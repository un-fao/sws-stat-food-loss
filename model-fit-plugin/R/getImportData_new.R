##' Get Production Data
##' 
##' Function to obtain production data at primary level
#' @import  faosws
#' @export getImportData


getImportData_new <- function(areaList,itemList, elementList = "5610", selectedYear){
 
  totaltradekey <-
    DatasetKey(
      domain = "trade",
      dataset = "total_trade_cpc_m49",
      dimensions = list(
        Dimension(name = "geographicAreaM49",
                  keys = areaList),
        Dimension(name = "measuredElementTrade", keys = elementList), 
        Dimension(name = "timePointYears", keys = as.character(selectedYear)),
        Dimension(name = "measuredItemCPC",
                  keys = itemList))
    )
  
  existing_data <- GetData(key = totaltradekey) 
  setnames(existing_data, "measuredElementTrade", "measuredElement")
  
  return(existing_data)
}

