#' Get Official Food Loss Data
#'
#' Function to obtain official food loss data at primary level
#' @param protected Logical only return observations with protected flag combination 
#' @import faoswsFlag faosws
#' @export getLossData

getLossData_SUA <- function(areaList,itemList, elementList = "5016",
                            faostatcountryList, faostatitemList,
                            selectedYear, startYearNewMethodology, protected = FALSE){
  
  # Period for new methodology production data
  yearNM <- as.numeric(startYearNewMethodology):max(as.numeric(selectedYear))
  
  ## define measured elements
  lossKey = DatasetKey(
    domain = "suafbs", #"agriculture",
    dataset = 'sua_balanced', #"aproduction",
    dimensions = list(
      Dimension(name = "geographicAreaM49",
                keys = areaList),
      Dimension(name = "measuredElementSuaFbs", keys = elementList), 
      Dimension(name = "measuredItemFbsSua",
                keys = itemList),
      Dimension(name = "timePointYears", keys = as.character(yearNM)))
  )
  
  lossQuery <- GetData(key = lossKey)
  
  setnames(lossQuery, c("measuredElementSuaFbs", "measuredItemFbsSua"), c("measuredElement", "measuredItemCPC"))
  
  # Period for frozen FAOSTAT production data
  yearOM <- as.numeric(min(as.numeric(selectedYear)):(as.numeric(startYearNewMethodology)-1))
  
  faostatKey = DatasetKey(
    domain = "faostat_one",
    dataset = "updated_sua_2013_data",
    dimensions = list(
      Dimension(name = "geographicAreaFS",
                keys = faostatcountryList),
      Dimension(name = "measuredElementFS", keys = '121'),
      Dimension(name = "timePointYears", keys = as.character(yearOM)),
      Dimension(name = "measuredItemFS",
                keys = faostatitemList)
    )
  )
  
  faostatQuery = GetData(key = faostatKey, flags = TRUE)
  
  
  faostatQuery[, geographicAreaM49 := fs2m49(geographicAreaFS)]
  faostatQuery[, measuredItemCPC := fcl2cpc(formatC(as.numeric(measuredItemFS), width = 4,
                                                    flag = "0"))]
  
  # unique(faostatQuery[is.na(geographicAreaM49)]$geographicAreaFS)
  # unique(faostatQuery[is.na(measuredItemCPC)]$measuredItemFS)
  # fs <- GetCodeList("faostat_one", "updated_sua_2013_data", "measuredItemFS")[code %in% unique(faostatQuery[is.na(measuredItemCPC)]$measuredItemFS)]
  # 861  Improved Pasture
  # 860  Range Pasture
  
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
  
  # Put together two Losses
  Lossdata <- rbind(faostatQuery, lossQuery)

  ## Convert time to numeric
  Lossdata[, timePointYears := as.numeric(timePointYears)]
  Lossdata[, geographicAreaM49 := as.numeric(geographicAreaM49)]
  
  Lossdata[ , flagCombination := paste(flagObservationStatus, 
                               flagMethod, sep = ";")]
  
  ## Reading FlagValidTable specific for loss
  flagValidTableLoss <- as.data.table(flagValidTable)
  
  ## Taking only official data
  if (protected) {
    protectedFlag <- flagValidTableLoss[ , flagCombination := paste(flagObservationStatus, 
                                                                    flagMethod, sep = ";")][Protected == TRUE, ]
    ## subset to protected flags
    Lossdata <- Lossdata[flagCombination %in% protectedFlag$flagCombination]
  }
  
  return(Lossdata)
}

