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
  yearNM <- min(as.numeric(selectedYear)):max(as.numeric(selectedYear))
  
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
  
  
  Lossdata <- lossQuery
  
  ## Convert time to numeric
  Lossdata[, timePointYears := as.numeric(timePointYears)]
  Lossdata[, geographicAreaM49 := as.numeric(geographicAreaM49)]
  
  Lossdata[ , flagCombination := paste(flagObservationStatus, 
                                       flagMethod, sep = ";")]
  
  ## Reading FlagValidTable specific for loss
  flagValidTableLoss <- as.data.table(flagValidTable)
  
  ## Taking only official data
  if (protected) {
    # subset to protected flags
    Lossdata <- Lossdata[flagObservationStatus %in% c('', 'T')]
  }
  
  return(Lossdata)
}

