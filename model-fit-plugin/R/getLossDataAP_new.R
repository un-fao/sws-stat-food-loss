#' Get Official Food Loss Data
#'
#' Function to obtain official food loss data at primary level
#' @param protected Logical only return observations with protected flag combination 
#' @import faoswsFlag faosws
#' @export getLossData

getLossData_AP <- function(areaList,itemList, elementList = "5016",
                            selectedYear, protected = FALSE){
  
  # Period for new methodology production data
 
  ## define measured elements
  lossKey = DatasetKey(
    domain = "agriculture",
    dataset = "aproduction",
    dimensions = list(
      Dimension(name = "geographicAreaM49",
                keys = areaList),
      Dimension(name = "measuredElement", keys = elementList), 
      Dimension(name = "timePointYears", keys = as.character(selectedYear)),
      Dimension(name = "measuredItemCPC",
                keys = itemList))
  )
  
  Lossdata = GetData(lossKey, flags = T)
  

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

