# ReadDatatable <- function(x, readOnly = T) {
#   f <- paste0(yaml::yaml.load_file('swsPROD.yml')$all$localdata, '/', x, '.rds')
#   if (file.exists(f)) {
#     readRDS(f)
#   } else {
#     faosws::ReadDatatable(x, readOnly = readOnly)
#   }
# }

SaveDataLoss <- function(x, datasetName) {
  faosws::SaveData(x, domain = "lossWaste", dataset = datasetName, waitTimeout = Inf)
}

sd_ <- function(x) sd(x, na.rm = TRUE)
max_ <- function(x) max(x, na.rm = TRUE)
min_ <- function(x) min(x, na.rm = TRUE)
sum_ <- function(x) sum(x, na.rm = TRUE)
mean_ <- function(x) mean(x, na.rm = TRUE)
median_ <- function(x) median(x, na.rm = TRUE)
rowSums_ <- function(x) rowSums(x, na.rm = TRUE)
rowMeans_ <- function(x) rowMeans(x, na.rm = TRUE)

na.fill_ <- function(x) {
  if(sum(!is.na(x)) > 1) {
    zoo::na.fill(x, "extend")
  } else {
    rep(x[!is.na(x)], length(x))
  }
}

thresh <- function(x, type = NA, magnitude = 3) {
  if (type %in% c("lower", "upper")) {
    if (type == "lower") {
      median(x, na.rm = TRUE) - magnitude*sd(x, na.rm = TRUE)
    } else {
      median(x, na.rm = TRUE) + magnitude*sd(x, na.rm = TRUE)
    }
  } else {
    stop("upper or lower?")
  }
}

pastep <- function(x) paste(x, sep = "+", collapse = " + ")

logit <- function(x) boot::logit(x)
invlogit <- function(x) boot::inv.logit(x)

x_impute <- function(x, fun) {
  x[is.na(x)] <- fun(x, na.rm = TRUE)
  return(x)
} 

# XXX: currently not used
# get_confint<-function(model, vcovCL){
#   t <- qt(.975, model$df.residual)
#   ct <- coeftest(model, vcovCL)
#   est <- cbind(ct[,1], ct[,1] - t*ct[,2], ct[,1] + t*ct[,2])
#   colnames(est) <- c("Estimate", "LowerCI", "UpperCI")
#   return(est)
# }


na_impute <- function(x) {
  # If more NAs (currently +50%) are in the first half,
  # build the model with reversed data (results will
  # then be reversed for output). Why? It seems to work
  # better. See, e.g., with this:
  # c(NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, 7.8, 8.68, 9.6, 11.48,
  # 16.89, 26.84, 27.16, 28.68, 29.06, 30.89, 40.14, 53.62, 54.77,
  # 38.19, 30.96, 33.75, 31.13, 27.5, 34.88, 38, 39.67, 39.67, 38.56,
  # 31.33, 32.3, 39.37, 38.07, 35.1, 29.23, 25.89, 26.25, 32.31,
  # 25.31, 26.09, 52.95, 47.62, 49.09, 65.73, 127.1, 71.84, 98.97,
  # 121.45, 96.36, 84.56, 70.13, 57.51, 65.86)
  if (any(is.na(x)) == TRUE) {
    if (class(x) %in% c('integer', 'numeric')) {
      more_nas_first <- sum(is.na(x)[(1:length(x)) < (length(x) / 2)]) > sum(is.na(x)[(1:length(x)) >= (length(x) / 2)])
      
      if (more_nas_first == TRUE) {
        x <- rev(x)
      }
      if (sum(!is.na(x)) >= 2) {
        # If data >= 10 & available > 60%, do the ARMA model
        # (otherwise an ARMA model may not be that good)
        if (length(x) >= 10 && sum(!is.na(x)) / length(x) > 0.6) {
          mod <- try(forecast::auto.arima(x))
          if (inherits(mod, 'try-error') | length(mod$model$phi) == 0) {
            res <- zoo::na.fill(x, "extend")
          } else {
            res <- suppressWarnings(imputeTS::na.kalman(x, model = mod$model))
          }
        } else {
          res <- zoo::na.fill(x, "extend")
        }
      } else {
        if (length(unique(x[!is.na(x)])) == 0) {
          res <- rep(as(NA, class(x)), length(x))
        } else {
          res <- rep(x[!is.na(x)], length(x))
        }
      }
      if (more_nas_first == TRUE) {
        res <- rev(res)
      }
    } else {
      if (length(unique(x[!is.na(x)])) == 0) {
        res <- rep(as(NA, class(x)), length(x))
      } else if (length(unique(x[!is.na(x)])) == 1) {
        res <- rep(unique(x[!is.na(x)]), length(x))
      } else {
        res <- x
      }
    }
    return(as(res, class(x)))
  } else {
    return(x)
  }
}

