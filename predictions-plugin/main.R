#options(repos = c(
#CRAN      = "https://rstudiopm.qa.aws.fao.org/fao-sws-cran/__linux__/centos7/latest",
#  CRAN_OLD  = "https://rstudiopm.qa.aws.fao.org/fao-sws-cran/2025-09-29+5HHVJ1y8"
#))
#source("renv/activate.R")
#renv::restore(prompt = FALSE)

cat("R version:", as.character(base::getRversion()), "\n")


library(mgcv)
library(countrycode)
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


if (faosws::CheckDebug()) {
    library(faoswsModules)
    SETT <- ReadSettings("sws.yml")
    GetTestEnvironment(baseUrl = SETT[["server"]], token = SETT[["token"]])
}


R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")

main_libpaths <- .libPaths()
#######
#Predictions part
######

# Number of samples to predict with (due to memory limitations).
n_sim_pred <- 4000



save_dir_post <- file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss","Saved_models","mcmc_outputs_2026")
save_dir_data <- file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss","Model_data")

fit_combined_samples <- qs2::qs_read(file.path(save_dir_post, "fit_combined_samples.qs2"))

scale_fit          <- qs2::qs_read(file.path(save_dir_post, "scale_fit.qs2"))

#model_data  <- qs2::qs_read(file.path(save_dir_data, "model_data.qs2"))
pred_inputs <- qs2::qs_read(file.path(save_dir_data, "prediction_inputs.qs2"))

pred_meta   <- qs2::qs_read(file.path(save_dir_data, "prediction_meta.qs2"))

# unpack inputs (so that the existing code works unchanged)
list2env(pred_inputs, envir = .GlobalEnv)
rm(pred_inputs); gc()

P <- pred_meta$P
year_seq <- pred_meta$year_seq
rm(pred_meta); gc()

###We will need to consider only valid country*year*production combinations
setDT(prod_support)
prod_support[, `:=`(
    m49_numeric     = as.integer(m49_numeric),
    year            = as.integer(year),
    measureditemcpc = as.character(measureditemcpc)
)]
setkey(prod_support, m49_numeric, measureditemcpc, year)





#n_sim_pred=4000 #4000

#Recreate the posterior arrays exactly as before:
n_sim_fit <- nrow(fit_combined_samples)

# These depend on model_data levels, which we loaded
# N_l1 <- length(levels(model_data$region_l1))
# N_l2 <- length(levels(model_data$region_l2))
# N_basket <- length(levels(model_data$food_group))
# N_country <- length(levels(model_data$iso3))
# N_crop <- length(levels(model_data$crop))
# N_basket_country <- length(levels(model_data$basket_country))
# N_crop_country <- length(levels(model_data$crop_country))
# N_stage <- length(levels(model_data$stage))


levels_fit <- qs2::qs_read(file.path(save_dir_post, "levels_fit.qs2"))

bc_levels <- levels_fit$basket_country
cc_levels <- levels_fit$crop_country
method_levels <- levels_fit$method
#survey_idx <- which(method_levels == "Survey")


N_l1            <- length(levels_fit$region_l1)
N_l2            <- length(levels_fit$region_l2)
N_basket        <- length(levels_fit$food_group)
N_country       <- length(levels_fit$country_m49)
N_crop          <- length(levels_fit$crop)
N_basket_country<- length(levels_fit$basket_country)
N_crop_country  <- length(levels_fit$crop_country)
N_stage         <- length(levels_fit$stage)


a1_samples <- fit_combined_samples[, grepl("^a1\\[", colnames(fit_combined_samples))] |> array(dim=c(n_sim_fit,P))
a2_samples <- fit_combined_samples[, grepl("^a2\\[", colnames(fit_combined_samples))] |> array(dim=c(n_sim_fit,N_l1,P))
a3_samples <- fit_combined_samples[, grepl("^a3\\[", colnames(fit_combined_samples))] |> array(dim=c(n_sim_fit,N_l2,P))
a4_samples <- fit_combined_samples[, grepl("^a4\\[", colnames(fit_combined_samples))] |> array(dim=c(n_sim_fit,N_country,2))

b1_samples <- fit_combined_samples[, grepl("^b1\\[", colnames(fit_combined_samples))] |> array(dim=c(n_sim_fit,N_basket,P))
b2_samples <- fit_combined_samples[, grepl("^b2\\[", colnames(fit_combined_samples))] |> array(dim=c(n_sim_fit,N_crop,2))
b3_samples <- fit_combined_samples[, grepl("^b3\\[", colnames(fit_combined_samples))] |> array(dim=c(n_sim_fit,N_basket_country,2))
b4_samples <- fit_combined_samples[, grepl("^b4\\[", colnames(fit_combined_samples))] |> array(dim=c(n_sim_fit,N_crop_country,2))

c1_samples <- fit_combined_samples[, grepl("^c1\\[", colnames(fit_combined_samples))]
c2_samples <- fit_combined_samples[, grepl("^c2\\[", colnames(fit_combined_samples))]





# # Extract samples for prediction ------------------------------------------
# 
# a1_samples <- fit_combined_samples[,grepl("^a1\\[",colnames(fit_combined_samples))]%>%
#   array(dim=c(n_sim_fit,P))
# a2_samples <- fit_combined_samples[,grepl("^a2\\[",colnames(fit_combined_samples))]%>%
#   array(dim=c(n_sim_fit,N_l1,P))
# a3_samples <- fit_combined_samples[,grepl("^a3\\[",colnames(fit_combined_samples))]%>%
#   array(dim=c(n_sim_fit,N_l2,P))
# a4_samples <- fit_combined_samples[,grepl("^a4\\[",colnames(fit_combined_samples))]%>%
#   array(dim=c(n_sim_fit,N_country,2))
# 
# b1_samples <- fit_combined_samples[,grepl("^b1\\[",colnames(fit_combined_samples))]%>%
#   array(dim=c(n_sim_fit,N_basket,P))
# b2_samples <- fit_combined_samples[,grepl("^b2\\[",colnames(fit_combined_samples))]%>%
#   array(dim=c(n_sim_fit,N_crop,2))
# b3_samples <- fit_combined_samples[,grepl("^b3\\[",colnames(fit_combined_samples))]%>%
#   array(dim=c(n_sim_fit,N_basket_country,2)) 
# b4_samples <- fit_combined_samples[,grepl("^b4\\[",colnames(fit_combined_samples))]%>%
#   array(dim=c(n_sim_fit,N_crop_country,2)) 
# 
# c1_samples <- fit_combined_samples[,grepl("^c1\\[",colnames(fit_combined_samples))]%>%
#   array(dim=c(n_sim_fit,N_stage))
# c2_samples <- fit_combined_samples[,grepl("^c2\\[",colnames(fit_combined_samples))]

set.seed(3408537)
sample_rows <- sample(1:n_sim_fit,n_sim_pred,replace = FALSE)

# We need to complete some effects, e.g. the model only sees countries that we have
# loss percentage data points for; for other countries/products we need to simulate
# from the posterior predictive distributions of relevant effects.

# Complete country effect.
N_country_full <- nrow(M49)
a4_samples_full <- array(NA,dim=c(n_sim_pred,N_country_full,2))
a4_samples_full[,sapply(levels_fit$country_m49,function(u)which(as.character(M49$m49_numeric)==u)),] <- a4_samples[sample_rows,,]

for(i in which(is.na(a4_samples_full[1,,1]))) a4_samples_full[,i,] <- rnorm(n_sim_pred*2,0,1)

# Complete product effect.
N_product_full <- nrow(fbsTree1)
b2_samples_full <- array(NA,dim=c(n_sim_pred,N_product_full,2))
b2_samples_full[,sapply(levels_fit$crop,function(u)which(fbsTree1$crop==u)),] <- b2_samples[sample_rows,,]

for(i in which(is.na(b2_samples_full[1,,1]))) b2_samples_full[,i,] <- rnorm(n_sim_pred*2,0,1)

# Sequence of years to predict for.
#year_seq <- 1991:2023
N_years <- length(year_seq)


# Prediction function, predict all years and products one country at a time -------------------------

pred_function <- function(X){
    #library(tidyverse)
    library(dplyr)
    library(tidyr)
    library(tibble)
    library(ggplot2)
    library(readr)
    library(stringr)
    
    library(nimble)
    library(float)
    library(data.table)
    
    # Create the full data frame for predictions.
    pred_df <- expand_grid(measureditemcpc=fbsTree1$measureditemcpc,year=year_seq,iso3=M49$iso3[X])%>%
        left_join(fbsTree1)%>%
        #left_join(select(FAOCrops,crop,measureditemcpc))%>%
        left_join(select(M49,m49_numeric,country,iso3,sdg_region_name,sdg_subregion_l1,sdg_subregion_l2,
                         l1_num,l2_num),
                  by=c("iso3"))%>%
        left_join(harvest_calendar_imputed,by=c("iso3","measureditemcpc"))%>%
        left_join(select(ungroup(GDP_full),iso3,year,GDP_percap,sm_GDP_percap),by=c("iso3","year"))%>%
        left_join(select(ungroup(LPI_full),iso3,year,sm_lpi),by=c("iso3","year"))%>%
        left_join(monthly_weather_full,by=c("harvesting_month_end"="month","year","iso3"))%>%
        mutate(
            year_raw = as.integer(year),   # <-- keep original year
            year = (year - scale_fit$year_mean) / scale_fit$year_sd,
            rain = (log(rainfall_mean+1) - scale_fit$rain_mean) / scale_fit$rain_sd,
            temp = (temperature_mean - scale_fit$temp_mean) / scale_fit$temp_sd,
            gdp  = (sm_GDP_percap - scale_fit$gdp_mean) / scale_fit$gdp_sd,
            lpi  = (sm_lpi - scale_fit$lpi_mean) / scale_fit$lpi_sd
        )
    
    
    ###Added by Lyuba to filter out the invalid combinations
    # ---- support for this country only (5510 is already in prod_support) ----
    support_x <- prod_support[list(as.integer(M49$m49_numeric[X])), .(measureditemcpc, year)]
    
    # fast lookup: years available per product for this country
    support_years <- split(support_x$year, support_x$measureditemcpc)
    
    pred_df$year_pos <- match(pred_df$year_raw, year_seq)
    ###
    
    
    
    
    # Create the covariate matrices.
    X1 <- cbind(1,pred_df$year,pred_df$rain,pred_df$temp,pred_df$gdp,pred_df$lpi)
    X2 <- cbind(1,pred_df$year)
    
    # Pre-allocate arrays for storing the predictions.
    mu_array <- array(NA,dim=c(n_sim_pred,N_years,N_product_full))
    #y_array <- array(NA,dim=c(n_sim_pred,N_years,N_product_full)) 
    
    # Random effect variance parameter samples.
    sigma_a3 <- fit_combined_samples[sample_rows,grepl("^sigma_a3",colnames(fit_combined_samples))]
    sigma_a4 <- fit_combined_samples[sample_rows,grepl("^sigma_a4",colnames(fit_combined_samples))]
    sigma_b2 <- fit_combined_samples[sample_rows,grepl("^sigma_b2",colnames(fit_combined_samples))]
    sigma_b3 <- fit_combined_samples[sample_rows,grepl("^sigma_b3",colnames(fit_combined_samples))]
    sigma_b4 <- fit_combined_samples[sample_rows,grepl("^sigma_b4",colnames(fit_combined_samples))]
    sigma_y <- fit_combined_samples[sample_rows,grepl("^sigma_y",colnames(fit_combined_samples))]
    
    # Predict one product at a time.
    for(j in 1:nrow(fbsTree1)){
        
        # Country-food group effects (b3)
        bc_key <- paste(fbsTree1$food_group[j], M49$m49_numeric[X])
        if (bc_key %in% bc_levels) {
            idx <- match(bc_key, bc_levels)
            b3_samples_here <- b3_samples[sample_rows, idx, ]
        } else {
            b3_samples_here <- matrix(rnorm(n_sim_pred * 2, 0, 1), ncol = 2)
        }
        
        # Country-product effects (b4)
        cc_key <- paste(fbsTree1$measureditemcpc[j], M49$m49_numeric[X])
        if (cc_key %in% cc_levels) {
            idx <- match(cc_key, cc_levels)
            b4_samples_here <- b4_samples[sample_rows, idx, ]
        } else {
            b4_samples_here <- matrix(rnorm(n_sim_pred * 2, 0, 1), ncol = 2)
        }
        
        # Compute the mean.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
        # mu_array[,,j] <- 
        #   (a1_samples[sample_rows,]+a2_samples[sample_rows,M49$l1_num[X],]+
        #      a3_samples[sample_rows,M49$l2_num[X],]*sigma_a3+
        #      b1_samples[sample_rows,fbsTree1$basket_num[j],]
        #   )%*%
        #   t(X1[pred_df$measureditemcpc==fbsTree1$measureditemcpc[j],])+
        #   (a4_samples_full[,X,]*sigma_a4+b2_samples_full[,j,]*sigma_b2+
        #      b3_samples_here*sigma_b3+b4_samples_here*sigma_b4)%*%
        #   t(X2[pred_df$measureditemcpc==fbsTree1$measureditemcpc[j],])
        # 
        
        
        #this replaces mu_array for considering only valid combinations
        cpc_j <- fbsTree1$measureditemcpc[j]
        years_keep <- support_years[[cpc_j]]
        if (is.null(years_keep) || length(years_keep) == 0L) next
        
        rows_j <- which(pred_df$measureditemcpc == cpc_j & pred_df$year_raw %in% years_keep)
        if (length(rows_j) == 0L) next
        
        pos_j <- pred_df$year_pos[rows_j]      # positions into 1..N_years
        X1_j  <- X1[rows_j, , drop = FALSE]
        X2_j  <- X2[rows_j, , drop = FALSE]
        
        mu_array[, pos_j, j] <-
            (a1_samples[sample_rows,] + a2_samples[sample_rows, M49$l1_num[X],] +
                 a3_samples[sample_rows, M49$l2_num[X],] * sigma_a3 +
                 b1_samples[sample_rows, fbsTree1$basket_num[j],]) %*% t(X1_j) +
            (a4_samples_full[,X,] * sigma_a4 + b2_samples_full[,j,] * sigma_b2 +
                 b3_samples_here * sigma_b3 + b4_samples_here * sigma_b4) %*% t(X2_j)
        
        
        
        
        
        
        # Simulate cloglog loss percentage (not currently used).
        #for(jj in 1:N_years){
        #  y_array[,jj,j] <- rnorm(n_sim_pred,mu_array[,jj,j],sigma_y) #y_array is not used anywhere!
        #}
        
    }
    
    # Remove objects to keep memory use efficient.
    rm(list = setdiff(ls(), c(
        "n_sim_pred","sample_rows","fbsTree1","M49","harvest_calendar_imputed",
        "GDP_full","LPI_full","FAOCrops","monthly_weather_full",
        "a1_samples","a2_samples","a3_samples","a4_samples_full","b1_samples",
        "b2_samples_full","b3_samples","b4_samples","fit_combined_samples",
        "N_years","year_seq","N_product_full","c2_samples","cloglog","icloglog",
        "mu_array",
        "levels_fit","scale_fit","bc_levels","cc_levels"
    )))
    gc()
    
    # We return the predictions as a single-precision float object
    # to save memory.
    return(fl(icloglog(mu_array)))
    
}
###THE COMBINATIONS NOT IN PROD_SUPPORT STAY AS NA

RNGkind("L'Ecuyer-CMRG")
set.seed(12345)

# where to store per-country prediction vectors
dir_country <- file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss","Working_samples","COUNTRY")
dir.create(dir_country, recursive = TRUE, showWarnings = FALSE)

save_country <- function(X) {
    out_file <- file.path(dir_country, paste0("pred_country_", M49$iso3[X], ".qs2"))
    qs2::qs_save(list(samples = pred_function(X)), out_file)
    NULL
}

n_workers <- 2

if (.Platform$OS.type == "unix") {
    
    # forked workers (no clusterExport needed)
    parallel::mclapply(
        1:N_country_full,
        save_country,
        mc.cores = n_workers,
        mc.set.seed = TRUE
    )
    
} else {
    
    # Windows PSOCK
    par_cluster <- parallel::makeCluster(n_workers)
    on.exit(try(parallel::stopCluster(par_cluster), silent = TRUE), add = TRUE)
    
    parallel::clusterSetRNGStream(par_cluster, iseed = 12345)
    
    parallel::clusterExport(par_cluster, "main_libpaths")
    parallel::clusterEvalQ(par_cluster, { .libPaths(main_libpaths); library(nimble); library(qs2); NULL })
    
    parallel::clusterExport(par_cluster, c(
        "save_country","pred_function","dir_country",
        "n_sim_pred","sample_rows","fbsTree1","M49","harvest_calendar_imputed",
        "GDP_full","LPI_full","FAOCrops","monthly_weather_full",
        "a1_samples","a2_samples","a3_samples","a4_samples_full","b1_samples",
        "b2_samples_full","b3_samples","b4_samples","fit_combined_samples",
        "N_years","year_seq","N_product_full","c2_samples","cloglog","icloglog",
        "scale_fit","levels_fit","bc_levels","cc_levels",
        "prod_support"
    ))
    
    parallel::parLapply(par_cluster, 1:N_country_full, save_country)
}

gc()


#pred_float has the dimension n_sim_pred*N_years*N_product_full*N_country_full
#Lyuba reshapes it back into 4D array
#pred_arr <- array(
#  as.numeric(pred_float),
#  dim = c(n_sim_pred, N_years, N_product_full, N_country_full)
#)

#dim(pred_arr)


###For Lyuba since I couldn't export those huge objects
#try(stopCluster(par_cluster), silent=TRUE)
#par_cluster <- makeCluster(5)
#clusterEvalQ(par_cluster, { library(nimble); NULL })


#saveRDS(list(
#  fit_combined_samples = fit_combined_samples,
#  a1_samples = a1_samples,
#   a2_samples = a2_samples,
#   a3_samples = a3_samples,
#   a4_samples_full = a4_samples_full,
#   b1_samples = b1_samples,
#   b2_samples_full = b2_samples_full,
#   b3_samples = b3_samples,
#   b4_samples = b4_samples
# ), "posterior_draws_test.rds")
# 
# 
# clusterEvalQ(par_cluster, {
#   posterior <- readRDS("posterior_draws_test.rds")
#   list2env(posterior, envir = .GlobalEnv)
#   rm(posterior); gc()
#   NULL
# })
# 
# 
# 
# 
# # Delete saved prediction files.
# unlink("Working samples/SUA/*", recursive = FALSE)
# unlink("Working samples/Survey/*", recursive = FALSE)

# Now we will save SUA and Survey level predictions. We will produce
# one file per MCMC sample, so that the FLP/FLI functions can access
# them one at a time later. This is necessary to limit memory usage.
# Directories
# --- SAVE SUA & SURVEY FILES DRAW-BY-DRAW ---------------------

dir_country  <- file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss","Working_samples","COUNTRY")
out_dir_sua  <- file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss","Working_samples","SUA")
out_dir_surv <- file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss","Working_samples","Survey")
dir.create(out_dir_sua,  recursive=TRUE, showWarnings=FALSE)
dir.create(out_dir_surv, recursive=TRUE, showWarnings=FALSE)

country_files <- file.path(dir_country, paste0("pred_country_", M49$iso3, ".qs2"))
if (!all(file.exists(country_files))) {
    miss <- country_files[!file.exists(country_files)]
    stop("Missing per-country files, e.g.: ", miss[1])
}

# sizes
n_cell_country <- N_years * N_product_full
n_cell_draw    <- n_cell_country * N_country_full

# Survey method index (use levels_fit; model_data not needed)
survey_col <- which(levels_fit$method == "Survey")
stopifnot(length(survey_col) == 1)

# reusable buffer for ONE draw across all countries (keeps RAM stable)
buf <- numeric(n_cell_draw)

progress_bar <- txtProgressBar(min=0, max=n_sim_pred, style=1)

for (i in 1:n_sim_pred) {
    
    pos <- 1L
    
    for (X in 1:N_country_full) {
        
        v <- qs2::qs_read(country_files[X])$samples
        idx <- seq(i, by=n_sim_pred, length.out=n_cell_country)
        
        buf[pos:(pos + n_cell_country - 1L)] <- as.numeric(v[idx])
        
        pos <- pos + n_cell_country
        rm(v)
    }
    
    # SUA draw i (all countries x years x products)
    float_slice <- float::fl(buf)
    qs2::qs_save(
        list(samples = float_slice),
        file = file.path(out_dir_sua, paste0("pred_sample_", i, ".qs2"))
    )
    
    # Survey draw i (same dimensions)
    float_slice_survey <- float::fl(
        icloglog(cloglog(float_slice) + c2_samples[sample_rows[i], survey_col])
    )
    qs2::qs_save(
        list(samples = float_slice_survey),
        file = file.path(out_dir_surv, paste0("pred_sample_", i, ".qs2"))
    )
    
    rm(float_slice, float_slice_survey, idx)
    if (i %% 10 == 0) gc()  # optional housekeeping
    setTxtProgressBar(progress_bar, value = i)
}

close(progress_bar)
gc()

# pred_float does not exist anymore, so DO NOT rm(pred_float)

# IMPORTANT: remove these lines now (pred_float no longer exists)
# rm(pred_float)



# Full prediction quantiles (from per-country files) ----------------------

dir_country <- file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss","Working_samples","COUNTRY")
country_files <- file.path(dir_country, paste0("pred_country_", M49$iso3, ".qs2"))

probs <- c(0.025, 0.5, 0.975)

n_cell_country <- N_years * N_product_full  # cells per country = year x product

survey_col <- which(levels_fit$method == "Survey")
stopifnot(length(survey_col) == 1)

# Allocate output: [quantile, year, product, method, country]
full_prediction_quantiles <- array(
    NA_real_,
    dim = c(3, N_years, N_product_full, 2, N_country_full),
    dimnames = list(
        quantile = c("CI_lower","median","CI_upper"),
        year = as.character(year_seq),
        measureditemcpc = fbsTree1$measureditemcpc,
        method = c("Supply utilization accounts", "Survey"),
        iso3 = M49$iso3
    )
)

for (X in seq_len(N_country_full)) {
    
    v <- qs2::qs_read(country_files[X])$samples
    mat <- matrix(as.numeric(v), nrow = n_sim_pred, ncol = n_cell_country)  # draws x cells
    
    # SUA quantiles
    q_sua <- apply(mat, 2, quantile, probs = probs, na.rm = TRUE) #Lyuba added na.rm=TRUE
    full_prediction_quantiles[,,,1,X] <- array(q_sua, dim = c(3, N_years, N_product_full))
    
    # Survey quantiles (shift in cloglog space by draw)
    delta <- c2_samples[sample_rows, survey_col]
    mat_survey <- icloglog(cloglog(mat) + delta)
    
    q_survey <- apply(mat_survey, 2, quantile, probs = probs, na.rm = TRUE) #Lyuba added na.rm=TRUE
    full_prediction_quantiles[,,,2,X] <- array(q_survey, dim = c(3, N_years, N_product_full))
    
    rm(v, mat, mat_survey, q_sua, q_survey, delta)
    gc()
}

# Coerce to a data frame 
full_prediction_quantiles_df <- full_prediction_quantiles %>%
    as.data.frame.table() %>%
    dplyr::rename(value = Freq) %>%
    pivot_wider(names_from = quantile, values_from = value) %>%
    mutate(year = as.numeric(as.character(year))) %>%
    left_join(select(M49, iso3, country, sdg_region_name, region_l2)) %>%
    left_join(select(fbsTree1, measureditemcpc, gfli_basket))

save(
    full_prediction_quantiles_df,
    file = file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss/Saved_models/full_prediction_quantiles_df_2026.RData")
)



##########Save SUA and Survey predictions separately
###SUA
full_prediction_quantiles_SUA_df <- full_prediction_quantiles_df %>% 
    dplyr::filter(method == "Supply utilization accounts")
save(
    full_prediction_quantiles_SUA_df,
    file = file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss/Saved_models/full_prediction_quantiles_SUA_df_2026.RData")
)


###Survey
full_prediction_quantiles_survey_df <- full_prediction_quantiles_df %>% 
    dplyr::filter(method == "Survey")
save(
    full_prediction_quantiles_survey_df,
    file = file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss/Saved_models/full_prediction_quantiles_survey_df_2026.RData")
)


#We need to plot the model_data for plotting only
#model_data <- qs2::qs_read(file.path(save_dir_data, "model_data.qs2"))
# # Function to plot predictions and original data points.
# pred_plot <- function(prediction_quantiles,model_data,plot_cpc,plot_iso3){
#   method_pal <- brewer.pal(5,"Set1")
#   ggplot(prediction_quantiles%>%filter(measureditemcpc==plot_cpc,iso3==plot_iso3,method=="Supply utilization accounts"))+
#     geom_ribbon(aes(x=year,ymin=CI_lower,ymax=CI_upper),alpha=0.25)+
#     geom_line(aes(x=year,y=median,linetype="Supply utilization\naccounts"))+
#     geom_line(data=prediction_quantiles%>%filter(measureditemcpc==plot_cpc,iso3==plot_iso3,method=="Survey"),aes(x=year,y=median,linetype="Survey"))+
#     geom_point(data=filter(model_data,measureditemcpc==plot_cpc,iso3==plot_iso3)%>%
#                  mutate(stage_original=case_match(stage_original,
#                                                   "wholesupplychain"~"Whole supply chain",
#                                                   "farm"~"Farm",
#                                                   "storage"~"Storage",
#                                                   "transport"~"Transport",
#                                                   "processing"~"Processing",
#                                                   "wholesale"~"Wholesale",
#                                                   "primaryproduct"~"Primary product")),
#                aes(x=year,y=loss_percentage,colour=method,shape=stage_original),size=1.5,
#                show.legend = TRUE)+
#     labs(title=filter(M49,iso3==plot_iso3)$country,
#          subtitle=str_to_title(filter(FAOCrops,measureditemcpc==plot_cpc)$crop),
#          x="Year",y="Loss percentage",shape="Food supply\nchain stage",
#          colour="Data collection\nmethod")+
#     scale_y_continuous(labels=scales::label_percent())+
#     theme_minimal()+
#     scale_color_manual(values=c("Reference"=method_pal[1],"Modelled estimates"=method_pal[2],
#                                 "Literature review"=method_pal[4]),
#                        drop=FALSE)+
#     scale_shape_manual(values=c("Whole supply chain"=16,
#                                 "Farm"=15,"Storage"=17,
#                                 "Transport"=4,"Wholesale"=8,
#                                 "Processing"=18,"Primary product"=0),drop=FALSE)+
#     scale_linetype(name="Prediction level")
# }

# # Example to plot wheat in India.
# pred_plot(full_prediction_quantiles_df,model_data,
#           plot_cpc="0111",
#           plot_iso3="IND")



file = file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss/Saved_models/full_prediction_quantiles_df_2026.RData")


save_dir_post <- file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss","Saved_models","mcmc_outputs_2026")
save_dir_data <- file.path(R_SWS_SHARE_PATH,"Bayesian_food_loss","Model_data")

fit_combined_samples <- qs2::qs_read(file.path(save_dir_post, "fit_combined_samples.qs2"))

scale_fit          <- qs2::qs_read(file.path(save_dir_post, "scale_fit.qs2"))

#Now combine the prediction code with the Output saving code Charlotte prepared
#losses <- load("NewModelData/full_prediction_quantiles_df_19_01_26.RData")
losses <- load(file.path(
    R_SWS_SHARE_PATH,
    "Bayesian_food_loss", "Saved_models",
    "full_prediction_quantiles_df_2026.RData"
))
head(full_prediction_quantiles_df)
setDT(full_prediction_quantiles_df)
names(full_prediction_quantiles_df)
full_prediction_quantiles_df[,.N, method]

full_prediction_quantiles_df[,c("country", 
                                #"m49_region", 
                                #"m49_subregion", 
                                "sdg_region_name", 
                                "region_l2", 
                                "gfli_basket") := NULL]

full_prediction_quantiles_df[, c("flagObservationStatus", "flagMethod") := list("I", "e")]

setnames(full_prediction_quantiles_df, 
         c("year", "measureditemcpc"),
         c("timePointYears", "measuredItemSuaFbs"))



full_prediction_quantiles_df[, geographicAreaM49 := countrycode(iso3, origin = "iso3c", destination = "un")]

full_prediction_quantiles_df$geographicAreaM49 <- as.character(full_prediction_quantiles_df$geographicAreaM49)

full_prediction_quantiles_df[iso3 == "TWN", geographicAreaM49 := "158"]
full_prediction_quantiles_df[iso3 == "XSQ", geographicAreaM49 := "680"]

any(is.na(full_prediction_quantiles_df$geographicAreaM49))

full_prediction_quantiles_df[, iso3 := NULL]

full_prediction_quantiles_df <- melt(full_prediction_quantiles_df, measure.vars = c("CI_lower", "median", "CI_upper"),
                                     variable.name = "measuredElementSuaFbs",
                                     value.name = "Value")

full_prediction_quantiles_df$measuredElementSuaFbs <- as.character(full_prediction_quantiles_df$measuredElementSuaFbs)

full_prediction_quantiles_df[measuredElementSuaFbs == "CI_lower", measuredElementSuaFbs := "61211"]
full_prediction_quantiles_df[measuredElementSuaFbs == "median", measuredElementSuaFbs := "5126"]
full_prediction_quantiles_df[measuredElementSuaFbs == "CI_upper", measuredElementSuaFbs := "61212"]

setcolorder(full_prediction_quantiles_df, c("geographicAreaM49", "measuredElementSuaFbs",
                                            "measuredItemSuaFbs", "timePointYears",
                                            "Value", "flagObservationStatus",
                                            "flagMethod", "method"))

loss_sua <- full_prediction_quantiles_df[method == "Supply utilization accounts"][, method := NULL]
loss_survey <- full_prediction_quantiles_df[method == "Survey"][, method := NULL]

loss_sua[,.N, measuredItemSuaFbs]

#Added by Lyuba. Since the invalid combinations of prod*country*year are NAs, we need to filter out NAs before saving them
loss_sua    <- loss_sua[!is.na(Value)]
loss_survey <- loss_survey[!is.na(Value)]

if(CheckDebug()){
    settings <- ReadSettings(file = "swsPROD.yml")
    GetTestEnvironment(
        baseUrl = settings[["server"]],
        token = "6b91b96c-57ed-40e6-bc76-9d5c03d440fc" # SUA level dataset
        # "3814e9fe-1e5d-4685-8fe7-dd1158e943d6" # Survey level dataset
    )
}


# Parallelized saving
SaveData <- function(
        domain,
        dataset,
        data,
        metadata = NULL,
        normalized = TRUE,
        waitMode = "wait",
        waitTimeout = Inf,
        chunkSize = 50000,
        ncore = 3,
        verbose = FALSE
) {
    
    require(data.table)
    require(faosws)
    require(future.apply)
    require(future)
    require(parallel)
    
    # Check available cores
    logical_cores <- tryCatch(parallel::detectCores(logical = TRUE), error = function(e) NA)
    physical_cores <- tryCatch(parallel::detectCores(logical = FALSE), error = function(e) NA)
    vals <- na.omit(c(as.numeric(logical_cores), as.numeric(physical_cores)))
    usable_cores <- if (length(vals) > 0) min(vals) else 1
    
    if (ncore > usable_cores) {
        warning(sprintf(
            "⚠ Requested %d cores but only %d are available. Using %d instead.",
            ncore, usable_cores, usable_cores
        ))
        ncore <- usable_cores
    }
    
    # Check duplicated keys
    keys_dimensions <- faosws::GetDatasetConfig(domain, dataset)$dimensions
    
    duplicate_mask <- duplicated(data, by = keys_dimensions)
    dup_count <- sum(duplicate_mask)
    
    if (dup_count > 0) {
        warning(sprintf(
            "⚠ Detected %d duplicated key rows in data. Keeping the first occurrence of each key.",
            dup_count
        ))
        data <- data[!duplicate_mask]
    }
    
    # Split data into chunks
    total_rows <- nrow(data)
    if (total_rows == 0) stop("No data to save.")
    chunks <- split(data, rep(seq_len(ncore), length.out = total_rows))
    
    # Split metadata if provided
    metadata_chunks <- NULL
    if (!is.null(metadata)) {
        dim_ <- faosws::GetDatasetConfig(domain, dataset)[["dimensions"]]
        metadata_chunks <- lapply(seq_along(chunks), function(i){
            keys_data <- unique(chunks[[i]][, mget(dim_)])
            metadata[keys_data, on = dim_, nomatch = NULL]
        })
    }
    
    if (verbose) {
        message(sprintf("→ Parallel SaveData: %s rows | %d processes",
                        format(total_rows, big.mark = ","), length(chunks)))
    }
    
    # Start timer -----------------------------------------------------------
    global_start <- Sys.time()
    
    # Try multicore ---------------------------------------------------------
    results <- tryCatch({
        plan(multicore, workers = ncore)
        
        suppressWarnings({  # prevent flood of mccollect/future warnings
            future_lapply(seq_along(chunks), function(i) {
                chunk <- chunks[[i]]
                metadata_chunk <- metadata_chunks[[i]]
                start_time <- Sys.time()
                
                save <- if (!is.null(metadata)) {
                    faosws::SaveData(
                        domain = domain,
                        dataset = dataset,
                        data = chunk,
                        metadata = metadata_chunk,
                        normalized = normalized,
                        waitMode = waitMode,
                        waitTimeout = waitTimeout,
                        chunkSize = chunkSize
                    )
                } else {
                    faosws::SaveData(
                        domain = domain,
                        dataset = dataset,
                        data = chunk,
                        normalized = normalized,
                        waitMode = waitMode,
                        waitTimeout = waitTimeout,
                        chunkSize = chunkSize
                    )
                }
                
                elapsed <- difftime(Sys.time(), start_time, units = "secs")
                
                list(process = i, rows = nrow(chunk), success = TRUE,
                     save = save, time_sec = as.numeric(elapsed))
            })
        })
    },
    error = function(e) {
        if (verbose) message("⚠ Multicore failed, using sequential: ", conditionMessage(e))
        
        plan(sequential)
        lapply(seq_along(chunks), function(i) {
            chunk <- chunks[[i]]
            metadata_chunk <- metadata_chunks[[i]]
            start_time <- Sys.time()
            
            save <- if (!is.null(metadata)) {
                faosws::SaveData(
                    domain = domain,
                    dataset = dataset,
                    data = chunk,
                    metadata = metadata_chunk,
                    normalized = normalized,
                    waitMode = waitMode,
                    waitTimeout = waitTimeout,
                    chunkSize = chunkSize
                )
            } else {
                faosws::SaveData(
                    domain = domain,
                    dataset = dataset,
                    data = chunk,
                    normalized = normalized,
                    waitMode = waitMode,
                    waitTimeout = waitTimeout,
                    chunkSize = chunkSize
                )
            }
            
            elapsed <- difftime(Sys.time(), start_time, units = "secs")
            
            list(process = i, rows = nrow(chunk), success = TRUE,
                 save = save, time_sec = as.numeric(elapsed))
        })
    })
    
    # End global timer ------------------------------------------------------
    global_elapsed <- as.numeric(difftime(Sys.time(), global_start, units = "secs"))
    
    # Aggregate results -----------------------------------------------------
    inserted  <- sum(sapply(results, \(r) if (r$success) r$save$inserted  else 0), na.rm = TRUE)
    appended  <- sum(sapply(results, \(r) if (r$success) r$save$appended  else 0), na.rm = TRUE)
    ignored   <- sum(sapply(results, \(r) if (r$success) r$save$ignored   else 0), na.rm = TRUE)
    discarded <- sum(sapply(results, \(r) if (r$success) r$save$discarded else 0), na.rm = TRUE)
    
    # Fix: Properly unlist warnings columns before rbindlist
    warnings_list <- lapply(results, \(r) {
        if (r$success && !is.null(r$save$warnings) && nrow(r$save$warnings) > 0) {
            w <- r$save$warnings
            # Unlist any list columns
            w$row <- unlist(w$row)
            w$message <- unlist(w$message)
            w
        } else {
            NULL
        }
    })
    warnings_all <- data.table::rbindlist(warnings_list, fill = TRUE, use.names = TRUE)
    
    # Remove duplicates and issue warnings
    if (nrow(warnings_all) > 0) {
        warnings_all <- unique(warnings_all, by = c("message"))
        sapply(warnings_all$message, warning)
    } else {
        # Match original format: warnings = NULL when no warnings
        warnings_all <- NULL
    }
    
    # Build parallel summary
    parallel_summary <- data.table::rbindlist(lapply(results, \(r) {
        data.table(
            process = r$process,
            rows = r$rows,
            inserted = if (r$success) r$save$inserted else 0,
            appended = if (r$success) r$save$appended else 0,
            ignored = if (r$success) r$save$ignored else 0,
            discarded = if (r$success) r$save$discarded else 0,
            time_sec = r$time_sec,
            rows_per_sec = round(r$rows / r$time_sec, 2),
            status = if (r$success) "success" else "failed"
        )
    }))
    
    # Calculate performance metrics
    total_rows_per_sec <- round(total_rows / global_elapsed, 2)
    avg_time <- mean(parallel_summary$time_sec)
    speedup_estimate <- round(avg_time * ncore / global_elapsed, 2)
    
    # Verbose output summary
    if (verbose) {
        n_warnings <- if (is.null(warnings_all)) 0 else nrow(warnings_all)
        message(sprintf(
            "✓ Completed in %.2fs | %s rows/sec | Speedup: %.2fx | Inserted: %s | Appended: %s | Ignored: %s | Discarded: %s | Warnings: %d",
            global_elapsed,
            format(total_rows_per_sec, big.mark = ","),
            speedup_estimate,
            format(inserted, big.mark = ","),
            format(appended, big.mark = ","),
            format(ignored, big.mark = ","),
            format(discarded, big.mark = ","),
            n_warnings
        ))
    }
    
    # Build output with summary always included
    list(
        inserted = inserted,
        appended = appended,
        ignored = ignored,
        discarded = discarded,
        warnings = warnings_all,
        summary = list(
            total_rows = total_rows,
            total_time_sec = global_elapsed,
            throughput_rows_per_sec = total_rows_per_sec,
            speedup_factor = speedup_estimate,
            processes_used = ncore,
            parallel_details = parallel_summary
        )
    )
}

years <- seq(min(unique(loss_survey$timePointYears)), max(unique(loss_survey$timePointYears)), by =5)

for(i in seq_along(years)){
    SaveData("lossWaste", 
             "food_loss_estimates_survey", 
             loss_survey[timePointYears %in% years[i]:(years[i]+4)],
             metadata = NULL,
             normalized = TRUE,
             waitMode = "wait",
             waitTimeout = Inf,
             chunkSize = 50000,
             ncore = 3,
             verbose = FALSE)
    print(i)
}


years <- seq(min(unique(loss_sua$timePointYears)), max(unique(loss_sua$timePointYears)), by =5)

loss_sua[, timePointYears := as.character(timePointYears)]

for(i in seq_along(years)){
    SaveData("lossWaste", 
             "food_loss_estimates_sua", 
             loss_sua[timePointYears %in% years[i]:(years[i]+4)],
             metadata = NULL,
             normalized = TRUE,
             waitMode = "wait",
             waitTimeout = Inf,
             chunkSize = 50000,
             ncore = 3,
             verbose = FALSE)
    print(i)
}


# CHECKS -----------------------------------------------------------------------

surveyKey = DatasetKey(
    domain = "lossWaste",
    dataset = "food_loss_estimates_survey",
    dimensions = list(
        Dimension(name = "geographicAreaM49",
                  keys = GetCodeList("lossWaste", "food_loss_estimates_survey", "geographicAreaM49")[,code]),
        Dimension(name = "measuredElementSuaFbs", 
                  keys = GetCodeList("lossWaste", "food_loss_estimates_survey", "measuredElementSuaFbs")[,code]),
        Dimension(name = "timePointYears", 
                  keys = GetCodeList("lossWaste", "food_loss_estimates_survey", "timePointYears")[,code]),
        Dimension(name = "measuredItemSuaFbs",
                  keys = GetCodeList("lossWaste", "food_loss_estimates_survey", "measuredItemSuaFbs")[,code]))
)

surveyQuery = GetData(
    key = surveyKey,
    flags = F)


loss_survey[, Value := as.numeric(Value)]
loss_survey[, timePointYears := as.character(timePointYears)]

xxx <- loss_survey[!surveyQuery, on = names(loss_survey)[1:4]]
countries <- unique(xxx$geographicAreaM49)

for(i in countries){
    SaveData("lossWaste", 
             "food_loss_estimates_survey", 
             xxx,
             metadata = NULL,
             normalized = TRUE,
             waitMode = "wait",
             waitTimeout = Inf,
             chunkSize = 50000,
             ncore = 3,
             verbose = FALSE)
    print(i)
}

