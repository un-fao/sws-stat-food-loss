
library("quarto")

# Prevent quarto from trying to open a browser
Sys.setenv(QUARTO_BROWSER = "none")

quarto::quarto_preview("clean_test.qmd", host = "0.0.0.0", port = 4000, browse = FALSE)
