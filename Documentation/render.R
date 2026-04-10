
library("quarto")
quarto_render("clean_test.qmd")

# library("servr")
# servr::httd(".", host = "0.0.0.0", port = 4001)

quarto::quarto_preview("clean_test.qmd", host = "0.0.0.0", port = 4002)

quarto::quarto_preview("clean_test.qmd")
