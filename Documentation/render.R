
# quarto preview renders and serves - system() blocks so R stays alive
system("quarto preview clean_test.qmd --no-browser --no-watch-inputs --host 0.0.0.0 --port 4000 --timeout 0")
