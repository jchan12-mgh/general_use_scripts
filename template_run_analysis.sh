# This needs to be copied into the code folder with the Rmd file to be run

Rscript -e "source('template_analysis_script.R', echo=T, max.deparse.length=1e6)" \
--args \
add_log=1
